"""Can a candidate engine join the bench? candidate_engine.yml runs this; README says the rules.

THREE REQUIREMENTS, all against the real lakehouse, and a candidate needs every one:

  1. SQL          the 22 TPC-H statements run as SQL (bench.tpch.queries, the bench's own text)
  2. read Azure   the OneLake Iceberg tables through the REST catalog -- nation must count 25,
                  which takes real data-file reads, not just metadata -- AND a raw CSV in the
                  lakehouse Files section, where the ETL lands its input
  3. write        CTAS an Iceberg table through the same catalog, read it back in the engine AND
                  in pyiceberg -- a table only the writer can read is not Iceberg

Every probe runs regardless of the others and the SUMMARY is the result: which layer fails
(catalog auth, storage auth, dialect, Iceberg write) is the finding. Where the right credential
form is undocumented for OneLake, the candidate lists VARIANTS and each one's error is printed.

Statements carry the bearer or a SAS in their text and engines quote statements back in errors,
so everything printed goes through `scrub`.

A new candidate is a subclass with `start`, `sql`, and the variant lists, plus a registry entry.
"""

from __future__ import annotations

import csv
import io
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from bench import auth, onelake, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS
from bench.etl.config import EtlConfig
from bench.etl.data import csv_names
from bench.etl.schema import COLUMNS, FILTER
from bench.suite import suite_class
from bench.tpch import queries
from bench.tpch.engines.pyspark_gluten_iceberg import onelake_sas

CONTAINER = "candidate"
WRITE_NS = "candidate"
NATION_ROWS = 25
# The GitHub OIDC assertion, on the host and as the container sees it. Hadoop's
# WorkloadIdentityTokenProvider re-reads the file on every token refresh, so a thread rewrites it
# well inside the assertion's ~5-minute life -- the scheme bench/tpch/engines/pyspark_iceberg.py
# uses for Spark-OSS.
ASSERTION_DIR = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "candidate-oidc"
ASSERTION_IN_CONTAINER = "/var/run/candidate-oidc/assertion"
ASSERTION_REFRESH_S = 240


def _keep_assertion_fresh() -> None:
    ASSERTION_DIR.mkdir(parents=True, exist_ok=True)
    target = ASSERTION_DIR / "assertion"

    def write() -> None:
        target.write_text(auth._github_oidc_assertion(), encoding="utf-8")
        target.chmod(0o644)  # the container's user is not the runner's

    write()

    def loop() -> None:
        while True:
            time.sleep(ASSERTION_REFRESH_S)
            try:
                write()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                _say(f"  warning: assertion refresh failed: {exc}")

    threading.Thread(target=loop, daemon=True).start()


def _say(text: str) -> None:
    print(scrub.scrub(text), flush=True)


class Candidate:
    name = ""
    default_image = ""

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.image = os.environ.get("CANDIDATE_IMAGE") or self.default_image

    def start(self) -> None: ...
    def sql(self, statement: str) -> list[tuple]: ...
    def version(self) -> str: ...
    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]: ...
    def files_variants(self, path: str, sas: str) -> dict[str, str]: ...
    def write_variants(self, table: str, source: str) -> dict[str, list[str]]: ...
    def use_catalog(self) -> list[str]: ...


class StarRocks(Candidate):
    """StarRocks allin1: the Java FE (planner, catalog) and C++ BE (execution) in one container.

    Catalog properties are the documented REST ones (docs/en/data_source/catalog/iceberg/
    iceberg.md). Storage goes through hadoop-azure, keyed by the HOST of the abfss URI.

    NEVER `azure.adls2.storage_account`: StarRocks turns it into `<account>.dfs.core.windows.net`
    (AzureStorageCloudCredential), so "onelake" configures the wrong host and every read of
    onelake.dfs.fabric.microsoft.com falls back to SharedKey -- `fs.azure.account.key` null, the
    error of run 36214160572. Left empty, the OAuth keys are set unscoped, and
    `azure.adls2.endpoint` scopes a SAS to OneLake's host instead.
    """

    name = "starrocks"
    default_image = "starrocks/allin1-ubuntu:4.1-latest"

    def start(self) -> None:
        _keep_assertion_fresh()
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-v",
                f"{ASSERTION_DIR}:{Path(ASSERTION_IN_CONTAINER).parent}:ro",
                "-p",
                "127.0.0.1:9030:9030",
                "-p",
                "127.0.0.1:8030:8030",
                "-p",
                "127.0.0.1:8040:8040",
                self.image,
            ],
            check=True,
        )
        digest = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", self.image],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        _say(f"image {self.image}  {digest}")
        # The FE answers SELECT 1 before the BE has registered; a query needs a live BE.
        deadline = time.time() + 300
        while True:
            try:
                alive = [r for r in self.sql("SHOW BACKENDS") if "true" in map(str, r)]
                if alive:
                    return
            except Exception:  # noqa: BLE001 - not up yet
                pass
            if time.time() > deadline:
                raise RuntimeError("StarRocks BE not alive after 300s")
            time.sleep(5)

    def _conn(self):
        import pymysql

        if getattr(self, "_c", None) is None:
            self._c = pymysql.connect(
                host="127.0.0.1",
                port=9030,
                user="root",
                password="",
                autocommit=True,
                read_timeout=3600,
            )
        return self._c

    def sql(self, statement: str) -> list[tuple]:
        try:
            with self._conn().cursor() as cur:
                cur.execute(statement)
                return list(cur.fetchall())
        except Exception:
            # A connection the server dropped must not poison every later probe.
            if getattr(self, "_c", None) is not None and not self._c.open:
                self._c = None
            raise

    def version(self) -> str:
        return str(self.sql("SELECT current_version()")[0][0])

    def storage_variants(self, sas: str) -> dict[str, str]:
        """hadoop-azure credentials for OneLake, as StarRocks property lists."""
        return {
            # Spark-OSS's credential: the OIDC assertion file, exchanged by hadoop-azure itself,
            # refreshable for any run length.
            "workload identity": (
                f'"azure.adls2.oauth2_token_file"="{ASSERTION_IN_CONTAINER}", '
                f'"azure.adls2.oauth2_tenant_id"="{os.environ.get("AZURE_TENANT_ID", "")}", '
                f'"azure.adls2.oauth2_client_id"="{os.environ.get("AZURE_CLIENT_ID", "")}"'
            ),
            # Gluten's credential: a user-delegation SAS, scoped to OneLake's dfs host. One hour.
            "SAS on the OneLake endpoint": (
                f'"azure.adls2.endpoint"="{ONELAKE_DFS}", "azure.adls2.sas_token"="{sas}"'
            ),
        }

    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]:
        base = (
            '"type"="iceberg", "iceberg.catalog.type"="rest", '
            f'"iceberg.catalog.uri"="{ICEBERG_ENDPOINT}", '
            f'"iceberg.catalog.warehouse"="{self.cfg.warehouse}"'
        )
        oauth = f'"iceberg.catalog.security"="oauth2", "iceberg.catalog.oauth2.token"="{token}"'

        def ddl(props: str) -> list[str]:
            return [
                "DROP CATALOG IF EXISTS onelake",
                f"CREATE EXTERNAL CATALOG onelake PROPERTIES ({base}, {props})",
            ]

        no_vending = '"iceberg.catalog.vended-credentials-enabled"="false"'
        variants = {
            f"oauth2 token + {label}": ddl(f"{oauth}, {no_vending}, {props}")
            for label, props in self.storage_variants(sas).items()
        }
        # Vended alone attaches and lists, but in run 36214160572 every data read still fell back
        # to SharedKey: whatever OneLake vends does not reach hadoop-azure. Kept as a check.
        variants["oauth2 token + vended credentials"] = ddl(
            f'{oauth}, "iceberg.catalog.vended-credentials-enabled"="true"'
        )
        return variants

    def use_catalog(self) -> list[str]:
        return ["SET CATALOG onelake", "SET query_timeout = 3600"]

    def files_variants(self, path: str, sas: str) -> dict[str, str]:
        """The ETL's actual read of one AEMO file: parse, filter, cast -- not just reach it.

        AEMO "daily" files are RAGGED: a `C` header line, then `I`/`D` rows of several record
        types, each its own width. Default inference fails on that ("Schema column count: 120
        doesn't match source value column count: 10", run 36213949272). The ETL's engines read it
        with the 53-column DUNIT layout (bench/etl/schema.py), short rows padded with NULL, then
        filter to DUNIT v3 -- so that is the read asked for here. `schema` is FILES() from 4.1.2.
        """
        storage = self.storage_variants(sas)["workload identity"]
        declared = ", ".join(f"{c} VARCHAR" for c in COLUMNS)
        where = " AND ".join(f"{c} = '{v}'" for c, v in FILTER)
        csv = (
            '"format"="csv", "csv.column_separator"=",", "csv.enclose"=\'"\', "csv.skip_header"="1"'
        )

        def read(options: str) -> str:
            return (
                f"SELECT count(*), sum(CAST(TOTALCLEARED AS DOUBLE)) FROM FILES("
                f'"path"="{path}", {csv}, {options}, {storage}) WHERE {where}'
            )

        return {
            "53-column schema, short rows NULL-padded": read(
                f'"schema"="{declared}", "fill_mismatch_column_with"="null"'
            ),
            "53-column schema, strict": read(f'"schema"="{declared}"'),
        }

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        location = f"{self.cfg.base_path}/Tables/{WRITE_NS}/{table}"
        prep = [
            f"CREATE DATABASE IF NOT EXISTS {WRITE_NS}",
            f"DROP TABLE IF EXISTS {WRITE_NS}.{table}",
        ]
        return {
            # OneLake wants tables under Tables/<ns>/<table>, which pyiceberg and Spark had to
            # pass explicitly (bench/etl/iceberg.py, bench/etl/engines/pyspark_iceberg.py).
            "CTAS with location": prep
            + [
                f'CREATE TABLE {WRITE_NS}.{table} PROPERTIES ("location"="{location}") '
                f"AS SELECT * FROM {source}"
            ],
            "CTAS": prep + [f"CREATE TABLE {WRITE_NS}.{table} AS SELECT * FROM {source}"],
            "CREATE + INSERT": prep
            + [
                f"CREATE TABLE {WRITE_NS}.{table} (n_nationkey INT, n_name VARCHAR(25), "
                f"n_regionkey INT, n_comment VARCHAR(152))",
                f"INSERT INTO {WRITE_NS}.{table} SELECT * FROM {source}",
            ],
        }


CANDIDATES = {"starrocks": StarRocks}


def _try(engine: Candidate, label: str, statements: list[str]) -> tuple[bool, list[tuple]]:
    started = time.perf_counter()
    rows: list[tuple] = []
    try:
        for statement in statements:
            rows = engine.sql(statement)
    except Exception as exc:  # noqa: BLE001 - reporting failures is this script's job
        took = time.perf_counter() - started
        _say(f"    FAIL  {label}  ({took:.1f}s)  {scrub.scrub_exc(exc, 1500)}")
        return False, []
    _say(f"    PASS  {label}  ({time.perf_counter() - started:.1f}s)  {str(rows[:5])[:300]}")
    return True, rows


def _reference(etl: EtlConfig, name: str) -> tuple[int, float]:
    """DUNIT v3 row count and sum(TOTALCLEARED) of one CSV, by Python's csv module alone.

    Same rules as the ETL: skip the first line, filter by position on bench/etl/schema.py's
    FILTER, pad short rows. Independent of every engine, so a match means the parse is right.
    """
    raw = (
        onelake.file_system(etl)
        .get_file_client(f"{etl.csv_relative}/{name}")
        .download_file()
        .readall()
        .decode("utf-8", errors="replace")
    )
    position = {c: i for i, c in enumerate(COLUMNS)}
    cleared = position["TOTALCLEARED"]
    rows, total = 0, 0.0
    for record in csv.reader(io.StringIO(raw).readlines()[1:]):
        if all(len(record) > position[c] and record[position[c]] == v for c, v in FILTER):
            rows += 1
            if len(record) > cleared and record[cleared]:
                total += float(record[cleared])
    return rows, total


def _first_passing(
    engine: Candidate, gate: str, variants: dict[str, list[str]], check
) -> str | None:
    _say(f"\n[{gate}]")
    for label, statements in variants.items():
        ok, rows = _try(engine, label, statements)
        if ok and check(rows):
            return label
        if ok:
            _say(f"          {label}: ran, but the check failed on {str(rows[:5])[:300]}")
    return None


def main() -> int:
    name = sys.argv[1] if len(sys.argv) > 1 else "starrocks"
    cfg = suite_class("tpch").from_env()
    engine = CANDIDATES[name](cfg)
    engine.start()
    _say(f"{name} {engine.version()}")

    token = auth.onelake_token()
    sas, _ = onelake_sas(cfg.workspace_id, cfg.lakehouse_id)
    results: dict[str, bool] = {}

    # Gate 0: attach. A variant counts only when it lists the namespace AND reads nation's data
    # files: listing alone passed in run 36214160572 with storage auth entirely broken.
    attached = None
    _say("\n[attach REST catalog]")
    for label, statements in engine.attach_variants(token, sas).items():
        ok, _ = _try(engine, label, statements)
        if not ok:
            continue
        listed, rows = _try(engine, f"{label}: list namespaces", ["SHOW DATABASES FROM onelake"])
        if not (listed and any(cfg.schema in map(str, r) for r in rows)):
            continue
        read, rows = _try(
            engine, f"{label}: nation", [f"SELECT count(*) FROM onelake.{cfg.schema}.nation"]
        )
        if read and rows and int(rows[0][0]) == NATION_ROWS:
            attached = label
            break
    results["attach catalog"] = attached is not None
    # Even with no variant listing the namespace, carry on: each later gate's error is diagnosis.
    for statement in engine.use_catalog():
        _try(engine, statement, [statement])

    # Gate 1: read Iceberg from OneLake.
    ok, rows = _try(engine, "nation count", [f"SELECT count(*) FROM onelake.{cfg.schema}.nation"])
    results["read iceberg (nation = 25)"] = bool(ok and rows and int(rows[0][0]) == NATION_ROWS)

    # Gate 1b: parse a raw AEMO CSV from the Files section, as the ETL does, and match a count
    # and a sum computed here in plain Python from the same bytes.
    files = None
    try:
        etl = EtlConfig.from_env()
        names, _ = csv_names(etl, 1)
        path = f"{etl.csv_abfss}/{names[0]}"
        want_rows, want_sum = _reference(etl, names[0])
        _say(f"\nfile {path}\n  reference (python csv): {want_rows} DUNIT v3 rows, sum {want_sum}")

        def matches(rows) -> bool:
            if not rows or rows[0][0] is None:
                return False
            got_rows, got_sum = int(rows[0][0]), float(rows[0][1] or 0)
            return got_rows == want_rows > 0 and abs(got_sum - want_sum) <= 1e-6 * max(
                1.0, abs(want_sum)
            )

        files = _first_passing(
            engine,
            "parse Files/csv (ragged AEMO, DUNIT v3)",
            {label: [stmt] for label, stmt in engine.files_variants(path, sas).items()},
            matches,
        )
    except Exception as exc:  # noqa: BLE001 - no CSV landed, or no SAS: the gate fails, loudly
        _say(f"\n[parse Files/csv]\n    FAIL  setup  {scrub.scrub_exc(exc, 1500)}")
    results["parse Files csv (count + sum = python)"] = files is not None

    # Gate 2: SQL, the whole TPC-H suite.
    _say(f"\n[TPC-H SF={cfg.sf}, {queries.N_QUERIES} statements]")
    passed = 0
    for index, statement in enumerate(queries.load(name, cfg.schema, cfg.sf), start=1):
        ok, rows = _try(engine, f"Q{index}", [statement])
        if ok:
            _say(f"          Q{index}: {len(rows)} rows")
        passed += ok
    results[f"SQL (TPC-H {passed}/{queries.N_QUERIES})"] = passed == queries.N_QUERIES

    # Gate 3: write Iceberg, then read it back in the engine AND in pyiceberg.
    table = name
    written = _first_passing(
        engine,
        "write Iceberg",
        engine.write_variants(table, f"{cfg.schema}.nation"),
        lambda rows: True,
    )
    readback = False
    if written:
        ok, rows = _try(engine, "read back", [f"SELECT count(*) FROM {WRITE_NS}.{table}"])
        try:
            arrow = auth.catalog(cfg).load_table(f"{WRITE_NS}.{table}").scan().to_arrow()
            _say(f"    pyiceberg read-back: {arrow.num_rows} rows, schema {arrow.schema}")
            readback = (
                bool(ok and rows)
                and int(rows[0][0]) == NATION_ROWS
                and arrow.num_rows == NATION_ROWS
            )
        except Exception as exc:  # noqa: BLE001
            _say(f"    FAIL  pyiceberg read-back  {scrub.scrub_exc(exc, 1500)}")
        _try(engine, "drop", [f"DROP TABLE IF EXISTS {WRITE_NS}.{table}"])
    results["write Iceberg (+ pyiceberg read-back = 25)"] = readback

    _say(f"\nSUMMARY  {name}  {engine.image}")
    _say(f"  attach variant: {attached}   files variant: {files}   write variant: {written}")
    for label, ok in results.items():
        _say(f"  {'PASS' if ok else 'FAIL'}  {label}")
    requirements = all(results.values())
    _say(
        f"\n  {'QUALIFIES' if requirements else 'DOES NOT QUALIFY'}: SQL, read Azure, write Iceberg"
    )
    return 0 if requirements else 1


if __name__ == "__main__":
    sys.exit(main())
