"""chDB (ClickHouse in-process) against the OneLake Iceberg REST catalog.

Port of cell 12's `chdb_iceberg` branch, and the one engine whose OneLake support is narrow enough
to be worth writing down:

  * `DataLakeCatalog(...) SETTINGS catalog_type='onelake', onelake_bearer_token=...` is the ONLY
    route into OneLake that accepts an Entra bearer token. The same string signs the catalog's
    REST calls and the blob reads.
  * `azureBlobStorage()` / `icebergAzure()` / `deltaLakeAzure()` do NOT take a bearer token at
    all -- their credential surface is a connection string, account_name+key, a SAS, or a
    workload identity. Irrelevant here (everything is catalog-attached) but it is why there is no
    fallback path if the catalog attach fails.
  * `onelake_bearer_token` does not exist below chdb-core 26.7, which chdb 4.4.0 is the first
    release to require. Resolve anything older and the attach fails with "Unknown setting", which
    reads like a OneLake permissions problem rather than a wheel problem. Hence the >=4.4.0 floor
    in requirements/chdb_iceberg.txt.
"""

from __future__ import annotations

import os
import pathlib

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT, Config
from bench.tpch.config import chdb_cache_gib

# The attached catalog's name inside chDB.
DB = "onelake"

# Beta gate. The datalake catalog database engine refuses to be created without it. It must be a
# session-scoped SET, live when CREATE DATABASE runs -- a SETTINGS clause there configures the
# DATABASE, not the statement.
GATE = "allow_database_iceberg"

# SETTINGS THAT CHANGE ANSWERS, kept apart from the ones that only change speed.
#
# .github/scripts/smoke_sql.py applies exactly this tuple to its local session, so the dialect
# check runs with the benchmark's semantics. That separation exists because the first version did
# NOT have it: the smoke adapter built a bare session, the join_use_nulls fix below was never in
# effect there, and the run came back 41 again as though the fix had done nothing.
SEMANTIC_SETTINGS = (
    # ClickHouse defaults join_use_nulls=0, which fills an unmatched outer-join cell with the
    # column's DEFAULT VALUE -- 0 for an integer key -- instead of NULL. Q13 counts exactly that:
    # `COUNT(o_orderkey)` over a LEFT OUTER JOIN, so every customer with no orders scored 1
    # instead of 0, collapsed into the c_count=1 bucket, and the c_count=0 group vanished. chDB
    # returned 41 rows where DuckDB, Polars and LakeSail all returned 42.
    #
    # Wrong in every published chDB result, at every scale factor, and invisible to a timing
    # chart. smoke_sql.py found it by making all four engines read identical local parquet.
    "SET join_use_nulls = 1",
)


class ChdbIceberg:
    name = "chdb_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._session = None

    @property
    def version(self) -> str:
        import chdb

        return chdb.__version__

    def _write_config(self, scratch: pathlib.Path) -> pathlib.Path:
        """The ClickHouse server config: cache location and size, spill path, memory ceiling.

        The notebook asked for a 150GiB cache under /mnt/notebookfusetmp, a Fabric-only path. On a
        runner both halves are wrong: the directory does not exist, and 150GiB is ten times the
        whole disk. ClickHouse does NOT check free space before filling the cache, so an
        oversized max_size is an ENOSPC in the middle of a query rather than an eviction.
        """
        cache_dir = scratch / "cache"
        tmp_dir = scratch / "tmp"
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = scratch / "config.xml"
        cfg_path.write_text(
            f"""<clickhouse>
    <tmp_path>{tmp_dir}/</tmp_path>
    <max_server_memory_usage>11000000000</max_server_memory_usage>
    <filesystem_caches>
        <onelake_cache>
            <path>{cache_dir}/onelake</path>
            <max_size>{chdb_cache_gib(self.cfg.sf)}Gi</max_size>
        </onelake_cache>
    </filesystem_caches>
</clickhouse>""",
            encoding="utf-8",
        )
        return cfg_path

    def setup(self) -> None:
        from chdb import session

        scratch = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "chdb"
        cfg_path = self._write_config(scratch)
        self._session = session.Session(f"{scratch}/bench?config-file={cfg_path}")

        for statement in (
            f"SET {GATE} = 1",
            "SET filesystem_cache_name = 'onelake_cache'",
            # 0 means "pick automatically". Left from the notebook: on a 4-core box an explicit
            # download thread count mostly fights the query threads.
            "SET max_download_threads = 0",
            f"SET iceberg_metadata_staleness_ms = {CATALOG_CACHE_SECONDS * 1000}",
            *SEMANTIC_SETTINGS,
            "SET max_threads = 4",
            "SET max_memory_usage = 10000000000",
            # THE SETTINGS THAT DECIDE WHETHER SF>=10 FINISHES. Q9 and Q21 build hash tables over
            # lineitem that do not fit in 16GB. Without grace_hash and the external thresholds
            # they do not spill, they raise MEMORY_LIMIT_EXCEEDED.
            "SET max_bytes_before_external_group_by = 5000000000",
            "SET max_bytes_before_external_sort = 5000000000",
            "SET join_algorithm = 'grace_hash,hash'",
        ):
            self._session.query(statement)

        # NOT logged, at any verbosity: this statement contains the bearer token.
        token = auth.onelake_token()
        self._session.query(
            f"""
            CREATE DATABASE {DB}
            ENGINE = DataLakeCatalog('{ICEBERG_ENDPOINT}')
            SETTINGS catalog_type = 'onelake',
                     warehouse = '{self.cfg.warehouse}',
                     onelake_bearer_token = '{token}'
            """
        )
        self._session.query(f"USE {DB}")
        scrub.safe_print(
            f"  chdb {self.version} attached, cache {chdb_cache_gib(self.cfg.sf)}Gi at {scratch}"
        )

    def execute(self, sql: str) -> int:
        """Run and count.

        JSONCompact, not the notebook's 'Pretty': Pretty renders an ASCII table inside the timed
        window, and echoes the statement back on error -- which for the attach would put the
        token in the log.
        """
        import json

        result = self._session.query(sql, "JSONCompact")
        payload = json.loads(str(result))
        return len(payload.get("data", []))

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None
