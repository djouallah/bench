"""StarRocks against OneLake: the container, the connection, the catalog. Shared by the query
engine (bench/tpch/engines/starrocks_iceberg.py), the ETL engine and the candidate probe.

A SERVER, NOT A LIBRARY. There is no pip install: StarRocks is a Java front end (planner,
catalog) and a C++ back end (execution), run here as the single-node `allin1` image, and Python
talks to it over the MySQL protocol. `start()` brings the container up and waits for the back end
to register -- the FE answers `SELECT 1` well before a query can run. The image is pulled by the
workflow before the engine is timed, so the setup row measures a cold start, not a download.

HOW IT READS ONELAKE, each line found by a failed CI run (candidate_engine.yml, 2026-09-26):

* CATALOG: the REST catalog with the bearer as a fixed `oauth2.token`. It cannot be refreshed in
  place, so `attach()` is re-run on a fresh bearer between statements when it runs low
  (`needs_refresh`), exactly like Spark's session restart. StarRocks' metadata caches are left at
  their defaults: ~24 h in memory, above the bench's CATALOG_CACHE_SECONDS, so no run outlives
  them. The re-attach empties them, and that cost stays in the numbers.
* STORAGE: hadoop-azure's `WorkloadIdentityTokenProvider` reading the GitHub OIDC assertion from a
  file mounted into the container and rewritten every 4 minutes -- Spark-OSS's credential, and
  refreshable for any run length. Vended credentials attach but never reach the reader.
* NEVER `azure.adls2.storage_account`: StarRocks turns it into `<account>.dfs.core.windows.net`,
  the wrong host for onelake.dfs.fabric.microsoft.com, and every read falls back to SharedKey
  ("fs.azure.account.key" null). Left empty, the OAuth keys apply to any host.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, TOKEN_MIN_LIFETIME_SECONDS, Config

IMAGE = os.environ.get("STARROCKS_IMAGE", "starrocks/allin1-ubuntu:4.1-latest")
CONTAINER = "starrocks"
CATALOG = "onelake"
PORT = 9030

ASSERTION_DIR = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "starrocks-oidc"
ASSERTION_IN_CONTAINER = "/var/run/starrocks-oidc/assertion"
ASSERTION_REFRESH_S = 240

_refresher: threading.Thread | None = None


def _write_assertion() -> None:
    target = ASSERTION_DIR / "assertion"
    target.write_text(auth._github_oidc_assertion(), encoding="utf-8")
    target.chmod(0o644)  # the container's user is not the runner's


def _keep_assertion_fresh() -> None:
    """Write the OIDC assertion now and every 4 minutes; it lives about five.

    Only in a job that can mint one (`id-token: write`). The smoke test's local phase has no
    credentials by design and reads mounted parquet, so it runs with the directory empty.
    """
    global _refresher
    ASSERTION_DIR.mkdir(parents=True, exist_ok=True)
    if not os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
        return
    _write_assertion()
    if _refresher is not None:
        return

    def loop() -> None:
        while True:
            time.sleep(ASSERTION_REFRESH_S)
            try:
                _write_assertion()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                scrub.safe_print(f"  warning: assertion refresh failed: {exc}")

    _refresher = threading.Thread(target=loop, daemon=True)
    _refresher.start()


def _running() -> bool:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
        capture_output=True,
        text=True,
        check=False,
    )
    return out.stdout.strip() == "true"


def start(timeout_s: int = 300, mounts: dict[Path, str] | None = None) -> None:
    """The container, up with a live back end. Idempotent: a running container is reused.

    `mounts` maps host directories to read-only paths in the container, for `file://` reads.
    """
    _keep_assertion_fresh()
    if not _running():
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
        volumes = {ASSERTION_DIR: str(Path(ASSERTION_IN_CONTAINER).parent), **(mounts or {})}
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                *[arg for host, inside in volumes.items() for arg in ("-v", f"{host}:{inside}:ro")],
                "-p",
                f"127.0.0.1:{PORT}:9030",
                "-p",
                "127.0.0.1:8030:8030",
                "-p",
                "127.0.0.1:8040:8040",
                IMAGE,
            ],
            check=True,
            capture_output=True,
        )
    deadline = time.time() + timeout_s
    while True:
        try:
            conn = connect()
            with conn.cursor() as cur:
                cur.execute("SHOW BACKENDS")
                if any("true" in map(str, row) for row in cur.fetchall()):
                    conn.close()
                    return
            conn.close()
        except Exception:  # noqa: BLE001 - not up yet
            pass
        if time.time() > deadline:
            raise RuntimeError(f"StarRocks back end not alive after {timeout_s}s")
        time.sleep(3)


def connect():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1", port=PORT, user="root", password="", autocommit=True, read_timeout=None
    )


def version(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_version()")
        return str(cur.fetchone()[0])


def storage_properties() -> str:
    """hadoop-azure credentials for OneLake, as StarRocks properties (see the module docstring)."""
    return (
        f'"azure.adls2.oauth2_token_file"="{ASSERTION_IN_CONTAINER}", '
        f'"azure.adls2.oauth2_tenant_id"="{os.environ.get("AZURE_TENANT_ID", "")}", '
        f'"azure.adls2.oauth2_client_id"="{os.environ.get("AZURE_CLIENT_ID", "")}"'
    )


def attach(conn, cfg: Config, token: str) -> None:
    """(Re)create the OneLake catalog on `token` and make it the session's current catalog."""
    with conn.cursor() as cur:
        cur.execute(f"DROP CATALOG IF EXISTS {CATALOG}")
        cur.execute(
            f"CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES ("
            '"type"="iceberg", "iceberg.catalog.type"="rest", '
            f'"iceberg.catalog.uri"="{ICEBERG_ENDPOINT}", '
            f'"iceberg.catalog.warehouse"="{cfg.warehouse}", '
            f'"iceberg.catalog.security"="oauth2", "iceberg.catalog.oauth2.token"="{token}", '
            f'"iceberg.catalog.vended-credentials-enabled"="false", {storage_properties()})'
        )
        cur.execute(f"SET CATALOG {CATALOG}")
        # No per-statement ceiling: the job's own timeout is the bound, as for every engine.
        cur.execute("SET query_timeout = 86400")
        # SPILL IS OFF BY DEFAULT in StarRocks (`enable_spill`, "Default: false"): an aggregation,
        # join or sort that outgrows memory fails instead of spilling. TPC-H SF=100 lost Q18 and
        # Q21 to "Memory of process exceed limit" that way (run 36227016523), while DuckDB and
        # Gluten, which spill by default, finish all 22. `spill_mode` stays at its default, auto.
        cur.execute("SET enable_spill = true")


def needs_refresh(expires: float) -> bool:
    return expires - time.time() < TOKEN_MIN_LIFETIME_SECONDS
