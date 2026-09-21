"""Run configuration, assembled from the environment. Shared by both benchmarks.

REPLACES cells 1, 2 and 5 of the TPC-H notebook. Those three cells existed as a unit: cell 1 set
the constants, cell 2 wrote them to /tmp/tpch_params.json, cell 5 read them back -- a round-trip
whose ONLY purpose was to survive `notebookutils.session.restartPython()` in cell 3, which wipes
globals. Nothing restarts Python here (pip install happens in a workflow step before the
interpreter starts), so the whole dance collapses into reading env vars once.

WHAT IS HERE AND WHAT IS NOT. This module is what bench/interactive (the TPC-H queries) and
bench/etl (the CSV-to-Iceberg load) both need: the OneLake endpoints, the catalog-cache lifetime
every engine derives from, the DuckDB transport rule, and `Config` -- workspace, lakehouse,
scale, run identity. The TPC-H specifics (the engine list, the tables, the part plan, chDB's
cache size) are bench/interactive/config.py; the ETL's (file count, CSV paths) are
bench/etl/config.py, whose EtlConfig is a Config.
"""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass

ICEBERG_ENDPOINT = "https://onelake.table.fabric.microsoft.com/iceberg"
ONELAKE_DFS = "onelake.dfs.fabric.microsoft.com"
ONELAKE_BLOB = "onelake.blob.fabric.microsoft.com"
STORAGE_SCOPE = "https://storage.azure.com/.default"

# HOW LONG AN ENGINE MAY CACHE CATALOG METADATA. One number, four spellings.
#
# This is a FAIRNESS setting, not a tuning knob. An engine that re-resolves a table on every
# statement is timed on REST round-trips to Fabric while the others are timed on execution, and
# nothing in a timing chart would tell you that is what you are looking at.
#
# The defaults are wildly different -- Iceberg's Spark catalog expires after 30 SECONDS, Sail's
# is off entirely, and DuckDB and chDB were set to 10 by hand -- so leaving each engine on its
# own default silently benchmarks four different caching policies. Each engine below derives its
# own spelling from this constant; nobody writes a literal.
#
# IT DOES NOT EQUALISE SAIL, and it is worth knowing why before reading its numbers. DuckDB, chDB
# and Spark cache the LOADED TABLE for this long. Sail's `table_cache_*` options cache the
# namespace's table LISTING -- its docs say "table listing cache" -- and `get_table` and
# `begin_table_access` call `loadTable` on every statement regardless. So Sail alone still pays a
# REST round-trip per table per statement, which is the 1.5-3s floor under its small queries and
# where its 20-25s warm-pass stalls land. The setting is still applied to Sail, for the day it
# caches the loaded table. See RUN.md and https://github.com/lakehq/sail/issues/2629.
CATALOG_CACHE_SECONDS = 900  # 15 minutes


def azure_transport() -> str | None:
    """Which HTTP transport DuckDB's azure extension should use, or None to leave its default.

    THE SINGLE MOST EXPENSIVE THING TO GET WRONG HERE, because getting it wrong does not look
    like a transport problem. DuckDB's azure extension has its own HTTP stack, separate from the
    iceberg extension's. On a Linux runner its `default` transport fails the OneLake TLS
    handshake ("Problem with the SSL CA cert (path? access rights?)"), while `curl` respects the
    system CA bundle and works.

    So the Iceberg ATTACH SUCCEEDS -- that is a plain HTTPS REST call made by the iceberg
    extension -- and then every single data-file read fails with

        IOException: AzureStorageFileSystem could not open file: 'abfss://...'

    which reads exactly like a missing storage credential and is not one. A genuinely bad
    credential says `Unauthorized`.

    The Fabric notebook this repo was ported from never hit any of this: a notebook's default
    transport works. Nothing here runs in Fabric -- it is storage and nothing else -- which is
    why the bug only appeared once the benchmark moved onto a runner.

    Every benchmark run is a GitHub Linux runner, so `curl` is the answer that matters and
    bench.yml sets AZURE_TRANSPORT_OPTION_TYPE explicitly. The Windows branch exists only so that
    running an engine by hand on a developer's laptop works: DuckDB's bundled libcurl has no CA
    bundle there, so `curl` fails every handshake, while the default (WinHTTP) trusts the system
    cert store. Exactly backwards from Linux.

    An explicit AZURE_TRANSPORT_OPTION_TYPE always wins.
    """
    override = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if override:
        return override
    return None if platform.system() == "Windows" else "curl"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


@dataclass(frozen=True)
class Config:
    workspace_id: str
    lakehouse_id: str
    sf: int
    engine: str = ""
    run_id: str = "local"
    run_url: str = ""
    git_sha: str = ""

    # Runner-sized concurrency. The notebook's values (upload_threads=6, upload_concurrency=8,
    # PYICEBERG_MAX_WORKERS=16) were set for 8 vCores; on 4 they mostly contend.
    max_local_parts: int = 4
    upload_threads: int = 3
    upload_concurrency: int = 4
    pyiceberg_workers: int = 4

    @property
    def schema(self) -> str:
        """Iceberg namespace holding the TPC-H tables: CH0001, CH0010, CH0100.

        Kept identical to the notebook's `f'CH{SF:04d}'` ON PURPOSE. The owner still runs the
        Fabric notebook against the same lakehouse, so identical naming means both share generated
        data instead of each paying to generate its own. bench.etl.config.EtlConfig overrides this
        with the ETL notebook's `T{n}`, for the same reason.
        """
        return f"CH{self.sf:04d}"

    @property
    def warehouse(self) -> str:
        """What the Iceberg REST catalog calls `warehouse`: GUID/GUID. A name does not resolve."""
        return f"{self.workspace_id}/{self.lakehouse_id}"

    @property
    def base_path(self) -> str:
        return f"abfss://{self.workspace_id}@{ONELAKE_DFS}/{self.lakehouse_id}"

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            lakehouse_id=os.environ["FABRIC_LAKEHOUSE_ID"],
            sf=_env_int("TPCH_SF", 10),
            engine=os.environ.get("BENCH_ENGINE", ""),
            run_id=os.environ.get("GITHUB_RUN_ID", "local"),
            run_url=os.environ.get("BENCH_RUN_URL", ""),
            git_sha=os.environ.get("GITHUB_SHA", "")[:7],
        )
