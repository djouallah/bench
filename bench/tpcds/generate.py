"""Generate TPC-DS at scale factor N into OneLake as Iceberg, using DuckDB for both halves.

WHY DUCKDB AND NOT tpcgen. The tpcgen-rs project (the tpchgen-cli that bench/tpch/generate.py
runs) added TPC-DS in its v3.0.0, but as of 2026-09-22 the binary that carries it, `tpcgen-cli`,
is not on PyPI, the crates.io build is 0.1.0-alpha.1 without `--parts` for TPC-DS, and multi-part
TPC-DS output has an open correctness issue. DuckDB's `tpcds` extension is a pip install away,
deterministic, and `CALL dsdgen(sf = 10)` is the whole generator.

WHY DUCKDB WRITES THE TABLES TOO. The TPC-H generator uploads tpchgen-cli's parquet as-is and
registers it with pyiceberg `add_files`, after rewriting every file to carry field ids for
Polars. Here there is no parquet to upload: dsdgen fills a local DuckDB database, and

    CREATE TABLE onelake.DS0010.store_sales AS SELECT * FROM main.store_sales

through the write-capable ATTACH the ETL benchmark already uses (bench/duckdb_onelake.py) has
DuckDB's own Iceberg writer produce the files, the field ids and the commit. One tool, one code
path, nothing rewritten. The file layout is whatever that writer chooses; the log prints it per
table (files, average size) so the first run says whether store_sales landed as a few whole
files or as fragments, which is what the next reader pays for.

RUNNER BUDGET. dsdgen(sf=10) is ~3-4 GB of DuckDB storage on a 14 GB disk / 16 GB RAM runner.
The database is FILE-BACKED so generation spills instead of dying, and memory_limit sits below
the runner's RAM to leave room for the azure extension's buffers and the process itself. SF=30
does not fit; tpcds.yml offers 1 and 10.

IDEMPOTENT the way the TPC-H generate is, through the same two functions: the completion marker
is the same table property, written on this suite's MARKER_TABLE (`web_site`, last in TABLES),
and a table that already has data files is skipped so a crashed run resumes rather than
restarts. A table that exists EMPTY -- created by a run that died before its data landed -- is
dropped and written again; the TPC-H generator documents why an empty husk must never count.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

from bench import auth, scrub
from bench.duckdb_onelake import CATALOG, attach
from bench.etl.iceberg import layout
from bench.tpcds.config import TpcdsConfig
from bench.tpch.generate import _mark_complete, is_complete

# Below the runner's 16 GB: the azure extension, the Python process and the OS need the rest.
MEMORY_LIMIT = "10GB"
THREADS = 4


def _log(message: str) -> None:
    scrub.safe_print(message)


def _scratch() -> Path:
    """Where the local database and DuckDB's spill files live: the runner's temp, gone with it."""
    root = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "tpcds"
    (root / "tmp").mkdir(parents=True, exist_ok=True)
    return root


def load_tpcds_extension(con) -> None:
    """INSTALL and LOAD `tpcds`, falling back to the nightly repository.

    The DuckDB engine jobs pin a pre-release wheel (`duckdb>=2.0.0.dev0`), and a dev build's
    extensions are served from `core_nightly` rather than the default repository. Trying the
    default first keeps a stable wheel -- the smoke test's, or a laptop's -- on the release
    extension. Shared with .github/scripts/smoke_sql.py, which generates SF=1 locally.
    """
    try:
        con.sql("INSTALL tpcds")
    except Exception as exc:  # noqa: BLE001 - the fallback is the point
        _log(f"  INSTALL tpcds failed ({scrub.scrub_exc(exc, 160)}); trying core_nightly")
        con.sql("INSTALL tpcds FROM core_nightly")
    con.sql("LOAD tpcds")


def _has_data(catalog, identifier: str) -> bool:
    if not catalog.table_exists(identifier):
        return False
    return any(True for _ in catalog.load_table(identifier).scan().plan_files())


def generate(cfg: TpcdsConfig, force: bool = False) -> dict:
    """Generate every TPC-DS table for `cfg.sf` into OneLake. Idempotent."""
    import duckdb

    catalog = auth.catalog(cfg)
    if not force and is_complete(catalog, cfg):
        _log(f"{cfg.schema} already generated at SF={cfg.sf} - skipping")
        return {"sf": cfg.sf, "skipped": True, "namespace": cfg.schema, "tables": {}}

    scratch = _scratch()
    con = duckdb.connect(str(scratch / f"sf{cfg.sf}.duckdb"))
    con.sql(
        f"SET memory_limit = '{MEMORY_LIMIT}'; SET threads = {THREADS}; "
        f"SET temp_directory = '{(scratch / 'tmp').as_posix()}'"
    )
    load_tpcds_extension(con)

    _log(f"Generating TPC-DS SF={cfg.sf} with dsdgen under {scratch}")
    overall = time.perf_counter()
    started = time.perf_counter()
    con.sql(f"CALL dsdgen(sf = {cfg.sf})")
    gen_s = time.perf_counter() - started
    _log(f"  dsdgen {gen_s:.1f}s")

    # Minted late, right before the attach: the token is baked into ATTACH and never refreshed,
    # so every second between minting and attaching is a second off the write's budget.
    attach(con, cfg, auth.onelake_token())
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{cfg.schema}")

    results: dict[str, dict] = {}
    totals = {"gen_s": gen_s, "write_s": 0.0, "rows": 0}
    for table in cfg.TABLES:
        identifier = f"{cfg.schema}.{table}"
        qualified = f"{CATALOG}.{cfg.schema}.{table}"
        rows = con.sql(f"SELECT count(*) FROM main.{table}").fetchone()[0]
        if not force and _has_data(catalog, identifier):
            _log(f"--- {table}: already has data files, skipped")
            results[table] = {"rows": rows, "skipped": True}
            continue
        if catalog.table_exists(identifier):
            _log(f"--- {table}: exists with no data files (a husk), dropping")
            con.sql(f"DROP TABLE {qualified}")

        _log(f"--- {table} ({rows:,} rows) ---")
        started = time.perf_counter()
        con.sql(f"CREATE TABLE {qualified} AS SELECT * FROM main.{table}")
        write_s = time.perf_counter() - started
        written = None
        try:
            written = layout(catalog.load_table(identifier))
        except Exception as exc:  # noqa: BLE001 - a check on the result, not the result
            _log(f"  warning: layout read failed: {scrub.scrub_exc(exc, 200)}")
        _log(f"  written in {write_s:.1f}s" + (f": {written}" if written else ""))
        results[table] = {"rows": rows, "write_s": round(write_s, 1), "layout": written}
        totals["write_s"] += write_s
        totals["rows"] += rows

    con.close()
    _mark_complete(catalog, cfg)
    elapsed = time.perf_counter() - overall
    _log(
        f"total {elapsed:.1f}s wall | dsdgen {totals['gen_s']:.1f}s | "
        f"write {totals['write_s']:.1f}s | {totals['rows']:,} rows"
    )
    return {
        "sf": cfg.sf,
        "skipped": False,
        "namespace": cfg.schema,
        "tables": results,
        "elapsed_s": elapsed,
        **totals,
    }


if __name__ == "__main__":  # pragma: no cover
    sys.exit(0 if generate(TpcdsConfig.from_env()) else 1)
