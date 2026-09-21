"""Run one ETL engine and write its slice as an artifact.

READS `Files/csv` and WRITES `T{n}.<engine>` in OneLake, plus exactly one local JSON file for
`publish` to merge. The file list is resolved here, untimed, and printed with its size so the
log says what was loaded.

EXIT CODE, same rule as run_engine.py: a load that failed is data and is recorded in the part,
but this job still exits 1 -- with one statement per engine there is no "completed 20 of 22";
a failed load is a broken configuration, not a measurement, and a green check on it would be a
lie. `publish` still runs (`if: always()`) and still publishes the engines that did load.
"""

from __future__ import annotations

import sys
from pathlib import Path

from bench import scrub
from bench.etl.config import EtlConfig
from bench.etl.data import csv_names
from bench.etl.engines import get_engine
from bench.etl.runner import benchmark, load_row
from bench.store import host_facts, write_engine_part

if __name__ == "__main__":
    cfg = EtlConfig.from_env()
    if not cfg.engine:
        raise SystemExit("BENCH_ENGINE is not set")

    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    out_dir.mkdir(parents=True, exist_ok=True)

    files, gb = csv_names(cfg, cfg.files)
    scrub.safe_print(
        f"{cfg.engine} | {len(files)} csv files ({gb:.2f} GB) from {cfg.csv_abfss} -> {cfg.schema}"
    )
    result = benchmark(get_engine(cfg.engine, cfg), cfg, files)
    # Captured HERE, on the runner that did the work -- not in publish, which is a different
    # machine and would stamp the results with its own hardware.
    result.host = host_facts()
    path = write_engine_part(out_dir, cfg.engine, result)

    row = load_row(result)
    if result.status == "setup_failed":
        scrub.safe_print(f"\n{cfg.engine} setup_failed -> {path}")
        sys.exit(1)
    if row is None or row.status != "ok":
        scrub.safe_print(f"\n{cfg.engine} load FAILED -> {path}")
        scrub.safe_print(f"::error::{cfg.engine} could not load {len(files)} files; see the log")
        sys.exit(1)
    scrub.safe_print(f"\n{cfg.engine} ok | load={row.dur:,.1f}s rows={row.rows} | -> {path}")
    sys.exit(0)
