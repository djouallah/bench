"""Run one engine's cold and warm passes and write its slice as an artifact.

READ-ONLY against OneLake. Writes exactly one local file, which `publish` merges.

EXIT CODE, and the distinction matters more than it looks.

A failing QUERY is DATA and exits 0 -- "LakeSail completed 20 of 22, and here is why the other
two died" is a result worth publishing. A failing SETUP exits 1: an engine that could not attach
has nothing to say.

ZERO SUCCESSFUL QUERIES also exits 1, even though the attach worked, because that is never a
result -- it is a broken configuration wearing a result's clothes. This guard exists because a
run went GREEN with all 22 queries failing on

    IOException: AzureStorageFileSystem could not open file: 'abfss://.../Tables/...'

DuckDB had the catalog token but no storage secret, so it listed the tables happily and could not
read one byte of them. A green check mark on that is worse than a red one.
"""

from __future__ import annotations

import sys
from pathlib import Path

from bench import scrub
from bench.config import Config
from bench.store import host_facts, write_engine_part
from bench.tpch.engines import get_engine
from bench.tpch.runner import benchmark, totals

if __name__ == "__main__":
    cfg = Config.from_env()
    if not cfg.engine:
        raise SystemExit("BENCH_ENGINE is not set")

    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    out_dir.mkdir(parents=True, exist_ok=True)

    scrub.safe_print(f"{cfg.engine} | SF={cfg.sf} | namespace {cfg.schema}")
    result = benchmark(get_engine(cfg.engine, cfg), cfg)
    # Captured HERE, on the runner that did the work -- not in publish, which is a different
    # machine and would stamp the results with its own hardware.
    result.host = host_facts()
    path = write_engine_part(out_dir, cfg.engine, result)

    summary = " ".join(f"{k}={v:,.1f}s" for k, v in sorted(totals(result).items()))
    errors = sum(1 for row in result.rows if row.status == "error")
    passed = sum(1 for row in result.rows if row.phase == "query" and row.status == "ok")
    scrub.safe_print(f"\n{cfg.engine} {result.status} | {summary} | {errors} failed | -> {path}")

    if result.status == "setup_failed":
        sys.exit(1)
    if passed == 0:
        scrub.safe_print(
            f"::error::{cfg.engine} attached but completed 0 statements -- "
            f"a broken configuration, not a benchmark result"
        )
        sys.exit(1)
    sys.exit(0)
