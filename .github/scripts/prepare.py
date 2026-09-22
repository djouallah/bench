"""Generate the suite's dataset into OneLake, once, before the matrix fans out.

THE SINGLE WRITER. Four matrix jobs calling generate() would race: `create_table_if_not_exists`
is check-then-act with a multi-minute window between the check and the `add_files` commit, so two
jobs can both decide the table is missing, both create it, and both register the same files. That
is a corrupt table, not a slow one. The fix is structural -- generation happens here, in a job
with no matrix, and the bench jobs are read-only.

WHICH SUITE is BENCH_SUITE's call (bench/suite.py). TPC-H generates with tpchgen-cli and registers
the parquet through pyiceberg; TPC-DS generates with DuckDB's dsdgen and has DuckDB write the
tables itself. Same marker, same idempotency, two generators.
"""

from __future__ import annotations

import sys

from bench.suite import suite_class

if __name__ == "__main__":
    cfg = suite_class().from_env()
    if cfg.TEST == "tpcds":
        from bench.tpcds.generate import generate
    else:
        from bench.tpch.generate import generate
    stats = generate(cfg)
    print(f"::notice::{cfg.schema} ready (skipped={stats['skipped']})")
    sys.exit(0)
