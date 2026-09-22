"""Generate the TPC-H dataset into OneLake, once, before the matrix fans out.

THE SINGLE WRITER. Four matrix jobs calling generate() would race: `create_table_if_not_exists`
is check-then-act with a multi-minute window between the check and the `add_files` commit, so two
jobs can both decide the table is missing, both create it, and both register the same files. That
is a corrupt table, not a slow one. The fix is structural -- generation happens here, in a job
with no matrix, and the bench jobs are read-only.
"""

from __future__ import annotations

import sys

from bench.config import Config
from bench.tpch.generate import generate

if __name__ == "__main__":
    cfg = Config.from_env()
    stats = generate(cfg)
    print(f"::notice::{cfg.schema} ready (skipped={stats['skipped']})")
    sys.exit(0)
