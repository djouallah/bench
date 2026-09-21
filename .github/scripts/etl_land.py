"""Land N AEMO daily CSVs into the lakehouse. Idempotent. The `land` job of etl.yml.

THE SINGLE WRITER of `Files/csv`, run once per workflow before any engine job starts, so seven
matrix jobs never race to download the same file. A lakehouse that already holds N files costs
one listing and nothing else.

Fails the job -- and so the workflow -- if the lakehouse cannot be listed or the landing comes up
short. Seven engine jobs starting against an input that is not there is the failure shape the
reference repo's `check_landing.py` exists to prevent; here the landing and the check are one
step.
"""

from __future__ import annotations

from bench import scrub
from bench.etl.config import EtlConfig
from bench.etl.landing import land

if __name__ == "__main__":
    cfg = EtlConfig.from_env()
    scrub.safe_print(f"landing {cfg.files} AEMO daily file(s) into {cfg.csv_abfss}")
    summary = land(cfg)
    print(
        f"::notice::{cfg.schema}: {summary['present']} csv file(s) under Files/csv "
        f"(landed={summary['landed']}, {summary['bytes'] / 2**30:.2f} GB this run)"
    )
