"""Which query suite a CI job is running: the config class for BENCH_SUITE.

bench.yml sets BENCH_SUITE=tpch and tpcds.yml sets BENCH_SUITE=tpcds; the same five scripts
(prepare, run_engine, publish, smoke_sql, smoke_catalog) serve both by asking here. The ETL is
not a query suite and has its own scripts.

Importing both config modules is cheap: neither imports an engine.
"""

from __future__ import annotations

import os

from bench.config import Config
from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import TpchConfig

SUITES: dict[str, type[Config]] = {"tpch": TpchConfig, "tpcds": TpcdsConfig}


def suite_class(name: str | None = None) -> type[Config]:
    """The config class for suite `name`, or for $BENCH_SUITE, or TPC-H."""
    name = name or os.environ.get("BENCH_SUITE", "tpch")
    try:
        return SUITES[name]
    except KeyError:
        raise ValueError(f"unknown suite {name!r}; expected one of {sorted(SUITES)}") from None
