"""DuckDB with the external file cache off. Everything else is DuckDBIceberg.

The control for DuckDB's warm pass: same wheel, same transport, same secret, same catalog cache
(`MAX_TABLE_STALENESS` is the fairness setting every engine gets, and it stays on), same
dialect. The one difference is `SET GLOBAL enable_external_file_cache = false`, issued first in
the session, before the ATTACH, so not even the metadata reads the attach performs are kept.
tests/test_engines.py asserts that the two engines' setup SQL differs by exactly that line.

A separate engine rather than a flag on the DuckDB one because it is a separate bar: the results
JSON, the charts and the tables are keyed by engine name, and the question this answers -- how
much of that cold-to-warm gap is the cache -- needs both bars on one chart.
"""

from __future__ import annotations

from bench.tpch.engines.duckdb_iceberg import DuckDBIceberg


class DuckDBNoCacheIceberg(DuckDBIceberg):
    name = "duckdb_nocache_iceberg"
    EXTERNAL_FILE_CACHE = False
