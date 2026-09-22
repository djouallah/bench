"""The TPC-DS side of the run configuration: the 24 tables, the namespace, where results go.

`TpcdsConfig` IS a `bench.config.Config` with the suite constants that bench/tpch/config.py's
`TpchConfig` documents, plus two things redefined:

* `schema` is `DS{sf:04d}` -- DS0001, DS0010 -- next to TPC-H's CH0010 in the same lakehouse, so
  the two suites can never read each other's tables.
* `from_env` reads TPCDS_SF, so tpcds.yml and bench.yml can be dispatched at different scales.

The engine list is TPC-H's: same seven, same identifiers, so bench/charts.py's labels and colours
apply and an engine looks the same in every picture.
"""

from __future__ import annotations

from dataclasses import dataclass

from bench.config import SQL_DIR, Config
from bench.tpch.config import ENGINES

# The 24 tables of the spec (dsdgen also emits `dbgen_version`, which is not one), LARGEST FIRST
# so that a generator or a writer that is going to fail on size fails in the first minutes, not
# the last. `web_site` is last on purpose: it carries the generation-complete marker (see
# bench/tpch/generate.py), and the marker must be the last thing written.
TABLES = (
    "store_sales",
    "inventory",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "customer_demographics",
    "customer",
    "customer_address",
    "item",
    "date_dim",
    "time_dim",
    "promotion",
    "household_demographics",
    "catalog_page",
    "store",
    "web_page",
    "call_center",
    "warehouse",
    "reason",
    "ship_mode",
    "income_band",
    "web_site",
)

# Approximate parquet MiB per scale factor unit, for chDB's cache sizing only. TPC-DS at SF=10 is
# ~3 GB as parquet -- a little over TPC-H at the same SF -- and the clamp in chdb_cache_gib
# makes a 30% error here change nothing.
PARQUET_MB_PER_SF = 300

# The scale the headline docs are built at, and tpcds.yml's default. SF=30 does not fit the
# runner through DuckDB (bench/tpcds/generate.py), so the workflow offers 1 and 10 only.
HEADLINE_SF = 10


@dataclass(frozen=True)
class TpcdsConfig(Config):
    TEST = "tpcds"
    TITLE = "TPC-DS"
    SF_ENV = "TPCDS_SF"
    HEADLINE_SF = HEADLINE_SF
    ENGINES = ENGINES
    TABLES = TABLES
    SQL_PATH = SQL_DIR / "tpcds.sql"
    N_QUERIES = 99
    MARKER_TABLE = "web_site"
    # smoke_catalog.py's two reads: Q3 and Q42 both scan store_sales, the biggest fact table and
    # the one whose data files a credential problem hides behind.
    PROBE_QUERIES = (3, 42)
    RESULTS_DIR = "results/tpcds"
    DOCS_DIR = "docs/tpcds"
    CSV = "docs/data/tpcds_results.csv"

    @property
    def schema(self) -> str:
        """Iceberg namespace holding the TPC-DS tables: DS0001, DS0010."""
        return f"DS{self.sf:04d}"

    @property
    def estimated_gib(self) -> float:
        return PARQUET_MB_PER_SF * self.sf / 1024
