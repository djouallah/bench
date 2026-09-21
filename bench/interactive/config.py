"""The TPC-H side of the run configuration: engines, tables, the part plan, chDB's cache size.

All of this was bench/config.py until the ETL benchmark arrived. What both benchmarks share --
the OneLake endpoints, the catalog-cache lifetime, the DuckDB transport rule and `Config` itself
-- stayed there; what only the query benchmark needs is here.
"""

from __future__ import annotations

import math

ENGINES = (
    "duckdb_iceberg",
    "chdb_iceberg",
    "polars_iceberg",
    "lakesail_iceberg",
    "daft_iceberg",
    "pyspark_iceberg",
)

TABLES = ("lineitem", "orders", "partsupp", "part", "customer", "nation", "region", "supplier")

# Approximate parquet MiB per scale factor unit, measured from a real tpchgen-cli run. Used only
# to choose a part count; being off by 30% moves a file from 200MB to 260MB and changes nothing.
PARQUET_MB_PER_SF = {
    "lineitem": 180,
    "orders": 40,
    "partsupp": 35,
    "customer": 12,
    "part": 7,
    "supplier": 1,
    "nation": 0,
    "region": 0,
}

# Target size of one uploaded parquet file.
#
# NOT the notebook's rule. Cell 9 used `scaled(base, floor) = max(floor, int(base * sf / 1000))`,
# tuned for SF=1000 on an 8-vCore Fabric node. At SF=10 it yields TWO lineitem parts of ~900MB
# each: file-level parallelism in every engine here collapses to 2, and each part's
# generate-then-upload cycle becomes a multi-minute serial stall because the semaphore can only
# overlap whole parts. Targeting a size instead keeps files at a shape readers like at every SF.
TARGET_PART_MB = 200

# Floors, so the smoke path still exercises multi-file reading. At SF=1 a size rule alone would
# give one part for everything and the SF=1 run would not resemble the SF=10 run it is screening.
PART_FLOOR = {"lineitem": 2, "orders": 2}
MAX_PARTS = 64


def parts_for(table: str, sf: int) -> int:
    """How many parts tpchgen-cli should split `table` into at scale factor `sf`."""
    want = math.ceil(sf * PARQUET_MB_PER_SF[table] / TARGET_PART_MB)
    return max(PART_FLOOR.get(table, 1), min(want, MAX_PARTS))


def parts_plan(sf: int) -> dict[str, int]:
    """Part counts for every table, in generation order.

    ORDER MATTERS and is load-bearing: `supplier` is last because it carries the
    generation-complete marker (see generate.py). Do not sort this dict.
    """
    return {t: parts_for(t, sf) for t in TABLES}


def estimated_gib(sf: int) -> float:
    """Total parquet the dataset occupies in OneLake, GiB."""
    return sum(PARQUET_MB_PER_SF.values()) * sf / 1024


def chdb_cache_gib(sf: int) -> int:
    """Size for chDB's filesystem cache.

    The notebook asked for 150Gi, which was fine on a Fabric node and is 10x the runner's entire
    disk. ClickHouse does NOT check free space before filling this cache, so a max_size larger
    than the disk is an ENOSPC in the middle of a query rather than an eviction. 1.5x the dataset
    gives the warm run somewhere to hit; the clamp keeps it inside a 14GB disk with room for
    spills and the OS.
    """
    return max(2, min(math.ceil(estimated_gib(sf) * 1.5), 8))
