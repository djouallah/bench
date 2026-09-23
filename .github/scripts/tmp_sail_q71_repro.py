"""TEMPORARY (delete after use). Repro for Sail "Physical input schema" on a UNION of
parquet sources whose columns carry different PARQUET:field_id metadata (what Iceberg writers
produce), followed by an aggregate -- the shape of TPC-DS Q71."""

import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession


def write(path: Path, first_id: int) -> None:
    schema = pa.schema(
        [
            pa.field("item_sk", pa.int64(), metadata={b"PARQUET:field_id": str(first_id).encode()}),
            pa.field(
                "price", pa.float64(), metadata={b"PARQUET:field_id": str(first_id + 1).encode()}
            ),
        ]
    )
    pq.write_table(pa.table({"item_sk": [1, 2, 1], "price": [1.0, 2.0, 3.0]}, schema=schema), path)


tmp = Path(tempfile.mkdtemp())
write(tmp / "a.parquet", 1)
write(tmp / "b.parquet", 15)
write(tmp / "c.parquet", 30)

server = SparkConnectServer()
server.start()
_, port = server.listening_address
spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()

for name in "abc":
    spark.read.parquet(str(tmp / f"{name}.parquet")).createOrReplaceTempView(name)

cases = {
    "union of two, aggregate": "SELECT item_sk, sum(price) FROM "
    "(SELECT item_sk, price FROM a UNION ALL SELECT item_sk, price FROM b) GROUP BY item_sk",
    "union of three, aggregate (Q71 shape)": "SELECT item_sk, sum(price) FROM "
    "(SELECT item_sk, price FROM a UNION ALL SELECT item_sk, price FROM b "
    "UNION ALL SELECT item_sk, price FROM c) GROUP BY item_sk",
    "union, no aggregate": "SELECT item_sk, price FROM a UNION ALL SELECT item_sk, price FROM b",
}
# THE ICEBERG CASE: two tables written by pyiceberg (field ids 1-2 and 3-4 by construction of
# different schemas), read by Sail through format("iceberg"), unioned and aggregated.
try:
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    wh = Path(tempfile.mkdtemp())
    cat = SqlCatalog("repro", uri=f"sqlite:///{wh}/cat.db", warehouse=wh.as_uri())
    cat.create_namespace("ns")
    s1 = Schema(NestedField(1, "item_sk", LongType()), NestedField(2, "price", DoubleType()))
    s2 = Schema(
        NestedField(1, "pad", StringType()),
        NestedField(2, "item_sk", LongType()),
        NestedField(3, "price", DoubleType()),
    )
    t1 = cat.create_table("ns.t1", s1)
    t1.append(pa.table({"item_sk": [1, 2], "price": [1.0, 2.0]}))
    t2 = cat.create_table("ns.t2", s2)
    t2.append(pa.table({"pad": ["x"], "item_sk": [1], "price": [3.0]}))
    spark.read.format("iceberg").load(t1.location()).createOrReplaceTempView("i1")
    spark.read.format("iceberg").load(t2.location()).createOrReplaceTempView("i2")
    cases["iceberg: union, aggregate"] = (
        "SELECT item_sk, sum(price) FROM "
        "(SELECT item_sk, price FROM i1 UNION ALL SELECT item_sk, price FROM i2) GROUP BY item_sk"
    )
except Exception as exc:  # noqa: BLE001
    print(f"SETUP iceberg case failed: {' '.join(str(exc).split())[:400]}")

failed = 0
for label, sql in cases.items():
    try:
        rows = spark.sql(sql).collect()
        print(f"OK    {label}: {len(rows)} rows")
    except Exception as exc:  # noqa: BLE001
        failed += 1
        print(f"FAIL  {label}: {str(exc).splitlines()[0][:300]}")
        print("      " + " ".join(str(exc).split())[:400])
server.stop()
sys.exit(1 if failed else 0)
