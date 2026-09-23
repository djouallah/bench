"""TEMPORARY (delete after use). TPC-DS Q71 on Sail over Iceberg tables, without OneLake.

DuckDB's dsdgen writes a tiny TPC-DS, pyiceberg lands Q71's six tables as Iceberg (so every
parquet column carries a PARQUET:field_id, as on OneLake), and Sail reads them by location.
Then the exact Q71 text from sql/tpcds.sql, plus narrowed variants to find the trigger.
"""

import os
import sys
import tempfile
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

os.environ["BENCH_SUITE"] = "tpcds"
sys.path.insert(0, os.getcwd())
from bench.suite import suite_class  # noqa: E402
from bench.tpch import queries  # noqa: E402

TABLES = ("item", "date_dim", "time_dim", "web_sales", "catalog_sales", "store_sales")

con = duckdb.connect()
con.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = 0.1)")

wh = Path(tempfile.mkdtemp())
cat = SqlCatalog("repro", uri=f"sqlite:///{wh}/cat.db", warehouse=wh.as_uri())
cat.create_namespace("ns")
locations = {}
for name in TABLES:
    data = con.execute(f"SELECT * FROM {name}").to_arrow_table()
    table = cat.create_table(f"ns.{name}", data.schema)
    table.append(data)
    locations[name] = table.location()
    print(f"iceberg ns.{name}: {data.num_rows} rows")

server = SparkConnectServer()
server.start()
_, port = server.listening_address
spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
for name, location in locations.items():
    spark.read.format("iceberg").load(location).createOrReplaceTempView(name)

suite = suite_class()
q71 = queries.load("lakesail_iceberg", "ns", 1, suite.SQL_PATH, suite.N_QUERIES)[70]
q71 = q71.replace("ns.", "")

branch = """SELECT {p}_ext_sales_price AS ext_price, {p}_item_sk AS sold_item_sk
            FROM {t}, date_dim WHERE d_date_sk = {p}_sold_date_sk AND d_moy = 11 AND d_year = 1999"""
union3 = " UNION ALL ".join(
    branch.format(p=p, t=t)
    for p, t in (("ws", "web_sales"), ("cs", "catalog_sales"), ("ss", "store_sales"))
)
cases = {
    "Q71 exact": q71,
    "union of 3 joins, sum(decimal)": f"SELECT sum(ext_price) FROM ({union3})",
    "union of 3 scans, sum(decimal)": "SELECT sum(p) FROM (SELECT ws_ext_sales_price p FROM web_sales "
    "UNION ALL SELECT cs_ext_sales_price FROM catalog_sales UNION ALL SELECT ss_ext_sales_price FROM store_sales)",
}
failed = 0
for label, sql in cases.items():
    try:
        rows = spark.sql(sql).collect()
        print(f"OK    {label}: {len(rows)} rows")
    except Exception as exc:  # noqa: BLE001
        failed += 1
        print(f"FAIL  {label}: {' '.join(str(exc).split())[:700]}")
print(f"{failed} of {len(cases)} failed")
os._exit(1 if failed else 0)  # skip the client's shutdown retries (they cost ~10 min last time)
