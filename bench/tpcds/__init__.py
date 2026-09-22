"""The TPC-DS query benchmark: 99 statements, cold then warm, on the same seven engines.

The second query suite. It owns only what differs from TPC-H -- its config (24 tables, its
namespace, sql/tpcds.sql) and its generator (DuckDB's dsdgen, written to OneLake by DuckDB's
own Iceberg writer). The runner, the query loader, the engines and the charts are bench/tpch's,
which read the suite off the config they are handed.
"""
