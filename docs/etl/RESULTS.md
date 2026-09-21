# Light ETL results

1000 AEMO daily CSV files read from OneLake, filtered, cast and written as one Iceberg table per engine, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-21T08:59:36Z` · commit `bb9d5c9` · [Actions run](https://github.com/djouallah/onelake-iceberg/actions/runs/35580753996)

## Latest run

| Engine | Version | Load | Attach | Rows | Error |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 414.3s | 5.3s | 149,146,763 | — |
| Polars | `2.0.0-rc.2` | 479.7s | 1.0s | 149,146,763 | — |
| Daft | `0.7.25` | 649.3s | 1.8s | 149,146,763 | — |
| chDB | `4.4.0` | 768.2s | 2.3s | 149,146,763 | — |
| LakeSail | `0.7.1` | 815.0s | 0.8s | 149,146,763 | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 918.2s | 11.7s | 149,146,763 | — |

Load = drop and create the table, read the CSVs, transform, write, commit. Attach = session start and catalog attach, timed separately and excluded from Load. Rows is the count read back from the table afterwards; every engine applies the same filter, so they agree.

## History

48 timed rows across 4 runs.
Raw data: one immutable JSON per run under [`results/etl/`](../../results/etl/), flattened to [`data/etl_results.csv`](../data/etl_results.csv).

