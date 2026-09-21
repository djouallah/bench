# Light ETL results

100 AEMO daily CSV files read from OneLake, filtered, cast and written as one Iceberg table per engine, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-21T11:30:21Z` · commit `44dd28a` · [Actions run](https://github.com/djouallah/onelake-iceberg/actions/runs/35591414961)

## Latest run

| Engine | Version | Load | Attach | Rows | Error |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 35.8s | 4.1s | 13,876,896 | — |
| Polars | `2.0.0-rc.2` | 53.2s | 1.5s | 13,876,896 | — |
| chDB | `4.4.0` | 76.0s | 1.8s | 13,876,896 | — |
| LakeSail | `0.7.1` | 76.0s | 0.6s | 13,876,896 | — |
| Daft | `0.7.25` | 78.8s | 1.9s | 13,876,896 | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 124.3s | 16.7s | 13,876,896 | — |

Load = drop and create the table, read the CSVs, transform, write, commit. Attach = session start and catalog attach, timed separately and excluded from Load. Rows is the count read back from the table afterwards; every engine applies the same filter, so they agree.

## History

60 timed rows across 5 runs.
Raw data: one immutable JSON per run under [`results/etl/`](../../results/etl/), flattened to [`data/etl_results.csv`](../data/etl_results.csv).

