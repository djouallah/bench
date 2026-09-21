# Results

TPC-H-like, scale factor 10, 22 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-21T10:53:26Z` · commit `44dd28a` · [Actions run](https://github.com/djouallah/onelake-iceberg/actions/runs/35591079526)

## Latest run

| Engine | Version | Cold total | Warm total | Attach | Failed queries |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 38.1s | 23.5s | 7.6s | — |
| Polars | `2.0.0-rc.2` | 87.8s | 89.4s | 6.5s | — |
| chDB | `4.4.0` | 116.4s | 102.7s | 2.5s | — |
| LakeSail | `0.7.1` | 117.9s | 112.0s | 1.2s | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 550.7s | 518.7s | 19.2s | — |

Cold = first pass after attaching the catalog. Warm = the identical 22 statements run again immediately. Attach is timed separately and excluded from both totals.

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | Polars | chDB | LakeSail | Spark-OSS |
|---|---|---|---|---|---|
| Q1 | 8.89 | 8.81 | 10.23 | 7.04 | 51.92 |
| Q2 | 2.24 | 3.71 | 8.13 | 5.36 | 14.14 |
| Q3 | 3.58 | 3.71 | 7.27 | 5.17 | 23.15 |
| Q4 | 1.05 | 2.74 | 4.20 | 2.86 | 16.11 |
| Q5 | 2.08 | 3.76 | 5.95 | 6.04 | 32.86 |
| Q6 | 0.46 | 1.87 | 1.21 | 3.24 | 14.95 |
| Q7 | 0.88 | 9.93 | 15.71 | 6.08 | 46.75 |
| Q8 | 1.35 | 4.20 | 7.40 | 8.04 | 28.80 |
| Q9 | 2.07 | 5.78 | 7.70 | 7.64 | 37.24 |
| Q10 | 1.45 | 2.55 | 4.75 | 5.89 | 20.99 |
| Q11 | 0.15 | 0.60 | 3.11 | 3.09 | 4.10 |
| Q12 | 0.57 | 1.26 | 3.08 | 3.90 | 17.24 |
| Q13 | 1.83 | 2.91 | 3.19 | 2.73 | 12.35 |
| Q14 | 0.78 | 1.67 | 1.93 | 3.98 | 14.71 |
| Q15 | 0.59 | 1.60 | 2.87 | 6.59 | 26.90 |
| Q16 | 0.24 | 0.65 | 1.67 | 1.93 | 5.69 |
| Q17 | 2.44 | 5.15 | 4.25 | 7.31 | 46.95 |
| Q18 | 1.14 | 8.67 | 3.71 | 9.07 | 40.30 |
| Q19 | 0.85 | 2.84 | 2.96 | 4.24 | 17.63 |
| Q20 | 1.07 | 4.41 | 3.58 | 5.60 | 15.78 |
| Q21 | 3.89 | 8.80 | 10.91 | 9.89 | 55.23 |
| Q22 | 0.49 | 2.18 | 2.58 | 2.22 | 6.97 |

## History

675 timed statements across 3 runs.
Raw data: one immutable JSON per run under [`results/`](../results/), flattened to [`data/tpch_results.csv`](data/tpch_results.csv).

