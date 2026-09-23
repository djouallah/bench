# Results

TPC-H-like, scale factor 10, 22 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-22T00:46:24Z` · commit `c2871af` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/35673269660)

## Latest run

| Engine | Version | Cold total | Warm total | Attach | Failed queries |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 44.3s | 23.9s | 7.1s | — |
| Polars | `2.0.0-rc.2` | 100.4s | 97.3s | 7.5s | — |
| chDB | `4.4.0` | 147.8s | 106.2s | 2.5s | — |
| LakeSail | `0.7.1` | 222.6s | 261.2s | 2.4s | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 429.8s | 389.9s | 18.9s | — |

Cold = first pass after attaching the catalog. Warm = the identical 22 statements run again immediately. Attach is timed separately and excluded from both totals.

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | Polars | chDB | LakeSail | Spark-OSS |
|---|---|---|---|---|---|
| Q1 | 8.68 | 11.28 | 11.34 | 13.60 | 42.51 |
| Q2 | 4.08 | 5.80 | 11.65 | 11.90 | 10.18 |
| Q3 | 5.50 | 3.78 | 9.13 | 9.68 | 37.80 |
| Q4 | 2.01 | 2.57 | 4.45 | 6.09 | 11.32 |
| Q5 | 2.45 | 3.92 | 6.51 | 11.92 | 26.32 |
| Q6 | 0.50 | 1.99 | 1.53 | 5.56 | 7.42 |
| Q7 | 0.96 | 10.42 | 17.65 | 11.22 | 40.10 |
| Q8 | 1.62 | 7.11 | 8.05 | 14.12 | 23.74 |
| Q9 | 2.13 | 7.70 | 7.18 | 13.18 | 31.40 |
| Q10 | 1.86 | 5.38 | 4.70 | 10.55 | 13.34 |
| Q11 | 0.15 | 3.13 | 3.81 | 6.05 | 2.92 |
| Q12 | 0.78 | 1.61 | 2.58 | 7.45 | 9.74 |
| Q13 | 1.85 | 4.17 | 3.49 | 4.20 | 11.42 |
| Q14 | 0.83 | 2.10 | 1.99 | 6.80 | 7.43 |
| Q15 | 0.62 | 1.43 | 2.83 | 12.12 | 19.78 |
| Q16 | 0.26 | 0.81 | 2.00 | 3.66 | 5.05 |
| Q17 | 2.35 | 5.23 | 4.24 | 12.87 | 36.76 |
| Q18 | 1.48 | 7.71 | 25.79 | 16.38 | 31.93 |
| Q19 | 0.92 | 2.12 | 2.60 | 9.18 | 9.18 |
| Q20 | 1.12 | 3.43 | 4.04 | 11.23 | 9.04 |
| Q21 | 3.55 | 7.67 | 9.32 | 19.56 | 37.19 |
| Q22 | 0.55 | 1.07 | 2.89 | 5.27 | 5.27 |

## History

900 timed statements across 4 runs.
Raw data: one immutable JSON per run under [`results/`](../results/), flattened to [`data/tpch_results.csv`](data/tpch_results.csv).

