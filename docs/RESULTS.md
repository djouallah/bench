# Results

TPC-H-like, scale factor 10, 22 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-20T14:51:49Z` · commit `c9e8c79` · [Actions run](https://github.com/djouallah/onelake-iceberg/actions/runs/35517764345)

## Latest run

| Engine | Version | Cold total | Warm total | Attach | Failed queries |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 31.8s | 16.8s | 7.7s | — |
| Polars | `2.0.0-rc.2` | 82.2s | 67.3s | 8.9s | — |
| chDB | `4.4.0` | 118.5s | 105.0s | 2.1s | — |
| LakeSail | `0.7.1` | 125.4s | 141.9s | 1.9s | — |
| Spark-OSS | `4.1.3` | 560.9s | 529.1s | 11.3s | — |

Cold = first pass after attaching the catalog. Warm = the identical 22 statements run again immediately. Attach is timed separately and excluded from both totals.

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | Polars | chDB | LakeSail | Spark-OSS |
|---|---|---|---|---|---|
| Q1 | 6.84 | 11.19 | 8.96 | 6.23 | 52.38 |
| Q2 | 2.99 | 6.35 | 7.36 | 4.66 | 13.89 |
| Q3 | 3.58 | 3.64 | 7.11 | 5.38 | 25.57 |
| Q4 | 1.42 | 2.72 | 3.43 | 3.08 | 16.79 |
| Q5 | 1.84 | 3.47 | 5.37 | 6.19 | 34.19 |
| Q6 | 0.35 | 1.89 | 1.53 | 3.64 | 12.44 |
| Q7 | 0.67 | 8.52 | 27.17 | 6.34 | 51.26 |
| Q8 | 1.10 | 3.92 | 8.65 | 8.15 | 30.12 |
| Q9 | 1.58 | 4.57 | 6.69 | 8.72 | 38.26 |
| Q10 | 1.12 | 2.28 | 4.46 | 5.89 | 19.83 |
| Q11 | 0.11 | 0.88 | 2.52 | 2.79 | 3.81 |
| Q12 | 0.56 | 1.60 | 2.42 | 3.89 | 17.58 |
| Q13 | 1.43 | 2.83 | 3.96 | 2.84 | 13.79 |
| Q14 | 0.57 | 1.69 | 1.69 | 4.04 | 13.38 |
| Q15 | 0.41 | 1.36 | 2.31 | 7.13 | 25.68 |
| Q16 | 0.18 | 0.95 | 1.43 | 1.99 | 5.87 |
| Q17 | 1.65 | 4.66 | 4.08 | 11.35 | 49.18 |
| Q18 | 1.04 | 6.58 | 3.21 | 9.78 | 43.31 |
| Q19 | 0.59 | 2.01 | 2.45 | 5.06 | 16.03 |
| Q20 | 0.73 | 3.52 | 2.72 | 5.40 | 16.73 |
| Q21 | 2.68 | 6.34 | 8.88 | 10.63 | 53.14 |
| Q22 | 0.39 | 1.26 | 2.13 | 2.23 | 7.65 |

## History

450 timed statements across 2 runs.
Raw data: one immutable JSON per run under [`results/`](../results/), flattened to [`data/tpch_results.csv`](data/tpch_results.csv).

