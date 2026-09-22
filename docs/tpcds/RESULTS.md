# Results

TPC-DS-like, scale factor 10, 99 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-22T14:23:39Z` · commit `947f402` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/35732997698)

## Latest run

| Engine | Version | Cold total | Warm total | Attach | Failed queries |
|---|---|---:|---:|---:|---|
| DuckDB | `2.0.0.dev2609121639` | 92.1s | 68.5s | 8.7s | — |
| DuckDB (no cache) | `2.0.0.dev2609121639` | 370.3s | 328.9s | 4.2s | — |
| LakeSail | `0.7.1` | 2,289.3s | 1,306.2s | 1.8s | Q16, Q32, Q50, Q62, Q71, Q92, Q94, Q95, Q99 |

Cold = first pass after attaching the catalog. Warm = the identical 99 statements run again immediately. Attach is timed separately and excluded from both totals.

## Failures

| Engine | Pass | Query | Error |
|---|---|---|---|
| LakeSail | cold | Q16 | `IllegalArgumentException: invalid argument: found "order count" at 54:67 expected identifier, or '('` |
| LakeSail | cold | Q32 | `IllegalArgumentException: invalid argument: found "excess discount amount" at 47:71 expected identifier, or '('` |
| LakeSail | cold | Q50 | `IllegalArgumentException: invalid argument: found "30 days" at 337:346 expected identifier, or '('` |
| LakeSail | cold | Q62 | `IllegalArgumentException: invalid argument: found "30 days" at 188:197 expected identifier, or '('` |
| LakeSail | cold | Q71 | `SparkRuntimeException: Physical input schema should be the same as the one converted from logical input schema. Differences:  	- field metadata at index 2 [#84]: (physical) {"PARQUET:field_id": "16"} vs (logical) {}` |
| LakeSail | cold | Q92 | `IllegalArgumentException: invalid argument: found "Excess Discount Amount" at 47:71 expected identifier, or '('` |
| LakeSail | cold | Q94 | `IllegalArgumentException: invalid argument: found "order count" at 54:67 expected identifier, or '('` |
| LakeSail | cold | Q95 | `IllegalArgumentException: invalid argument: found "order count" at 331:344 expected identifier, or '('` |
| LakeSail | cold | Q99 | `IllegalArgumentException: invalid argument: found "30 days" at 211:220 expected identifier, or '('` |
| LakeSail | warm | Q53 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q54 | `AnalysisException: Failed to load table DS0010.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q55 | `AnalysisException: Failed to load table DS0010.date_dim: response error: status code 400 Bad Request` |
| LakeSail | warm | Q56 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q57 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q58 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q59 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q60 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q61 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q63 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q64 | `AnalysisException: Failed to load table DS0010.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q65 | `AnalysisException: Failed to load table DS0010.store: response error: status code 400 Bad Request` |
| LakeSail | warm | Q66 | `AnalysisException: Failed to load table DS0010.web_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q67 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q68 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q69 | `AnalysisException: Failed to load table DS0010.customer: response error: status code 400 Bad Request` |
| LakeSail | warm | Q70 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q71 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q72 | `AnalysisException: Failed to load table DS0010.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q73 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q74 | `AnalysisException: Failed to load table DS0010.customer: response error: status code 400 Bad Request` |
| LakeSail | warm | Q75 | `AnalysisException: Failed to load table DS0010.catalog_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q76 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q77 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q78 | `AnalysisException: Failed to load table DS0010.web_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q79 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q80 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q81 | `AnalysisException: Failed to load table DS0010.catalog_returns: response error: status code 400 Bad Request` |
| LakeSail | warm | Q82 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q83 | `AnalysisException: Failed to load table DS0010.store_returns: response error: status code 400 Bad Request` |
| LakeSail | warm | Q84 | `AnalysisException: Failed to load table DS0010.customer: response error: status code 400 Bad Request` |
| LakeSail | warm | Q85 | `AnalysisException: Failed to load table DS0010.web_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q86 | `AnalysisException: Failed to load table DS0010.web_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q87 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q88 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q89 | `AnalysisException: Failed to load table DS0010.item: response error: status code 400 Bad Request` |
| LakeSail | warm | Q90 | `AnalysisException: Failed to load table DS0010.web_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q91 | `AnalysisException: Failed to load table DS0010.call_center: response error: status code 400 Bad Request` |
| LakeSail | warm | Q93 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q96 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q97 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |
| LakeSail | warm | Q98 | `AnalysisException: Failed to load table DS0010.store_sales: response error: status code 400 Bad Request` |

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | DuckDB (no cache) | LakeSail |
|---|---|---|---|
| Q1 | 4.77 | 3.73 | 15.40 |
| Q2 | 2.83 | 1.76 | 17.12 |
| Q3 | 4.51 | 3.36 | 11.50 |
| Q4 | 2.57 | 4.91 | 37.62 |
| Q5 | 1.82 | 4.42 | 50.83 |
| Q6 | 0.69 | 2.08 | 21.50 |
| Q7 | 0.98 | 4.65 | 20.59 |
| Q8 | 0.27 | 1.96 | 25.85 |
| Q9 | 2.37 | 22.17 | 24.01 |
| Q10 | 0.36 | 2.98 | 32.23 |
| Q11 | 1.00 | 2.92 | 29.19 |
| Q12 | 0.18 | 0.71 | 28.61 |
| Q13 | 0.96 | 3.89 | 26.63 |
| Q14 | 3.09 | 8.14 | 60.88 |
| Q15 | 0.11 | 0.69 | 6.40 |
| Q16 | 1.86 | 1.73 | — |
| Q17 | 1.62 | 3.87 | 27.98 |
| Q18 | 0.68 | 1.65 | 21.17 |
| Q19 | 2.28 | 4.04 | 22.68 |
| Q20 | 0.07 | 0.45 | 8.90 |
| Q21 | 0.81 | 0.56 | 66.10 |
| Q22 | 1.81 | 2.37 | 16.89 |
| Q23 | 4.42 | 7.03 | 37.93 |
| Q24 | 0.62 | 3.74 | 29.16 |
| Q25 | 0.26 | 3.77 | 23.51 |
| Q26 | 0.26 | 1.30 | 16.60 |
| Q27 | 0.50 | 4.12 | 26.39 |
| Q28 | 1.88 | 7.59 | 13.89 |
| Q29 | 0.37 | 4.68 | 32.33 |
| Q30 | 0.28 | 0.72 | 6.19 |
| Q31 | 0.40 | 3.46 | 37.35 |
| Q32 | 0.05 | 0.56 | — |
| Q33 | 0.41 | 3.82 | 21.53 |
| Q34 | 0.39 | 3.37 | 19.20 |
| Q35 | 0.53 | 2.59 | 21.89 |
| Q36 | 0.52 | 3.17 | 20.73 |
| Q37 | 0.76 | 1.95 | 67.88 |
| Q38 | 0.59 | 2.32 | 19.30 |
| Q39 | 0.54 | 0.74 | 64.79 |
| Q40 | 0.38 | 0.68 | 17.05 |
| Q41 | 0.05 | 0.37 | 2.02 |
| Q42 | 0.23 | 3.65 | 11.46 |
| Q43 | 0.37 | 2.69 | 18.92 |
| Q44 | 0.90 | 9.25 | 34.05 |
| Q45 | 0.18 | 1.08 | 12.70 |
| Q46 | 0.57 | 3.74 | 22.27 |
| Q47 | 1.14 | 2.31 | 47.15 |
| Q48 | 0.67 | 4.01 | 25.75 |
| Q49 | 0.73 | 4.20 | 27.48 |
| Q50 | 0.84 | 2.61 | — |
| Q51 | 2.81 | 6.17 | 18.02 |
| Q52 | 0.23 | 3.87 | 13.47 |
| Q53 | 0.33 | 2.24 | 19.39 |
| Q54 | 0.33 | 2.56 | 24.75 |
| Q55 | 0.24 | 3.39 | 18.37 |
| Q56 | 0.38 | 3.36 | 22.39 |
| Q57 | 0.43 | 1.01 | 13.07 |
| Q58 | 0.37 | 4.59 | 49.36 |
| Q59 | 1.11 | 3.54 | 20.65 |
| Q60 | 0.42 | 3.07 | 21.40 |
| Q61 | 0.76 | 6.19 | 48.40 |
| Q62 | 0.43 | 2.02 | — |
| Q63 | 0.34 | 2.23 | 19.75 |
| Q64 | 2.01 | 6.32 | 49.36 |
| Q65 | 1.23 | 6.42 | 30.86 |
| Q66 | 0.92 | 2.36 | 18.57 |
| Q67 | 4.49 | 5.71 | 31.25 |
| Q68 | 0.59 | 3.98 | 29.25 |
| Q69 | 0.30 | 3.15 | 29.84 |
| Q70 | 0.59 | 4.23 | 42.28 |
| Q71 | 0.39 | 5.13 | — |
| Q72 | 3.15 | 5.14 | 42.30 |
| Q73 | 0.29 | 2.85 | 25.31 |
| Q74 | 0.69 | 3.94 | 25.51 |
| Q75 | 0.95 | 4.35 | 32.18 |
| Q76 | 0.58 | 4.86 | 17.83 |
| Q77 | 0.70 | 26.26 | 36.18 |
| Q78 | 1.99 | 4.89 | 33.76 |
| Q79 | 0.59 | 2.99 | 21.22 |
| Q80 | 0.68 | 4.78 | 32.32 |
| Q81 | 0.19 | 0.66 | 6.21 |
| Q82 | 0.21 | 1.72 | 45.69 |
| Q83 | 0.10 | 1.25 | 15.14 |
| Q84 | 0.12 | 0.68 | 7.88 |
| Q85 | 0.37 | 2.05 | 18.03 |
| Q86 | 0.13 | 0.64 | 5.53 |
| Q87 | 0.68 | 2.26 | 18.46 |
| Q88 | 1.73 | 18.66 | 36.68 |
| Q89 | 0.40 | 3.19 | 19.85 |
| Q90 | 0.11 | 1.57 | 10.70 |
| Q91 | 0.18 | 1.00 | 11.92 |
| Q92 | 0.11 | 2.03 | — |
| Q93 | 0.45 | 3.34 | 12.81 |
| Q94 | 0.14 | 1.77 | — |
| Q95 | 2.93 | 4.92 | — |
| Q96 | 0.23 | 2.55 | 18.06 |
| Q97 | 0.68 | 4.11 | 13.22 |
| Q98 | 0.36 | 3.05 | 12.41 |
| Q99 | 0.22 | 0.64 | — |

## History

597 timed statements across 1 runs.
Raw data: one immutable JSON per run under [`results/tpcds/`](../../results/tpcds/), flattened to [`data/tpcds_results.csv`](../data/tpcds_results.csv).

