# Results

TPC-DS-like, scale factor 60, 99 queries, on 4 vCPU / 15.6 GB (linux-6.17.0-1022-azure, Python 3.12.14).

Last run: `2026-09-24T07:07:35Z` · commit `54a0f36` · [Actions run](https://github.com/djouallah/lakehouse_benchmark/actions/runs/35956245381)

## Latest run

Each engine's most recent run at this scale; the newest run may not include every engine.

| Engine | Version | Cold total | Attach | Failed queries |
|---|---|---:|---:|---|
| DuckDB | `2.0.0.dev2609222040` | 1,367.0s | 6.7s | — |
| Gluten/Velox | `4.1.1 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 2,117.1s | 39.3s | — |
| Spark-OSS | `4.1.3 + iceberg Apache Iceberg 1.11.0 (commit 6976e020b894f6a6777704df2b8c4458cb291ae9)` | 8,096.1s | 12.4s | Q62, Q64, Q66, Q71, Q77, Q80, Q84, Q85, Q88, Q90, Q93, Q94, Q95, Q96, Q99 |

One cold pass, the first after attaching the catalog. Attach is timed separately and excluded from the total.

## Failures

| Engine | Pass | Query | Error |
|---|---|---|---|
| Spark-OSS | cold | Q62 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T08:48:27.3987778Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q64 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T08:49:20.5030892Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q66 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T08:51:25.2168109Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q71 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T08:57:43.7087604Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q77 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:13:35.4961224Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q80 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:18:19.9653233Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q84 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:19:43.9343246Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q85 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:19:44.0969082Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q88 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:21:04.2435736Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q90 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:22:14.6843231Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q93 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:22:42.3484151Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q94 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:22:42.5009038Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q95 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:22:42.6549524Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q96 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:22:42.8037215Z","httpStatusCode":401,"hresult":-2147467259,"detail` |
| Spark-OSS | cold | Q99 | `Py4JJavaError: An error occurred while calling o95.sql. : org.apache.iceberg.exceptions.NotAuthorizedException: Not authorized: {"code":"Unauthorized","subCode":2,"message":"Access token validation failed.","timeStamp":"2026-09-24T09:25:01.2208903Z","httpStatusCode":401,"hresult":-2147467259,"detail` |

## Per query, latest run

Seconds, cold pass. `—` means the query failed; see Failures above.

| Query | DuckDB | Gluten/Velox | Spark-OSS |
|---|---|---|---|
| Q1 | 11.26 | 21.01 | 26.17 |
| Q2 | 15.93 | 31.22 | 30.31 |
| Q3 | 30.28 | 36.28 | 40.41 |
| Q4 | 20.00 | 93.60 | 236.87 |
| Q5 | 18.00 | 73.70 | 127.22 |
| Q6 | 5.87 | 7.70 | 56.58 |
| Q7 | 9.62 | 36.04 | 85.99 |
| Q8 | 6.06 | 3.66 | 34.24 |
| Q9 | 77.04 | 40.72 | 471.48 |
| Q10 | 7.69 | 10.28 | 51.66 |
| Q11 | 16.00 | 30.63 | 137.06 |
| Q12 | 2.01 | 2.70 | 17.74 |
| Q13 | 17.32 | 31.93 | 187.70 |
| Q14 | 39.09 | 60.94 | 288.01 |
| Q15 | 1.22 | 12.63 | 55.42 |
| Q16 | 10.12 | 42.01 | 86.50 |
| Q17 | 6.40 | 27.43 | 178.41 |
| Q18 | 3.74 | 15.61 | 65.63 |
| Q19 | 125.78 | 15.16 | 61.54 |
| Q20 | 3.51 | 3.29 | 34.30 |
| Q21 | 2.52 | 11.97 | 28.49 |
| Q22 | 3.65 | 15.36 | 50.15 |
| Q23 | 52.78 | 53.12 | 381.74 |
| Q24 | 13.36 | 15.61 | 111.76 |
| Q25 | 4.14 | 23.08 | 180.61 |
| Q26 | 4.19 | 13.25 | 54.63 |
| Q27 | 3.03 | 20.98 | 83.61 |
| Q28 | 7.48 | 36.19 | 341.74 |
| Q29 | 8.21 | 9.55 | 181.56 |
| Q30 | 1.43 | 3.40 | 9.09 |
| Q31 | 5.76 | 26.28 | 178.77 |
| Q32 | 0.67 | 10.62 | 48.83 |
| Q33 | 6.64 | 11.40 | 100.20 |
| Q34 | 2.43 | 12.98 | 53.34 |
| Q35 | 8.43 | 8.40 | 58.90 |
| Q36 | 8.77 | 10.93 | 57.48 |
| Q37 | 4.84 | 8.55 | 43.71 |
| Q38 | 9.28 | 9.44 | 73.79 |
| Q39 | 1.57 | 20.68 | 48.44 |
| Q40 | 3.11 | 9.50 | 50.48 |
| Q41 | 0.11 | 0.51 | 1.25 |
| Q42 | 7.27 | 2.60 | 46.72 |
| Q43 | 6.07 | 6.72 | 44.90 |
| Q44 | 44.36 | 6.28 | 112.49 |
| Q45 | 2.38 | 6.08 | 31.37 |
| Q46 | 19.42 | 11.93 | 70.14 |
| Q47 | 11.84 | 11.88 | 70.77 |
| Q48 | 11.19 | 13.23 | 49.79 |
| Q49 | 18.73 | 65.21 | 155.18 |
| Q50 | 9.76 | 20.83 | 85.43 |
| Q51 | 20.57 | 23.65 | 99.27 |
| Q52 | 10.06 | 14.36 | 46.55 |
| Q53 | 4.97 | 5.97 | 56.35 |
| Q54 | 14.18 | 21.32 | 99.59 |
| Q55 | 5.76 | 3.51 | 46.70 |
| Q56 | 10.33 | 12.13 | 102.47 |
| Q57 | 2.27 | 5.35 | 36.31 |
| Q58 | 12.73 | 6.12 | 81.35 |
| Q59 | 11.40 | 9.04 | 54.36 |
| Q60 | 17.11 | 10.59 | 95.75 |
| Q61 | 17.56 | 22.67 | 107.35 |
| Q62 | 3.80 | 12.67 | — |
| Q63 | 9.39 | 4.24 | 52.93 |
| Q64 | 31.93 | 69.68 | — |
| Q65 | 29.02 | 18.97 | 124.49 |
| Q66 | 14.18 | 18.79 | — |
| Q67 | 46.41 | 49.34 | 185.75 |
| Q68 | 47.34 | 26.68 | 74.09 |
| Q69 | 9.49 | 9.15 | 51.81 |
| Q70 | 2.09 | 23.18 | 66.67 |
| Q71 | 9.47 | 28.36 | — |
| Q72 | 23.69 | 71.33 | 484.70 |
| Q73 | 7.84 | 14.13 | 49.55 |
| Q74 | 8.03 | 42.80 | 113.61 |
| Q75 | 29.39 | 65.42 | 213.94 |
| Q76 | 16.77 | 29.86 | 89.78 |
| Q77 | 20.40 | 31.66 | — |
| Q78 | 29.13 | 60.05 | 224.30 |
| Q79 | 21.57 | 37.62 | 59.99 |
| Q80 | 15.16 | 53.34 | — |
| Q81 | 0.97 | 4.72 | 13.44 |
| Q82 | 4.02 | 10.95 | 59.10 |
| Q83 | 0.94 | 3.45 | 11.27 |
| Q84 | 0.62 | 3.33 | — |
| Q85 | 2.55 | 5.11 | — |
| Q86 | 2.05 | 2.85 | 12.50 |
| Q87 | 7.77 | 12.61 | 67.49 |
| Q88 | 16.77 | 35.12 | — |
| Q89 | 3.03 | 7.18 | 70.28 |
| Q90 | 3.64 | 8.65 | — |
| Q91 | 0.79 | 2.00 | 5.40 |
| Q92 | 1.14 | 7.54 | 22.11 |
| Q93 | 10.19 | 22.25 | — |
| Q94 | 3.77 | 18.16 | — |
| Q95 | 58.79 | 62.35 | — |
| Q96 | 20.71 | 10.11 | — |
| Q97 | 6.42 | 13.23 | 76.58 |
| Q98 | 8.57 | 6.08 | 61.66 |
| Q99 | 1.88 | 6.36 | — |

## History

1,400 timed statements across 11 runs.
Raw data: one immutable JSON per run under [`results/tpcds/`](../../results/tpcds/), flattened to [`data/tpcds_results.csv`](../data/tpcds_results.csv).

