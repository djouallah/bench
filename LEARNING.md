# Learnings

What running small data on small compute taught us: 4 vCPU / 16 GB / ~14 GB of free disk,
reading Iceberg on OneLake. The numbers come from `docs/data/*.csv` and the commits cited.

## The bottleneck is getting bytes off OneLake, not compute

- A query engine here spends most of its time waiting on remote reads. DuckDB with its file cache
  turned off, same wheel and same run, one line changed (c2871af):

  | | cache on | cache off |
  |---|---:|---:|
  | TPC-H SF=10, cold | 44.3 s | 80.2 s |
  | TPC-DS SF=10, cold | 92.1 s | 370.3 s |

- Stock Spark issues 2 × 4 MB reads in flight per stream (hadoop-azure defaults), so a tens-of-MB
  column chunk becomes a chain of round trips. The bench raises this to 4 × 8 MB.
- Engines share one pipe to the lakehouse. Run in parallel, every engine's time swung 20–50%
  between identical runs, so jobs run one at a time (`max-parallel: 1`, `concurrency: onelake`).

## Metadata round trips are a per-statement tax

On small data a query computes in under a second, so a fixed per-statement cost is visible.

- **Catalog.** Without a metadata cache, every statement re-resolves each table over REST. The
  defaults disagree: Spark's Iceberg catalog 30 s, Sail off. The bench sets one lifetime,
  `CATALOG_CACHE_SECONDS`, for every engine, as a fairness setting.
- **A cache setting is not always the cache you think.** Sail's `table_cache_*` caches the
  namespace *listing*, and `loadTable` still runs for every table in every statement. That leaves a
  1.5–3 s floor under Q11/Q16/Q22 ([lakehq/sail#2629](https://github.com/lakehq/sail/issues/2629)).
- **Manifests.** Iceberg's catalog cache keeps the table object, not its manifests. Spark needs the
  manifest cache as well (off by default), or Q11/Q16/Q22 have a ~5 s floor.
- **Credential vending** costs ~7 s per table, inside the first query that touches the table, so
  it is off.
- **Session start** is timed separately and kept out of totals. It is 2–9 s for DuckDB, chDB,
  Polars and Sail, 9–28 s for Spark, and 19–46 s for Spark + Gluten. A whole TPC-H SF=10 suite can
  take under a minute.

## Caches: the biggest lever, bounded by the box

- A data cache pays off **within a single cold pass**, because later queries re-read the same
  files. See the DuckDB table above. Gluten's Velox cache is off by default. Turning it on
  (8 GB SSD + 1 GB memory) took TPC-H SF=10 cold from 249.6 s to 123–160 s (fd1d3ef).
- It only helps while the working set fits. DuckDB, warm pass against cold:

  | | cold | warm | gain |
  |---|---:|---:|---:|
  | TPC-H SF=10 | 44.3 s | 23.9 s | 46% |
  | TPC-DS SF=10 | 92.1 s | 68.5 s | 26% |
  | TPC-DS SF=60 (~18 GiB > RAM) | 1,367.0 s | 1,280.7 s | 6% |

  So both suites now run **cold only**, and the averages are over 3 runs. Past RAM, a warm pass
  measures little and doubles the run.
- Size the cache to the disk, not the data. ClickHouse does not check free space, so an
  oversized chDB cache gives ENOSPC in the middle of a query rather than evicting. It is clamped
  at 1.5× the dataset, 2–8 GiB (`chdb_cache_gib`).

## A cache has to outlive the credential

- The Entra token lives about an hour, and a OneLake SAS at most an hour. Several engines take
  the token as a string at setup and never refresh it.
- Spark's catalog bearer is a fixed header that cannot be refreshed. When the table cache expired
  mid-run, the next lookup presented a dead bearer, and the statements after it failed. Raising
  the cache from 15 min to 2 h fixed TPC-DS SF=10 (0b65692). Raising it to 6 h, above the job
  cap, fixed SF=30/60 (680aa25).
- Where a credential must be replaced, the engine restarts between statements when less than 15
  minutes remain. The restart empties the catalog, manifest and Velox caches, and that cost is
  left in the numbers.

## Gluten/Velox: the ETL's CSV read runs on plain Spark, not Velox

- Run 36214140962, 2026-09-26 (10 files): the ETL load succeeds, but the log says
  `Validation failed for plan: Scan csv ... Unsupported file format TextReadFormat`. Plain Spark
  reads and parses the CSVs; Velox only runs the filter, casts and Parquet write after that.
- Why: open-source Gluten on Spark 4.x has no CSV reader. Velox itself cannot read CSV
  (apache/gluten#5414, open). Gluten used to have an Arrow-based CSV reader behind
  `spark.gluten.sql.native.arrow.reader.enabled`, but it was switched off for Spark 4
  (apache/gluten#11190) and then deleted (#12130, #12737). No setting brings it back.
- Fabric's Native Execution Engine does read CSV natively on Runtime 2.0 (Spark 4.1): its docs
  say "The vectorized CSV parser now supports CSV"
  (learn.microsoft.com/fabric/data-engineering/native-execution-engine-overview). That parser
  is Microsoft's own addition and is not in open-source Gluten.
- Effect on the ETL numbers: Gluten/Velox here shows open-source Gluten, not Fabric's engine.
  Expect its load time to be close to Spark-OSS, because the CSV parse is most of the work.
