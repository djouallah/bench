# Learnings

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

## Spark-OSS: TPC-H Q21 fails at SF=100 (driver OOM on a broadcast)

- Run 36151085980, 2026-09-25: 21/22 queries OK; Q21 fails after ~3.5 min with
  `STAGE_MATERIALIZATION_MULTIPLE_FAILURES` — "Not enough memory to build and broadcast the
  table" (x2).
- Setup: `local[4]`, 11 GB driver heap, stock Spark 4.1.3 + Iceberg 1.11.0. Q21 passes at SF=60.
- Likely cause: AQE turns a join into a broadcast based on the shuffle size, which is measured
  after compression. At SF=100 the table it actually builds does not fit in the 11 GB heap, and
  in `local` mode the query's tasks share that same heap.
- Known workarounds (not applied): `spark.sql.autoBroadcastJoinThreshold=-1` or
  `spark.sql.adaptive.autoBroadcastJoinThreshold` lowered, or a bigger driver heap (the runner
  has 16 GB).
- Effect on the charts: an engine only gets a totals bar at a scale when it completes every
  statement, so Spark-OSS has no TPC-H SF=100 bar.
