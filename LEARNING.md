# Learnings

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
