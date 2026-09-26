"""Apache Spark: read the CSVs over ABFS, create the table through the Iceberg Java API, append.

The seventh engine, and the one the notebook did not have. The session, the jars, the OIDC
assertion that ABFS refreshes on its own and the catalog config are all
`bench.tpch.engines.pyspark_iceberg.PysparkIceberg.setup()`, reused as-is; the read and
the transform
are `_spark_df.transform`, shared with LakeSail. What is specific to Spark is the CREATE.

WHY NOT `CREATE TABLE ... USING iceberg`, OR `writeTo(...).create()`. This repo's Spark write
smoke test (commit 6d1c2bf, since removed) established what OneLake's REST endpoint does with a
Spark-built create request: it returns `400 Malformed request`, with or without LOCATION,
because `SparkSchemaUtil` numbers a schema's field ids from 0 and the endpoint validates
`id > 0`. pyiceberg numbers from 1 and is accepted; so is the identical Spark request once
`TypeUtil.assignIncreasingFreshIds` has renumbered it. Everything after the create -- the
parquet writes over ABFS, the snapshot commit, `DROP TABLE ... PURGE` -- works.

So the table is built through the Iceberg Java API via py4j: convert the DataFrame's schema the
way Spark SQL would, renumber the ids from 1, set the location under `Tables/T{n}/` (the same
convention as every other table here), one `create()`. No partition spec: no engine here
partitions (bench/etl/iceberg.py says why). Then
`df.writeTo(...).append()` -- a name-resolved append, which is why the column order the endpoint
hands back does not matter. The single request this sends is byte-for-byte pyiceberg's, from
Spark's own REST client.

ONE FILE PER READ TASK, so the read decides the layout. This plan is map-only -- scan, project,
write, no exchange -- which is what Iceberg's default `write.distribution-mode=none` on an
unpartitioned table buys, and it means AQE never runs: `coalescePartitions` rewrites SHUFFLE
partitions and there is no shuffle to rewrite. Every read task writes its own file and commits
it. Iceberg's `write.target-file-size-bytes` (512 MB, left at its default) cannot save this: it
is a ROLLING cap that closes a file which grows past it, never a merge of files that stay under
it. So at the 128 MB default split, the 1000-file run read ~400 tasks' worth of CSV, each of
which filtered down to a few MB of parquet -- ~400 tiny files (run 35551660935).

`spark.sql.files.maxPartitionBytes` is the one knob that fixes that without a shuffle, and it is
the read side, not the write side. It is also what Fabric's autotune tunes.
"""

from __future__ import annotations

from bench import onelake, scrub
from bench.etl import iceberg
from bench.etl.config import TABLE, EtlConfig
from bench.etl.engines._spark_df import transform
from bench.tpch.engines.pyspark_iceberg import CATALOG
from bench.tpch.engines.pyspark_iceberg import PysparkIceberg as _TpchSpark

# Bytes of CSV one read task takes, and therefore roughly the input behind one output file.
#
# 1 GiB, not a number derived per run: Spark's split is
# `min(maxPartitionBytes, max(openCostInBytes, totalBytes / minPartitionNum))` and
# `minPartitionNum` defaults to the session's default parallelism, so a SMALL run still spreads
# across the runner's cores while a large one gets 8x fewer, 8x bigger files and 8x fewer OneLake
# creates. Nothing else about the write changes -- no shuffle, no sort, no repartition.
MAX_PARTITION_BYTES = 1024 * 1024 * 1024


class PysparkIceberg:
    name = "pyspark_iceberg"

    # The TPC-H engine whose setup() builds the session. Gluten swaps in its own.
    _session_engine = _TpchSpark

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._inner = self._session_engine(cfg)
        self._spark = None

    @property
    def version(self) -> str:
        return self._inner.version

    @property
    def table(self) -> str:
        return TABLE[self.name]

    @property
    def qualified(self) -> str:
        return f"{CATALOG}.`{self.cfg.schema}`.`{self.table}`"

    def setup(self) -> None:
        self._inner.setup()
        self._spark = self._inner.session
        self._spark.conf.set("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))

    # -- the create, through the Iceberg Java API -------------------------------------------

    @property
    def _jvm(self):
        return self._spark._jvm

    def _iceberg_schema(self, spark_schema):
        """`spark_schema` as Spark SQL's CREATE TABLE would convert it, with field ids from 1."""
        struct = self._spark._jsparkSession.parseDataType(spark_schema.json())
        converted = self._jvm.org.apache.iceberg.spark.SparkSchemaUtil.convert(struct)
        return self._jvm.org.apache.iceberg.types.TypeUtil.assignIncreasingFreshIds(converted)

    def _catalog(self):
        """The Iceberg Java catalog behind `CATALOG` -- the same REST client Spark SQL uses."""
        return self._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergCatalog(
            self._spark._jsparkSession, CATALOG
        )

    def _identifier(self):
        return self._jvm.org.apache.iceberg.catalog.TableIdentifier.parse(
            f"{self.cfg.schema}.{self.table}"
        )

    def _create(self, spark_schema) -> None:
        """One REST createTable, unpartitioned (`buildTable` defaults to no spec)."""
        schema = self._iceberg_schema(spark_schema)
        self._catalog().buildTable(self._identifier(), schema).withLocation(
            onelake.table_root(self.cfg, self.table)
        ).create()

    def _drop(self) -> None:
        """PURGE so the data files go too; a plain DROP if the endpoint refuses the purge."""
        try:
            self._spark.sql(f"DROP TABLE IF EXISTS {self.qualified} PURGE")
        except Exception as exc:  # noqa: BLE001 - best effort, then the plain drop
            scrub.safe_print(f"  PURGE refused ({scrub.scrub_exc(exc, 160)}); dropping metadata")
            self._spark.sql(f"DROP TABLE IF EXISTS {self.qualified}")

    # -- the engine contract ----------------------------------------------------------------

    def load(self, files: list[str]) -> None:
        uris = [f"{self.cfg.csv_abfss}/{name}" for name in files]
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.`{self.cfg.schema}`")
        self._drop()
        df = transform(self._spark, uris)
        self._create(df.schema)
        df.writeTo(self.qualified).append()
        scrub.safe_print(f"    {self.cfg.schema}.{self.table} written in one append")

    def row_count(self) -> int:
        return self._spark.sql(f"SELECT count(*) FROM {self.qualified}").collect()[0][0]

    def layout(self) -> str | None:
        """Files written and their average size, from the table's own snapshot summary.

        NOT `SELECT ... FROM <table>.files`, which is how one would normally ask Spark. Iceberg's
        SparkCatalog resolves a metadata table by first asking the catalog for a table named
        `files` in the namespace `T{n}.<table>`, and OneLake answers a TWO-LEVEL namespace with
        `400 Malformed request` rather than NoSuchTableException -- so the fallback that would
        then look for the metadata table never runs, and the BadRequestException propagates
        (run 35560496228). The Java catalog below is the same REST client that just created the
        table and committed to it, and the numbers are already in the snapshot summary.
        """
        snapshot = self._catalog().loadTable(self._identifier()).currentSnapshot()
        if snapshot is None:
            return None
        summary = snapshot.summary()
        return iceberg.describe(summary.get("total-data-files"), summary.get("total-files-size"))

    def close(self) -> None:
        self._inner.close()
        self._spark = None
