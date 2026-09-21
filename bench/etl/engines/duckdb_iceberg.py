"""DuckDB: read the CSVs over the azure extension, write Iceberg through the REST catalog itself.

Port of the notebook's `duckdb_clean_csv`, the one engine there that already wrote Iceberg. The
statement is the notebook's: `CREATE TABLE ... AS` over `read_csv(...)` with the 53-column
struct, the filter and the `COLUMNS(* EXCLUDE ...)` cast -- minus its `PARTITIONED BY (year)`,
because no engine here partitions (bench/etl/iceberg.py says why).

TWO ATTACH FLAGS the TPC-H engine does not carry, both from the notebook and both about writing:

* `STAGE_CREATE_TABLES false` -- OneLake does not implement staged creates, so the table is
  created in one request instead of created-then-committed.
* `SKIP_CREATE_TABLE_METADATA_UPDATES true` -- DuckDB follows a CREATE TABLE with a second commit
  that updates the fresh table's metadata, and OneLake rejects that commit. This flag fully
  initialises the metadata in the create itself. Spark's create has no such follow-up, which is
  why the Spark engine did not need an equivalent.

THE STORAGE PATH IS THE READ BENCHMARK'S: `CREATE SECRET ... access_token` plus
`ACCESS_DELEGATION_MODE 'none'`. The parquet that CTAS writes goes out through the azure
extension under that secret, the same way the TPC-H scans come in. Same curl transport too --
see config.azure_transport for why that is not optional on Linux.
"""

from __future__ import annotations

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, Config, azure_transport
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS

CATALOG = "onelake"


def attach(conn, cfg: Config, token: str) -> None:
    """The write-capable ATTACH on one connection: transport, storage secret, the two flags.

    A function rather than a line in `setup()` because the concurrency benchmark opens a fresh
    connection per writer -- every `duckdb.connect()` is its own database, with its own secrets
    and its own catalog attach -- and the attach has to be byte-for-byte this one.
    """
    conn.sql(f"""
        SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';
        SET preserve_insertion_order = false;

        CREATE OR REPLACE SECRET onelake_storage (
            TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');

        ATTACH OR REPLACE '{cfg.warehouse}' AS {CATALOG} (
            TYPE ICEBERG,
            ENDPOINT '{ICEBERG_ENDPOINT}',
            TOKEN '{token}',
            ACCESS_DELEGATION_MODE 'none',
            STAGE_CREATE_TABLES false,
            SKIP_CREATE_TABLE_METADATA_UPDATES true);
    """)


class DuckDBIceberg:
    name = "duckdb_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._conn = None

    @property
    def version(self) -> str:
        import duckdb

        return duckdb.__version__

    @property
    def qualified(self) -> str:
        return f"{CATALOG}.{self.cfg.schema}.{TABLE[self.name]}"

    def setup(self) -> None:
        import duckdb

        self._conn = duckdb.connect()
        attach(self._conn, self.cfg, auth.onelake_token())
        scrub.safe_print(f"  duckdb {self.version} attached")

    def load(self, files: list[str]) -> None:
        uris = [f"{self.cfg.csv_abfss}/{name}" for name in files]
        columns = ", ".join(f"'{c}': 'VARCHAR'" for c in COLUMNS)
        self._conn.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{self.cfg.schema}")
        self._conn.sql(f"DROP TABLE IF EXISTS {self.qualified}")
        self._conn.sql(f"""
            CREATE TABLE {self.qualified}
            AS
            WITH raw AS (
                SELECT * FROM read_csv(
                    {uris!r},
                    skip=1, header=0, all_varchar=1,
                    columns={{{columns}}},
                    filename=1, null_padding=true, ignore_errors=1, auto_detect=false
                )
                WHERE I = 'D' AND UNIT = 'DUNIT' AND VERSION = '3'
            )
            SELECT
                UNIT,
                DUID,
                filename,
                CAST(COLUMNS(* EXCLUDE (DUID, UNIT, SETTLEMENTDATE, I, XX, filename)) AS DOUBLE),
                CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
                year(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS year
            FROM raw
        """)

    def row_count(self) -> int:
        return self._conn.sql(f"SELECT count(*) FROM {self.qualified}").fetchone()[0]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
