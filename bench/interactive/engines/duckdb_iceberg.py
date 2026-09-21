from __future__ import annotations

from bench import auth, scrub
from bench.config import CATALOG_CACHE_SECONDS, ICEBERG_ENDPOINT, Config, azure_transport


class DuckDBIceberg:
    name = "duckdb_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn = None

    @property
    def version(self) -> str:
        import duckdb

        return duckdb.__version__

    def setup(self) -> None:
        import duckdb

        # curl transport: DuckDB's default one fails the OneLake TLS handshake on Linux, and the
        # ATTACH still succeeds (plain HTTPS) so every read fails instead, with an error that
        # reads like a bad credential. ACCESS_DELEGATION_MODE 'none' turns vending off -- it costs
        # ~7s per table cold, and the other three engines authenticate storage with one token too.
        token = auth.onelake_token()
        self._conn = duckdb.connect()
        self._conn.sql(f"""
            SET GLOBAL azure_transport_option_type = '{azure_transport() or "default"}';

            CREATE OR REPLACE SECRET onelake_storage (
                TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');

            ATTACH OR REPLACE '{self.cfg.warehouse}' AS onelake (
                TYPE ICEBERG,
                ENDPOINT '{ICEBERG_ENDPOINT}',
                TOKEN '{token}',
                ACCESS_DELEGATION_MODE 'none',
                MAX_TABLE_STALENESS '{CATALOG_CACHE_SECONDS // 60} minutes',
                DEFAULT_SCHEMA '{self.cfg.schema}');

            USE onelake;
        """)
        scrub.safe_print(f"  duckdb {self.version} attached to {self.cfg.schema}")

    def execute(self, sql: str) -> int:
        return len(self._conn.sql(sql).fetchall())

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
