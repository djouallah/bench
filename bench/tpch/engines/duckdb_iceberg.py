"""DuckDB: the iceberg extension attaches the catalog, the azure extension reads the files.

THE EXTERNAL FILE CACHE. Since 1.3 DuckDB keeps the byte ranges it reads from remote files in
its buffer pool (`enable_external_file_cache`, on by default), so a statement that touches a
parquet file the session has already read does not go back to OneLake for it. It is the only
cache DuckDB turns on by itself -- `enable_object_cache`, `enable_http_metadata_cache` and
`parquet_metadata_cache` all default to off. Left at its default.

THE STORAGE SECRET IS REPLACED WHEN THE TOKEN IS. It holds a token STRING, good for about an hour,
and TPC-DS at SF=100 runs DuckDB longer than that: run 35862492772 read fine for 65 minutes, then
failed Q88 onwards `Unauthorized` on store_sales. `refresh`, which the runner calls before every
statement and outside the timer, asks bench.auth for a token with at least
TOKEN_MIN_LIFETIME_SECONDS left and re-creates the secret only when the string changed: one
comparison per statement, one CREATE SECRET an hour. The catalog token in ATTACH is left alone:
table metadata is cached for CATALOG_CACHE_SECONDS (six hours), so the REST catalog is not called
again inside a run. (Removed once, in 994c96a, while Q64 killed the SF=100 run before the hour;
back for the run where Q64 does not.)
"""

from __future__ import annotations

from bench import auth, scrub
from bench.config import (
    CATALOG_CACHE_SECONDS,
    ICEBERG_ENDPOINT,
    TOKEN_MIN_LIFETIME_SECONDS,
    Config,
    azure_transport,
)


class DuckDBIceberg:
    name = "duckdb_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn = None
        self._token = ""

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
        """)
        self._storage_secret(token)
        self._conn.sql(f"""
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

    def _storage_secret(self, token: str) -> None:
        self._conn.sql(f"""
            CREATE OR REPLACE SECRET onelake_storage (
                TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}');
        """)
        self._token = token

    def refresh(self) -> None:
        """Outside the timer: a new secret once the token has under 15 minutes left."""
        token = auth.onelake_token(skew=TOKEN_MIN_LIFETIME_SECONDS)
        if self._conn is not None and token != self._token:
            self._storage_secret(token)
            scrub.safe_print("  storage token re-minted, secret replaced")

    def execute(self, sql: str) -> int:
        return len(self._conn.sql(sql).fetchall())

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
