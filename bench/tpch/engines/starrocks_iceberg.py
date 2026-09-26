"""StarRocks: the allin1 container, the OneLake REST catalog, queries over the MySQL protocol.

Everything about the container, the catalog and the two credentials is bench/starrocks.py. Here:
setup times the cold start (container up, back end registered, catalog attached) as the setup row,
and `refresh` re-attaches the catalog on a fresh bearer between statements when the one baked into
it runs low -- the catalog's `oauth2.token` is a fixed string, so that is the only way to renew
it. Storage renews itself (the assertion file).

The dialect: `catalog.database.table`, and `SET CATALOG` makes the suites' `CH0010.lineitem`
resolve (bench.tpch.queries.IDENT_STYLE, "dotted"). Q22 used `SUBSTRING(x FROM 1 FOR 2)`, which
StarRocks' parser rejects; sql/tpch.sql now spells it `SUBSTRING(x, 1, 2)` for every engine.
"""

from __future__ import annotations

from bench import auth, scrub, starrocks
from bench.config import Config


class StarrocksIceberg:
    name = "starrocks_iceberg"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._conn = None
        self._version = "unknown"
        self._expires = float("inf")

    @property
    def version(self) -> str:
        return self._version

    def _attach(self, fresh: bool) -> None:
        token = auth.onelake_token(fresh=fresh)
        self._expires = auth.token_expires_on()
        starrocks.attach(self._conn, self.cfg, token)
        with self._conn.cursor() as cur:
            cur.execute(f"USE {self.cfg.schema}")

    def setup(self) -> None:
        starrocks.start()
        self._conn = starrocks.connect()
        self._version = f"{starrocks.version(self._conn)} ({starrocks.IMAGE})"
        self._attach(fresh=False)
        scrub.safe_print(f"  starrocks {self._version} attached to {self.cfg.schema}")

    def refresh(self) -> None:
        """Outside the timer: a new bearer once less than TOKEN_MIN_LIFETIME_SECONDS remains."""
        if self._conn is None or not starrocks.needs_refresh(self._expires):
            return
        self._attach(fresh=True)
        scrub.safe_print("  bearer within 15 min of expiry: catalog re-attached on a fresh one")

    def execute(self, sql: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return len(cur.fetchall())

    def close(self) -> None:
        if self._conn is not None:
            try:
                cache = starrocks.datacache_metrics(self._conn)
                scrub.safe_print(f"  starrocks data cache: {cache}")
            except Exception as exc:  # noqa: BLE001 - a readout must never fail teardown
                scrub.safe_print(f"  warning: data cache readout failed: {exc}")
            try:
                self._conn.close()
            finally:
                self._conn = None
