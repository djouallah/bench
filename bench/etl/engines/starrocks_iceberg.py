"""StarRocks: read the CSVs with FILES(), write the Iceberg table with one CTAS, in its container.

The container, the catalog and both credentials are bench/starrocks.py, set up exactly as for the
query benchmark. The statement is DuckDB's (engines/duckdb_iceberg.py) in StarRocks' dialect: the
53-column DUNIT layout, short rows NULL-padded, the three-column filter, every measure cast to
DOUBLE, SETTLEMENTDATE parsed, `year` derived, and the source file's name as `filename`.

TWO THINGS FILES() NEEDS, each found by a failed CI run (candidate_engine.yml, 2026-09-26):

* STRING, NEVER BARE VARCHAR. In StarRocks a VARCHAR with no length is VARCHAR(1), and a CSV value
  that does not fit loads as NULL: only one-character fields survived and the DUNIT filter
  matched nothing. The declared `schema` (4.1.2+) plus `fill_mismatch_column_with=null` then reads
  the ragged AEMO rows correctly -- the gate matched Python's csv module row for row.
* ONE FILES() PER FILE. FILES() has no source-file column yet (the `path_column` feature,
  StarRocks#66975, is an open PR since 2025-12), and every other engine writes `filename`. So
  each file is its own FILES() scan carrying its name as a literal, UNION ALL'd into the one
  CTAS -- still a single statement and a single Iceberg commit.

OneLake wants the table under `Tables/T{n}/starrocks`, so the CTAS passes that location, as
pyiceberg and Spark also have to. No partitioning, as for every engine (bench/etl/iceberg.py).
"""

from __future__ import annotations

from bench import auth, scrub, starrocks
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, FILTER, numeric_columns

CSV = '"format"="csv", "csv.column_separator"=",", "csv.enclose"=\'"\', "csv.skip_header"="1"'
SCHEMA = ", ".join(f"{c} STRING" for c in COLUMNS)


class StarrocksIceberg:
    name = "starrocks_iceberg"

    def __init__(self, cfg: EtlConfig):
        self.cfg = cfg
        self._conn = None
        self._version = "unknown"

    @property
    def version(self) -> str:
        return self._version

    @property
    def qualified(self) -> str:
        return f"{self.cfg.schema}.{TABLE[self.name]}"

    def setup(self) -> None:
        starrocks.start()
        self._conn = starrocks.connect()
        self._version = f"{starrocks.version(self._conn)} ({starrocks.IMAGE})"
        starrocks.attach(self._conn, self.cfg, auth.onelake_token())
        scrub.safe_print(f"  starrocks {self._version} attached")

    def _sql(self, statement: str) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute(statement)
            return list(cur.fetchall())

    def _one_file(self, name: str) -> str:
        uri = f"{self.cfg.csv_abfss}/{name}"
        where = " AND ".join(f"{c} = '{v}'" for c, v in FILTER)
        return (
            "SELECT UNIT, DUID, "
            f"'{uri}' AS filename, "
            + ", ".join(f"CAST({c} AS DOUBLE) AS {c}" for c in numeric_columns())
            + ", str_to_date(SETTLEMENTDATE, '%Y/%m/%d %H:%i:%s') AS SETTLEMENTDATE"
            f' FROM FILES("path"="{uri}", {CSV}, "schema"="{SCHEMA}", '
            f'"fill_mismatch_column_with"="null", {starrocks.storage_properties()})'
            f" WHERE {where}"
        )

    def load(self, files: list[str]) -> None:
        location = f"{self.cfg.base_path}/Tables/{self.cfg.schema}/{TABLE[self.name]}"
        # `IF NOT EXISTS` does not see OneLake's existing `T100` (StarRocks checks its own view
        # of the name, the catalog answers "The given namespace already exists": run
        # 36223815451), so the namespace's existence is the catalog's answer, not the clause's.
        try:
            self._sql(f"CREATE DATABASE IF NOT EXISTS {self.cfg.schema}")
        except Exception as exc:  # noqa: BLE001 - pymysql is only importable in this job
            if "already exists" not in str(exc):
                raise
        self._sql(f"DROP TABLE IF EXISTS {self.qualified} FORCE")
        union = "\nUNION ALL\n".join(self._one_file(name) for name in files)
        self._sql(
            f'CREATE TABLE {self.qualified} PROPERTIES ("location"="{location}") AS '
            f"SELECT *, year(SETTLEMENTDATE) AS year FROM (\n{union}\n) AS dunit"
        )

    def row_count(self) -> int:
        return int(self._sql(f"SELECT count(*) FROM {self.qualified}")[0][0])

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
