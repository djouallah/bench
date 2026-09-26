"""StarRocks: read the CSVs with FILES(), write the Iceberg table with one CTAS, in its container.

The container, the catalog and both credentials are bench/starrocks.py, set up exactly as for the
query benchmark. The statement is DuckDB's (engines/duckdb_iceberg.py) in StarRocks' dialect: the
53-column DUNIT layout, short rows NULL-padded, the three-column filter, every measure cast to
DOUBLE, SETTLEMENTDATE parsed, `year` derived -- and NO `filename` column (below).

WHAT FILES() NEEDS, each found by a failed CI run (candidate_engine.yml and etl.yml, 2026-09-26):

* STRING, NEVER BARE VARCHAR. In StarRocks a VARCHAR with no length is VARCHAR(1), and a CSV value
  that does not fit loads as NULL: only one-character fields survived and the DUNIT filter
  matched nothing. The declared `schema` (4.1.2+) plus `fill_mismatch_column_with=null` then reads
  the ragged AEMO rows correctly -- the gate matched Python's csv module row for row.
* ONE FILES() OVER ALL N FILES. The path is a brace glob of exactly the run's file names --
  `.../Files/csv/{a.CSV,b.CSV,...}`, the same list every other engine gets from
  bench/etl/data.py -- so it is one scan streaming into one Iceberg sink: one statement, one
  commit. The folder can hold more files than the run reads, so a bare `*` would not do.

NO `filename` COLUMN -- A STARROCKS LIMITATION, like Sail's. FILES() cannot expose the source path:
`columns_from_path` only extracts `key=value` folder segments, and `path_column` is an open PR
(StarRocks#66975). When it ships, `"path_column"="filename"` brings the column back. The first
version put each file in its OWN FILES() scan with its name as a literal and UNION ALL'd them,
and that does not scale: every branch is its own plan fragment holding fixed buffers until the
statement ends, even under the phased scheduler (at most two branches running). At 1000 files
the BE's memory grew with every file read -- about 1.5 GB each 30 s, to 11.9 GB -- until the
container's cgroup killed it (run 36231764513; the uncapped run before it, 36230293425, took the
whole runner down).

OneLake wants the table under `Tables/T{n}/starrocks`, so the CTAS passes that location, as
pyiceberg and Spark also have to. No partitioning, as for every engine (bench/etl/iceberg.py).
"""

from __future__ import annotations

from bench import auth, scrub, starrocks
from bench.etl.config import TABLE, EtlConfig
from bench.etl.schema import COLUMNS, FILTER, numeric_columns

CSV = '"format"="csv", "csv.column_separator"=",", "csv.enclose"=\'"\', "csv.skip_header"="1"'
SCHEMA = ", ".join(f"{c} STRING" for c in COLUMNS)
# Characters that mean something in a Hadoop glob; a file name carrying one would change the set.
GLOB_CHARS = set("{},*?[]^\\")


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
        # ONE WRITER BY DEFAULT: `pipeline_sink_dop` 0 means max(1, cores / 3) for <= 24 cores
        # (fe SessionVariable, branch-4.1), so the Iceberg sink ran on 1 of the runner's 4 cores.
        self._sql("SET pipeline_sink_dop = 4")
        scrub.safe_print(f"  starrocks {self._version} attached")
        starrocks.watch_resources()

    def _sql(self, statement: str) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute(statement)
            return list(cur.fetchall())

    def _select(self, files: list[str]) -> str:
        """The transform over ONE FILES() scan of exactly `files`: a brace glob of their names."""
        unsafe = [name for name in files if GLOB_CHARS & set(name)]
        if unsafe:
            raise ValueError(f"CSV names that would change the glob: {unsafe[:3]}")
        path = f"{self.cfg.csv_abfss}/{{{','.join(files)}}}"
        where = " AND ".join(f"{c} = '{v}'" for c, v in FILTER)
        return (
            "SELECT UNIT, DUID, "
            + ", ".join(f"CAST({c} AS DOUBLE) AS {c}" for c in numeric_columns())
            + ", str_to_date(SETTLEMENTDATE, '%Y/%m/%d %H:%i:%s') AS SETTLEMENTDATE"
            f' FROM FILES("path"="{path}", {CSV}, "schema"="{SCHEMA}", '
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
        self._sql(
            f'CREATE TABLE {self.qualified} PROPERTIES ("location"="{location}") AS '
            f"SELECT *, year(SETTLEMENTDATE) AS year FROM ({self._select(files)}) AS dunit"
        )
        scrub.safe_print(f"    {len(files)} files in one FILES() scan, one statement, one commit")

    def row_count(self) -> int:
        return int(self._sql(f"SELECT count(*) FROM {self.qualified}")[0][0])

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
