"""The DuckDB pair: the no-cache engine is the DuckDB engine with exactly one line changed.

If that ever stops being true the no-cache bar stops meaning "the cache", so the test compares
the full setup SQL of the two engines rather than looking for the flag alone.
"""

from __future__ import annotations

import duckdb

from bench import auth
from bench.config import Config
from bench.tpch.config import ENGINES
from bench.tpch.engines.duckdb_iceberg import DuckDBIceberg
from bench.tpch.engines.duckdb_nocache_iceberg import DuckDBNoCacheIceberg


class _Recorder:
    """Stands in for a duckdb connection; keeps every statement text it is handed."""

    def __init__(self):
        self.statements: list[str] = []

    def sql(self, text: str):
        self.statements.append(text)

    def close(self):
        pass


def _setup_sql(cls, monkeypatch) -> str:
    recorder = _Recorder()
    monkeypatch.setattr(auth, "onelake_token", lambda: "tok")
    monkeypatch.setattr(duckdb, "connect", lambda: recorder)
    cls(Config(workspace_id="w", lakehouse_id="l", sf=10)).setup()
    assert len(recorder.statements) == 1
    return recorder.statements[0]


def test_the_two_duckdb_engines_differ_by_exactly_the_cache_flag(monkeypatch):
    cached = _setup_sql(DuckDBIceberg, monkeypatch)
    uncached = _setup_sql(DuckDBNoCacheIceberg, monkeypatch)

    on = "enable_external_file_cache = true"
    off = "enable_external_file_cache = false"
    assert on in cached and off not in cached
    assert off in uncached and on not in uncached
    # First statement of the session, so the reads the ATTACH itself performs obey it too.
    assert uncached.index(off) < uncached.index("ATTACH")
    assert cached.replace(on, "") == uncached.replace(off, "")


def test_both_duckdb_engines_are_registered():
    assert DuckDBIceberg.name == "duckdb_iceberg"
    assert DuckDBNoCacheIceberg.name == "duckdb_nocache_iceberg"
    assert DuckDBIceberg.name in ENGINES and DuckDBNoCacheIceberg.name in ENGINES
