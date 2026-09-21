"""bench/report.py: the three pieces every publish script shares, now importable as a module."""

from __future__ import annotations

import pyarrow as pa
import pytest

from bench.report import leak_check, merge, write_csv
from bench.store import EngineResult, Row, write_engine_part


def test_merge_collects_every_part_and_the_host_facts(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    monkeypatch.setenv("BENCH_RUN_STARTED_AT", "2026-09-21T03:17:44Z")
    facts = {"runner": "linux-6.8", "cpu": 4, "mem_gb": 15.6, "python": "3.12.7"}
    result = EngineResult("1.5.5", rows=[Row("cold", "setup", 0, 1.0)], host=facts)
    write_engine_part(tmp_path, "duckdb_iceberg", result)
    write_engine_part(tmp_path, "polars_iceberg", EngineResult("0.12.0"))
    run = merge(tmp_path, 4)
    assert run.run_id == "42-2" and run.sf == 4 and run.run_started_at == "2026-09-21T03:17:44Z"
    assert set(run.engines) == {"duckdb_iceberg", "polars_iceberg"}
    assert (run.cpu, run.mem_gb, run.runner) == (4, 15.6, "linux-6.8")


def test_merge_refuses_an_empty_parts_dir(tmp_path):
    with pytest.raises(SystemExit):
        merge(tmp_path, 4)


def test_write_csv_round_trips_the_columns(tmp_path):
    table = pa.table({"engine": ["a", "b"], "dur": [1.5, None]})
    path = tmp_path / "out" / "x.csv"
    write_csv(table, path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "engine,dur" and lines[1] == "a,1.5" and lines[2] == "b,"


def test_leak_check_refuses_a_token_shaped_string(tmp_path):
    clean = tmp_path / "clean.md"
    clean.write_text("nothing to see", encoding="utf-8")
    leak_check([clean, tmp_path / "missing.json"])
    leaky = tmp_path / "leaky.json"
    leaky.write_text("eyJ" + "a" * 30 + "." + "b" * 30 + "." + "c" * 30, encoding="utf-8")
    with pytest.raises(SystemExit):
        leak_check([leaky])
