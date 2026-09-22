"""The TPC-DS generator's pure pieces. dsdgen itself needs the extension download, so it is not
here; what is here is what a refactor could quietly break without a real run noticing."""

from __future__ import annotations

import pytest

from bench.tpcds.generate import field_ids_clause, plan_table


@pytest.mark.parametrize(
    ("exists", "has_files", "force", "want"),
    [
        (False, False, False, "write"),  # nothing there: create and write
        (True, False, False, "write"),  # a husk from a crashed run: write into it
        (True, True, False, "skip"),  # done: a resumed run must not register files twice
        (True, True, True, "purge"),  # regenerate: throw the table away first
        (True, False, True, "purge"),
        (False, False, True, "write"),
    ],
)
def test_resume_decision(exists, has_files, force, want):
    assert plan_table(exists, has_files, force) == want


def test_field_ids_clause_is_duckdbs_struct_literal():
    """The ids come from the Iceberg table pyiceberg created, 1-based and in column order;
    DuckDB's own 'auto' would count from 0 and add_files would reject every file."""
    assert field_ids_clause({"ss_sold_date_sk": 1, "ss_item_sk": 2}) == (
        "{ss_sold_date_sk: 1, ss_item_sk: 2}"
    )


def test_generator_imports_without_duckdb_or_pyiceberg_at_module_level():
    """smoke_sql.py imports this module in every engine job, most of which have neither."""
    import sys
    from pathlib import Path

    assert "bench.tpcds.generate" in sys.modules
    source = Path(sys.modules["bench.tpcds.generate"].__file__).read_text(encoding="utf-8")
    head = source.split("def _log")[0]
    assert "import duckdb" not in head and "import pyiceberg" not in head
