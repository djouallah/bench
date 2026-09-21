"""The SQL, and the per-engine rewriting that the notebook got wrong.

Everything here runs offline. These are the failures that would otherwise surface twenty minutes
into a real run, after the data has been generated and the catalog attached.
"""

from __future__ import annotations

import re

import pytest

from bench.interactive.config import ENGINES
from bench.interactive.queries import (
    IDENT_STYLE,
    N_QUERIES,
    SQL_PATH,
    load,
    render,
    rewrite_identifiers,
)

SCHEMA = "CH0010"
SF = 10


def test_twenty_two_statements():
    assert len(load("duckdb_iceberg", SCHEMA, SF)) == N_QUERIES


@pytest.mark.parametrize("engine", ENGINES)
def test_no_placeholder_survives(engine):
    """A leftover `{schema}` reaches the engine as a syntax error at query time, not here."""
    for index, statement in enumerate(load(engine, SCHEMA, SF), start=1):
        assert "{" not in statement, f"{engine} Q{index} still has a placeholder"
        assert "}" not in statement


@pytest.mark.parametrize("engine", ENGINES)
def test_every_table_reference_is_rewritten(engine):
    """No reference is left in the source spelling.

    The bug this catches: cell 15 stripped backticks for Polars and left `CH0010.lineitem`, which
    Polars parses as a relation named CH0010 and cannot resolve.
    """
    style = IDENT_STYLE[engine]
    joined = "\n".join(load(engine, SCHEMA, SF))
    if style == "backticked":
        # The qualified name must survive as ONE quoted identifier.
        assert f"`{SCHEMA}.lineitem`" in joined
        assert not re.search(rf"(?<!`){re.escape(SCHEMA)}\.\w+(?!`)", joined)
    elif style == "quoted":
        # Same single identifier, ANSI quoting. Daft rejects a backtick outright.
        assert f'"{SCHEMA}.lineitem"' in joined
        assert "`" not in joined
        assert not re.search(rf'(?<!"){re.escape(SCHEMA)}\.\w+(?!")', joined)
    else:
        assert f"{SCHEMA}.lineitem" in joined
        assert "`" not in joined


def test_polars_and_chdb_keep_the_qualified_name_quoted():
    """Both engines lack a second namespace level, for different reasons.

    chDB: the OneLake catalog reports `<namespace>.<table>` as one name.
    Polars: no catalog at all, so the engine registers frames under the full dotted name.
    """
    assert IDENT_STYLE["polars_iceberg"] == "backticked"
    assert IDENT_STYLE["chdb_iceberg"] == "backticked"
    # Daft has the same flat namespace and rejects backticks, so it gets the quoted spelling.
    assert IDENT_STYLE["daft_iceberg"] == "quoted"


def test_all_eight_tables_are_referenced():
    joined = "\n".join(load("duckdb_iceberg", SCHEMA, SF))
    for table in (
        "lineitem",
        "orders",
        "customer",
        "part",
        "partsupp",
        "supplier",
        "nation",
        "region",
    ):
        assert f"{SCHEMA}.{table}" in joined


def test_q11_threshold_scales_with_sf():
    """Q11's `(0.0001 / {SF})` is the only use of SF in the whole script.

    Dropping it would silently change how many rows Q11 returns at every scale factor but the one
    it was written for -- and no timing chart could ever show that.
    """
    for sf in (1, 10, 100):
        statements = load("duckdb_iceberg", f"CH{sf:04d}", sf)
        assert f"0.0001 / {sf}" in statements[10], f"Q11 lost its SF scaling at SF={sf}"


def test_sf_appears_exactly_once_in_the_source():
    assert SQL_PATH.read_text(encoding="utf-8").count("{SF}") == 1


def test_unknown_engine_is_rejected():
    with pytest.raises(ValueError, match="unknown engine"):
        load("clickhouse_local", SCHEMA, SF)


def test_render_is_idempotent_on_schema_name():
    """A schema name that is a prefix of another must not be double-rewritten."""
    raw = "SELECT * FROM `{schema}.lineitem` JOIN `{schema}.orders` ON 1=1"
    rendered = render(raw, "CH0010", 10)
    assert rewrite_identifiers(rendered, "duckdb_iceberg", "CH0010") == (
        "SELECT * FROM CH0010.lineitem JOIN CH0010.orders ON 1=1"
    )
