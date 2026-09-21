"""The AEMO DISPATCHLOAD record layout every engine reads, in one place.

The notebook spelled these 53 names out SIX TIMES -- once per engine, as a Polars dict, a DuckDB
struct literal, a Daft dict, a Spark DDL string and twice by position (`column_N`, `cN`). Six
copies of a 53-name list is six places for one to drift; here each engine derives its own
spelling from this tuple.

The files are AEMO "daily" reports: several record types in one ragged CSV, a `C` header line
first, then `I` (layout) and `D` (data) rows whose second field names the table. The benchmark
keeps one table -- `UNIT` = `DUNIT` at layout `VERSION` 3 -- and drops the two columns that carry
no data (`I`, the row kind, and `XX`, always "DISPATCH").
"""

from __future__ import annotations

COLUMNS = (
    "I",
    "UNIT",
    "XX",
    "VERSION",
    "SETTLEMENTDATE",
    "RUNNO",
    "DUID",
    "INTERVENTION",
    "DISPATCHMODE",
    "AGCSTATUS",
    "INITIALMW",
    "TOTALCLEARED",
    "RAMPDOWNRATE",
    "RAMPUPRATE",
    "LOWER5MIN",
    "LOWER60SEC",
    "LOWER6SEC",
    "RAISE5MIN",
    "RAISE60SEC",
    "RAISE6SEC",
    "MARGINAL5MINVALUE",
    "MARGINAL60SECVALUE",
    "MARGINAL6SECVALUE",
    "MARGINALVALUE",
    "VIOLATION5MINDEGREE",
    "VIOLATION60SECDEGREE",
    "VIOLATION6SECDEGREE",
    "VIOLATIONDEGREE",
    "LOWERREG",
    "RAISEREG",
    "AVAILABILITY",
    "RAISE6SECFLAGS",
    "RAISE60SECFLAGS",
    "RAISE5MINFLAGS",
    "RAISEREGFLAGS",
    "LOWER6SECFLAGS",
    "LOWER60SECFLAGS",
    "LOWER5MINFLAGS",
    "LOWERREGFLAGS",
    "RAISEREGAVAILABILITY",
    "RAISEREGENABLEMENTMAX",
    "RAISEREGENABLEMENTMIN",
    "LOWERREGAVAILABILITY",
    "LOWERREGENABLEMENTMAX",
    "LOWERREGENABLEMENTMIN",
    "RAISE6SECACTUALAVAILABILITY",
    "RAISE60SECACTUALAVAILABILITY",
    "RAISE5MINACTUALAVAILABILITY",
    "RAISEREGACTUALAVAILABILITY",
    "LOWER6SECACTUALAVAILABILITY",
    "LOWER60SECACTUALAVAILABILITY",
    "LOWER5MINACTUALAVAILABILITY",
    "LOWERREGACTUALAVAILABILITY",
)

# The row filter, as (column, value). Every engine applies exactly these three.
FILTER = (("I", "D"), ("UNIT", "DUNIT"), ("VERSION", "3"))

# Dropped after the filter: they carry nothing once the filter has run.
DROP = ("I", "XX")

# Left as text; everything else is cast to double.
TEXT = ("UNIT", "DUID", "SETTLEMENTDATE")

# `SETTLEMENTDATE` looks like `2018/04/01 00:05:00`, in strftime and in Spark/Java spelling.
TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S"
TIMESTAMP_FORMAT_SPARK = "yyyy/MM/dd HH:mm:ss"


def numeric_columns() -> list[str]:
    """The columns cast to double, in file order (the notebook built this from a set, so its
    column order differed run to run)."""
    return [c for c in COLUMNS if c not in TEXT and c not in DROP]
