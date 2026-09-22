"""The contract every engine implements.

REPLACES the execution half of cell 15, which was:

    if engine_lower == 'chdb_iceberg':
        print(conn.query(value, 'Pretty'))
    else:
        conn.sql(value).show()

That is not a fair measurement, for three separate reasons, and `execute` exists to fix all three:

1. `pl.sql(...)` returns a LAZYFRAME. Without `.collect()` the timer measures PLAN CONSTRUCTION --
   microseconds -- and not the query. Every Polars number the notebook ever produced for a lazy
   path is meaningless.
2. Spark Connect's `.show()` implies `limit(20)`. Q2, Q3, Q10, Q18 and Q21 end in `LIMIT 100`, so
   LakeSail was asked for 20 rows where the others computed 100. Under-execution, silently.
3. chDB's `'Pretty'` format renders an ASCII table INSIDE the timed window, and echoes the
   statement on error -- which, for the attach, is the bearer token (see bench/scrub.py).

So `execute` returns a ROW COUNT and every implementation must fully materialize to produce it.
All 22 results are <=100 rows, so collecting them costs nothing and buys a correctness signal a
timing chart can never give you: if two engines disagree about cardinality, one of them is wrong.
"""

from __future__ import annotations

from typing import Protocol


class Engine(Protocol):
    """One embedded query engine, attached to the OneLake Iceberg REST catalog."""

    name: str

    @property
    def version(self) -> str:
        """The resolved package version, recorded with every result.

        Not cosmetic: duckdb and polars track the pre-release line, so what resolved differs
        run to run. A benchmark chart without the version that produced it is an assertion
        rather than a result.
        """
        ...

    def setup(self) -> None:
        """Attach the catalog. Timed as its own row (`phase='setup'`, `query=0`).

        Raising from here is fatal to the job -- an engine that cannot attach has no numbers to
        report. Contrast a failing QUERY, which is recorded as `status='error'` and does not stop
        the other 21.
        """
        ...

    def execute(self, sql: str) -> int:
        """Run one statement to completion and return its row count."""
        ...

    def refresh(self) -> None:
        """OPTIONAL. Renew whatever the engine captured at setup, between the two passes.

        Not part of the measurement and not timed: the one engine that implements it (Spark)
        does so because its catalog bearer is a fixed string and a long suite outlives it.
        `bench/tpch/runner.py` calls it only if it exists, so no other engine needs a stub.
        """
        ...

    def close(self) -> None:
        """Release whatever the engine holds. Must be safe to call twice."""
        ...
