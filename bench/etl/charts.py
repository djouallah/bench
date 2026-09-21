"""The ETL charts: seconds per engine, and the same over time.

REPLACES cells 31-36 of the ETL notebook, which read the results Delta table, excluded a few
engines by name, filtered on a `cutoff` date and plotted duration against core count. Same
simplifications as bench/charts.py: the data is local JSON, runs are filtered on `test='etl'`
and the file count and nothing else, core count is a constant on `ubuntu-latest`, and every
chart renders light and dark.

Styling, palette, labels and the recent-runs window are bench/charts.py's; only the queries
differ (one `load` row per engine instead of 22 queries).
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from bench.charts import LABEL, RECENT_RUNS, THEMES, _legend, _runs_note, _save, _style
from bench.etl.config import ETL_ENGINES


def _window(con, sf: int) -> int:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW recent AS
        SELECT * FROM raw
        WHERE sf = {sf} AND test = 'etl' AND run_id IN (
            SELECT run_id FROM raw
            WHERE sf = {sf} AND test = 'etl'
            GROUP BY run_id
            ORDER BY max(run_started_at) DESC
            LIMIT {RECENT_RUNS}
        )
        """
    )
    return con.execute("SELECT count(DISTINCT run_id) FROM recent").fetchone()[0]


def totals(con, sf: int, out_dir: Path, subtitle: str) -> list[Path]:
    """Horizontal bars: mean load seconds per engine over the recent window, fastest at the top.

    An engine whose load failed in every recent run is drawn as an `x` at zero rather than left
    out: a missing bar and a near-zero bar look the same, and "failed" is the opposite of "fast".
    """
    rows = con.execute(
        "SELECT engine, AVG(dur) FROM recent WHERE phase='load' AND status='ok' GROUP BY engine"
    ).fetchall()
    ok = {engine: dur for engine, dur in rows}
    failed = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT engine FROM recent WHERE phase='load' AND status='error'"
        ).fetchall()
    } - set(ok)
    if not ok and not failed:
        return []
    order = sorted(
        (e for e in ETL_ENGINES if e in ok or e in failed),
        key=lambda e: ok.get(e, float("inf")),
        reverse=True,  # fastest ends up at the top of a horizontal axis
    )
    longest = max(ok.values(), default=1.0)

    paths = []
    for theme in THEMES.values():
        fig, ax = plt.subplots(figsize=(11, 0.7 * len(order) + 2.2))
        for index, engine in enumerate(order):
            if engine in ok:
                ax.barh(
                    index,
                    ok[engine],
                    height=0.62,
                    color=theme["colors"][engine],
                    edgecolor=theme["surface"],
                    linewidth=1.0,
                    zorder=3,
                )
                ax.text(
                    ok[engine],
                    index,
                    f"  {ok[engine]:,.0f}s",
                    va="center",
                    ha="left",
                    fontsize=9,
                    color=theme["secondary"],
                    zorder=4,
                )
            else:
                ax.plot(
                    0,
                    index,
                    marker="x",
                    markersize=8,
                    markeredgewidth=1.8,
                    color=theme["colors"][engine],
                    zorder=5,
                    clip_on=False,
                )
                ax.text(
                    longest * 0.01,
                    index,
                    "  failed (see docs/etl/RESULTS.md)",
                    va="center",
                    ha="left",
                    fontsize=9,
                    color=theme["secondary"],
                    zorder=4,
                )
        ax.set_yticks(range(len(order)))
        ax.set_yticklabels([LABEL[e] for e in order], color=theme["secondary"])
        ax.set_xlim(left=0, right=longest * 1.25)
        _style(ax, theme, f"CSV to Iceberg, seconds per engine — {subtitle}", "")
        ax.set_xlabel("seconds (lower is better)", color=theme["secondary"], fontsize=10)
        ax.grid(axis="y", visible=False)
        ax.grid(axis="x", linestyle="--", linewidth=0.7, color=theme["grid"], alpha=0.8)
        paths.append(_save(fig, out_dir, "totals", theme))
    return paths


def trend(con, sf: int, out_dir: Path, subtitle: str) -> list[Path]:
    """Load seconds per engine over time. Guarded to two or more distinct days, like TPC-H's."""
    rows = con.execute(
        "SELECT substr(run_started_at, 1, 10) AS day, engine, AVG(dur) "
        "FROM raw WHERE sf=? AND test='etl' AND phase='load' AND status='ok' "
        "GROUP BY day, engine ORDER BY day",
        [sf],
    ).fetchall()
    days = sorted({row[0] for row in rows})
    if len(days) < 2:
        return []
    series: dict[str, dict[str, float]] = {}
    for day, engine, dur in rows:
        series.setdefault(engine, {})[day] = dur

    paths = []
    for theme in THEMES.values():
        fig, ax = plt.subplots(figsize=(14, 6))
        for engine, values in series.items():
            if engine not in theme["colors"]:
                continue
            ax.plot(
                days,
                [values.get(d) for d in days],
                color=theme["colors"][engine],
                linewidth=2.0,
                marker="o",
                markersize=6,
                markeredgecolor=theme["surface"],
                markeredgewidth=1.5,
                zorder=3,
            )
        ax.set_ylim(bottom=0)
        _style(ax, theme, f"CSV to Iceberg over time — {subtitle}", "seconds (lower is better)")
        _legend(ax, theme, [e for e in ETL_ENGINES if e in series])
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        paths.append(_save(fig, out_dir, "trend", theme))
    return paths


def render_all(table, sf: int, out_dir: str | Path, subtitle: str) -> list[Path]:
    """Every chart that has data, light and dark."""
    import duckdb

    con = duckdb.connect()
    con.register("raw", table)
    out_dir = Path(out_dir)
    n = _window(con, sf)
    paths: list[Path] = []
    paths += totals(con, sf, out_dir, f"{subtitle} · {_runs_note(n)}")
    paths += trend(con, sf, out_dir, subtitle)
    con.close()
    return paths
