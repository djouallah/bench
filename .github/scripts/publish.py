"""Merge the matrix artifacts into one immutable run file, then render the public surface.

THE SINGLE WRITER of results, and the only job that commits to the repo.

Inputs : parts/*.json          -- one per engine, from run_engine.py
Outputs: results/<run>.json    -- the immutable record, committed
         docs/charts/*.png     -- light and dark, committed
         docs/RESULTS.md       -- the table view, committed
         docs/data/tpch_results.csv -- flattened history, committed
         $GITHUB_STEP_SUMMARY  -- the per-run signal on the Actions page

It touches nothing in Azure: no pyiceberg, no credentials, no network. That is why
requirements/report.txt has no azure-identity in it and the job needs no `id-token` permission.

THE LEAK CHECK IS NOT OPTIONAL, and it lives in bench/report.py with `merge` and `write_csv` --
the three pieces every publish script shares, including the concurrency benchmark's, which is a
package module and cannot sibling-import this script.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from bench.interactive import charts
from bench.interactive.config import ENGINES, HEADLINE_SF
from bench.report import leak_check, merge, write_csv
from bench.store import Run, load_all, write_run

LABEL = charts.LABEL


def summarize(run: Run) -> list[dict]:
    """Per-engine totals for the markdown tables."""
    out = []
    for engine in ENGINES:
        result = run.engines.get(engine)
        if result is None:
            continue
        totals = {"cold": 0.0, "warm": 0.0}
        failed = []
        for row in result.rows:
            if row.phase != "query":
                continue
            if row.status == "ok" and row.dur is not None:
                totals[row.run_type] += row.dur
            elif row.status == "error" and row.run_type == "cold":
                failed.append(row.query)
        setup = next((r.dur for r in result.rows if r.phase == "setup"), None)
        out.append(
            {
                "engine": engine,
                "label": LABEL[engine],
                "version": result.version,
                "status": result.status,
                "setup": setup,
                "cold": totals["cold"],
                "warm": totals["warm"],
                "failed": failed,
            }
        )
    return sorted(out, key=lambda r: r["cold"] if r["cold"] else float("inf"))


def _table(rows: list[dict]) -> list[str]:
    lines = [
        "| Engine | Version | Cold total | Warm total | Attach | Failed queries |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in rows:
        failed = ", ".join(f"Q{q}" for q in row["failed"]) if row["failed"] else "—"
        cold = f"{row['cold']:,.1f}s" if row["cold"] else "—"
        warm = f"{row['warm']:,.1f}s" if row["warm"] else "—"
        setup = f"{row['setup']:,.1f}s" if row["setup"] else "—"
        lines.append(
            f"| {row['label']} | `{row['version']}` | {cold} | {warm} | {setup} | {failed} |"
        )
    return lines


def write_results_md(run: Run, rows: list[dict], table, path: Path) -> None:
    """The table view.

    Required, not decorative: the light-mode palette carries a contrast WARN on two of the four
    slots, and the dataviz relief rule says a chart that cannot clear 3:1 must be accompanied by
    visible labels or a table. This is that table. It is also where a reader checks a number a
    chart only shows as a bar.
    """
    lines = [
        "# Results",
        "",
        f"TPC-H-like, scale factor {run.sf}, 22 queries, on {run.cpu} vCPU / {run.mem_gb} GB "
        f"({run.runner}, Python {run.python}).",
        "",
        f"Last run: `{run.run_started_at}` · commit `{run.git_sha}`"
        + (f" · [Actions run]({run.run_url})" if run.run_url else ""),
        "",
        "## Latest run",
        "",
        *_table(rows),
        "",
        "Cold = first pass after attaching the catalog. Warm = the identical 22 statements run "
        "again immediately. Attach is timed separately and excluded from both totals.",
        "",
    ]

    failures = [
        (engine, row)
        for engine, result in run.engines.items()
        for row in result.rows
        if row.status == "error"
    ]
    if failures:
        lines += ["## Failures", "", "| Engine | Pass | Query | Error |", "|---|---|---|---|"]
        seen = set()
        for engine, row in failures:
            key = (engine, row.query, row.error)
            if key in seen:
                continue
            seen.add(key)
            message = (row.error or "").replace("|", "\\|").replace("\n", " ")[:300]
            lines.append(f"| {LABEL[engine]} | {row.run_type} | Q{row.query} | `{message}` |")
        lines.append("")

    lines += [
        "## Per query, latest run",
        "",
        "Seconds, cold pass. `—` means the query failed; see Failures above.",
        "",
    ]
    engines = [r["engine"] for r in rows]
    lines.append("| Query | " + " | ".join(LABEL[e] for e in engines) + " |")
    lines.append("|---" * (len(engines) + 1) + "|")
    by_query: dict[int, dict[str, str]] = {}
    for engine, result in run.engines.items():
        for row in result.rows:
            if row.phase == "query" and row.run_type == "cold":
                by_query.setdefault(row.query, {})[engine] = (
                    f"{row.dur:,.2f}" if row.status == "ok" and row.dur is not None else "—"
                )
    for query in sorted(by_query):
        cells = " | ".join(by_query[query].get(e, "—") for e in engines)
        lines.append(f"| Q{query} | {cells} |")

    lines += [
        "",
        "## History",
        "",
        f"{table.num_rows:,} timed statements across "
        f"{len(set(table.column('run_id').to_pylist()))} runs.",
        "Raw data: one immutable JSON per run under [`results/`](../results/), "
        "flattened to [`data/tpch_results.csv`](data/tpch_results.csv).",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_step_summary(run: Run, rows: list[dict]) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    lines = [
        f"## TPC-H SF {run.sf} — {run.cpu} vCPU, {run.mem_gb} GB",
        "",
        *_table(rows),
        "",
        f"`{run.run_started_at}` · commit `{run.git_sha}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    parts_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "parts")
    results_dir = Path("results")
    docs = Path("docs")
    sf = int(os.environ.get("TPCH_SF", "10"))

    run = merge(parts_dir, sf)
    rows = summarize(run)

    if not any_engine_produced_a_measurement(run):
        write_step_summary(run, rows)
        print(
            "::error::every statement failed on every engine -- nothing was measured, so no "
            "results file is being committed. Read the per-engine logs in the artifacts."
        )
        return 1

    run_path = write_run(results_dir, run)
    print(f"wrote {run_path}")

    table = load_all(results_dir)
    write_csv(table, docs / "data" / "tpch_results.csv")

    # The charts and RESULTS.md are the HEADLINE_SF view; a run at another scale is recorded
    # (the JSON above, the CSV, the step summary) and rewrites neither. etl_publish.py says why.
    if sf == HEADLINE_SF:
        subtitle = (
            f"TPC-H SF {run.sf} · {run.cpu} vCPU {run.mem_gb:.0f} GB · "
            f"{run.run_started_at[:10]} · OneLake Iceberg REST catalog"
        )
        for path in charts.render_all(table, sf, docs / "charts", subtitle):
            print(f"wrote {path}")
        write_results_md(run, rows, table, docs / "RESULTS.md")
    else:
        print(
            f"::notice::SF={sf} is not the headline scale ({HEADLINE_SF}): the run and the CSV "
            "are committed; docs/charts and docs/RESULTS.md are left as they are."
        )
    leak_check([run_path, docs / "RESULTS.md", docs / "data" / "tpch_results.csv"])
    write_step_summary(run, rows)

    best = rows[0]
    print(f"::notice::fastest cold: {best['label']} at {best['cold']:,.1f}s")
    return 0


def any_engine_produced_a_measurement(run: Run) -> bool:
    """True if at least one engine completed at least one query.

    A run where EVERY statement failed is not a result, and committing one to a public repo
    records a broken configuration as a data point. It happened: run 35486545501 landed a file
    whose only non-error row was the setup timing, because `publish` has `if: always()` so that a
    single dead engine cannot suppress the other three.

    The artifacts and the step summary are still produced either way -- that is where you go to
    read the errors. Only the COMMIT is suppressed.
    """
    return any(
        row.phase == "query" and row.status == "ok"
        for result in run.engines.values()
        for row in result.rows
    )


if __name__ == "__main__":
    sys.exit(main())
