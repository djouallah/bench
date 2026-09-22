"""The palette, the two themes and the axis styling every chart in this repo uses.

Shared by bench/interactive/charts.py (the TPC-H charts) and bench/etl/charts.py (the ETL's).
The chart FUNCTIONS live next to the benchmark they draw; what is here is everything that has to
be the same across both so that an engine looks the same in every picture.

COLOR. One categorical slot per engine, in a FIXED order -- assigned to the ENGINE, never to its
rank, so a run that omits an engine never repaints the survivors. Daft holds slot 5 even though it
is not in bench.yml's engine list, which is the rule working as intended: adding it later must not
recolour Spark.

Slot 7, brown, is the DuckDB no-cache control, and it is last in ENGINES rather than next to
DuckDB: validated in ENGINES order in both modes, whereas nothing passes the normal-vision floor
between blue and orange -- teal and violet fail against blue, red, brown and green against
orange. The totals chart sorts by cold time, so the two DuckDB bars sit together there regardless,
and blue-brown-aqua passes.

The palette is validated, not eyeballed (dataviz `scripts/validate_palette.js`), and re-validated
whenever a slot is added -- every subset that can actually render has to pass, not just the full
set. Light passes every gate with a contrast WARN on aqua and yellow, which obligates the relief
rule -- hence value labels on the totals charts and the full tables in docs/. Dark passes
outright, though amber-green sits in the 6-8 CVD floor band, which the legend and the bar gaps
satisfy as secondary encoding.

Every chart is rendered TWICE, light and dark, so the README can serve the right one via
`<picture>` + `prefers-color-scheme`. A single PNG on a dark GitHub theme is a white slab.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display on a runner; must precede pyplot

import matplotlib.pyplot as plt  # noqa: E402

# Categorical slots, assigned to engines in a FIXED order.
LIGHT = {
    "duckdb_iceberg": "#2a78d6",  # slot 1 blue
    "chdb_iceberg": "#eb6834",  # slot 2 orange
    "polars_iceberg": "#1baf7a",  # slot 3 aqua
    "lakesail_iceberg": "#eda100",  # slot 4 yellow
    "daft_iceberg": "#8a5cd1",  # slot 5 purple
    "pyspark_iceberg": "#d6468f",  # slot 6 magenta
    "duckdb_nocache_iceberg": "#a8642a",  # slot 7 brown
}
DARK = {
    "duckdb_iceberg": "#3987e5",
    "chdb_iceberg": "#d95926",
    "polars_iceberg": "#199e70",
    "lakesail_iceberg": "#c98500",
    "daft_iceberg": "#9b6ee0",
    "pyspark_iceberg": "#e05a9c",
    "duckdb_nocache_iceberg": "#c07a3a",
}

THEMES = {
    "light": {
        "colors": LIGHT,
        "surface": "#fcfcfb",
        "primary": "#0b0b0b",
        "secondary": "#52514e",
        "grid": "#d8d7d2",
        "suffix": "",
    },
    "dark": {
        "colors": DARK,
        "surface": "#1a1a19",
        "primary": "#ffffff",
        "secondary": "#c3c2b7",
        "grid": "#3a3a37",
        "suffix": "-dark",
    },
}

# How many runs the headline charts average over.
#
# ALL-HISTORY AVERAGING IS A TRAP, and it bit this repo: every config change -- credential
# vending on and off, the catalog cache, an engine that was failing outright -- stayed blended
# into the published number forever, invisibly. A short window means a fix shows up within three
# runs instead of being diluted for months, while still smoothing the run-to-run noise of a
# shared runner, which is what a single run cannot do.
#
# The trend charts deliberately ignore this and plot everything: showing change over time is
# the one job they have.
RECENT_RUNS = 3

LABEL = {
    "duckdb_iceberg": "DuckDB",
    "chdb_iceberg": "chDB",
    "polars_iceberg": "Polars",
    "lakesail_iceberg": "LakeSail",
    "daft_iceberg": "Daft",
    "pyspark_iceberg": "Spark-OSS",
    "duckdb_nocache_iceberg": "DuckDB (no cache)",
}


def _runs_note(n: int) -> str:
    return "1 run" if n == 1 else f"mean of {n} runs"


def _style(ax, theme, title: str, ylabel: str) -> None:
    """Recessive axes and grid; ink in text tokens, never in a series color."""
    ax.set_title(title, color=theme["primary"], fontsize=13, pad=14, loc="left")
    ax.set_ylabel(ylabel, color=theme["secondary"], fontsize=10)
    ax.set_facecolor(theme["surface"])
    ax.figure.set_facecolor(theme["surface"])
    ax.tick_params(colors=theme["secondary"], labelsize=9)
    for side, spine in ax.spines.items():
        # Keep the baseline, drop the box.
        spine.set_visible(side == "bottom")
        spine.set_color(theme["grid"])
    ax.grid(axis="y", linestyle="--", linewidth=0.7, color=theme["grid"], alpha=0.8)
    ax.set_axisbelow(True)


def _legend(ax, theme, engines) -> None:
    """Always present for >=2 series: identity must never be color-alone."""
    handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=theme["colors"][e], edgecolor="none") for e in engines
    ]
    legend = ax.legend(
        handles,
        [LABEL[e] for e in engines],
        frameon=False,
        ncols=len(engines),
        loc="upper left",
        bbox_to_anchor=(0, 1.02),
        fontsize=9,
    )
    for text in legend.get_texts():
        text.set_color(theme["secondary"])


def _save(fig, out_dir: Path, stem: str, theme) -> Path:
    path = Path(out_dir) / f"{stem}{theme['suffix']}.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=theme["surface"])
    plt.close(fig)
    return path
