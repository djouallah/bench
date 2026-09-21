"""Which CSVs a run reads, and where they are.

REPLACES cells 27 and 28 (`generate_list_path`, `size`). The notebook globbed the Fabric mount;
here the listing is an ADLS call on the workspace filesystem, and the engines get file NAMES --
each turns them into the URI form it can read (`abfss://` for six engines, `https://` for chDB;
see EtlConfig.csv_abfss / csv_https).

ASCENDING, FIRST N. The landing selects the NEWEST N names to download, and this selects the
OLDEST N of what is there. Both are the notebook's rules, and they only agree when the folder
holds exactly N files -- which the landing guarantees on a fresh lakehouse and the notebook's own
lakehouse (3000 files landed, `total_files = 1000`) did not. Kept as-is: the point of matching
the notebook's naming is that the two can share a lakehouse, and the engines in it read the same
first thousand.
"""

from __future__ import annotations

from bench import onelake
from bench.etl.config import EtlConfig


def list_csvs(fs, cfg: EtlConfig) -> dict[str, int]:
    """`name -> bytes` for every `*.CSV` directly under `Files/csv`; empty if it is absent."""
    if not fs.get_directory_client(cfg.csv_relative).exists():
        return {}
    out: dict[str, int] = {}
    for path in fs.get_paths(path=cfg.csv_relative, recursive=False):
        if path.is_directory:
            continue
        name = path.name.rsplit("/", 1)[-1]
        if name.upper().endswith(".CSV"):
            out[name] = int(path.content_length or 0)
    return out


def csv_names(cfg: EtlConfig, n: int, fs=None) -> tuple[list[str], float]:
    """The first `n` CSV names in ascending order, and their total size in GB."""
    fs = fs if fs is not None else onelake.file_system(cfg)
    listed = list_csvs(fs, cfg)
    names = sorted(listed)[:n]
    if len(names) < n:
        raise RuntimeError(
            f"Files/csv holds {len(listed)} CSV file(s); {n} requested. Run the landing first."
        )
    return names, sum(listed[name] for name in names) / 2**30
