"""Land the AEMO daily archive as uncompressed CSV under `Files/csv` in the lakehouse.

REPLACES cells 7 and 8 of the ETL notebook (`download` and `unzip`), which pulled zips from a
GitHub mirror into the Fabric mount, unpacked them into `/lakehouse/default/Files/csv/` and kept
the zips next to them. The shape here is `download_aemo.py` from
djouallah/direct-lake-parquet-layout, the landing step that repo runs on a free runner before
every build, trimmed to what this benchmark needs:

* THE LISTING comes from nemweb's own index (`Reports/Current/Daily_Reports/`, the last couple of
  months) and is backfilled from the GitHub mirror `djouallah/aemo_data` only when nemweb does not
  list enough. Plain `urllib` + `re` + `json` in place of DuckDB's `read_text`.
* NEWEST FIRST, name-sorted descending, take what is missing -- the notebook's
  `sorted(filelist, reverse=True)[:files_needed]`. The file names embed the date, so the sort is
  chronological.
* UNZIP IN MEMORY, UPLOAD UNCOMPRESSED. A daily zip is ~5 MB and holds one ~52 MB CSV. Batches
  of 7 on 8 threads, three retries with backoff, nemweb's required User-Agent. Nothing is kept on
  the runner and no zip is stored anywhere: the engines read CSV, so CSV is what lands.
* THE WATERMARK IS THE DIRECTORY LISTING, not the reference's archive-log parquet. The only
  question here is "are there at least N CSVs" -- the notebook's own idempotency rule
  ("Folder already has N file(s). No download needed.") -- and ADLS answers it directly.

The transport is this repo's ADLS client (bench/onelake.py) rather than duckrun/obstore, so the
upload is signed by the same refreshing OIDC credential as everything else here.
"""

from __future__ import annotations

import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor

from bench import onelake, scrub
from bench.etl.config import EtlConfig
from bench.etl.data import list_csvs

NEMWEB_DAILY = "https://nemweb.com.au/Reports/Current/Daily_Reports/"
NEMWEB_HOST = "https://nemweb.com.au"
MIRROR = "https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/{year}"
MIRROR_YEARS = range(2018, 2027)

BATCH_SIZE = 7
WORKERS = 8
RETRIES = 3

# nemweb's index is an HTML directory listing: one `<A HREF="/Reports/.../X.zip">` per file.
_HREF = re.compile(r'HREF="([^"]*?/(PUBLIC_DAILY[^"/]*?)\.zip)"', re.IGNORECASE)

Fetch = Callable[[str], bytes]


def _fetch(url: str, timeout: int = 120) -> bytes:
    """GET `url` with retries.

    nemweb refuses the default urllib agent; GitHub's API is rate-limited to 60/hour anonymously,
    so the Actions token goes on those calls when it is present.
    """
    headers = {"User-Agent": "Mozilla/5.0 (bench-etl)"}
    token = os.environ.get("GITHUB_TOKEN")
    if token and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    for attempt in range(RETRIES):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            if attempt == RETRIES - 1:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def parse_nemweb(html: str) -> dict[str, str]:
    """`name -> url` for every PUBLIC_DAILY zip in nemweb's index page. `name` has no extension."""
    return {name: f"{NEMWEB_HOST}{href}" for href, name in _HREF.findall(html)}


def parse_mirror(payload: bytes | str) -> dict[str, str]:
    """`name -> download_url` for the PUBLIC_DAILY zips in one GitHub contents listing."""
    items = json.loads(payload)
    out: dict[str, str] = {}
    for item in items:
        name = item.get("name", "")
        if name.startswith("PUBLIC_DAILY") and name.endswith(".zip"):
            out[name[: -len(".zip")]] = item["download_url"]
    return out


def select(available: dict[str, str], present: Iterable[str], want: int) -> list[tuple[str, str]]:
    """Which files to land: the newest `want - len(present)` not already present, newest first.

    The notebook's rule: `files_needed = total_files - len(current)` and then
    `[f for f in filelist if f['name'] not in current][:files_needed]` over a list sorted by name
    descending. `present` holds CSV names; a zip and the CSV inside it share a stem.
    """
    have = {_stem(name) for name in present}
    needed = max(0, want - len(have))
    newest_first = sorted(available, reverse=True)
    return [(name, available[name]) for name in newest_first if name not in have][:needed]


def _stem(name: str) -> str:
    return name[:-4] if name.upper().endswith((".CSV", ".ZIP")) else name


def listing(present: Iterable[str], want: int, fetch: Fetch = _fetch) -> dict[str, str]:
    """nemweb's index, backfilled from the mirror only when nemweb cannot cover the shortfall."""
    present = list(present)
    found = parse_nemweb(fetch(NEMWEB_DAILY).decode("utf-8", "replace"))
    needed = max(0, want - len({_stem(n) for n in present}))
    if len(select(found, present, want)) >= needed:
        return found
    for year in MIRROR_YEARS:
        try:
            for name, url in parse_mirror(fetch(MIRROR.format(year=year))).items():
                found.setdefault(name, url)
        except urllib.error.HTTPError as exc:
            # A year the mirror does not have yet is not an error.
            scrub.safe_print(f"  mirror {year}: HTTP {exc.code}, skipped")
    return found


def _csv_members(zip_bytes: bytes) -> list[tuple[str, bytes]]:
    """Every `*.CSV` inside the zip, read into memory. A nested path becomes a flat name."""
    archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    return [
        (member.replace("/", "_"), archive.read(member))
        for member in archive.namelist()
        if member.upper().endswith(".CSV")
    ]


def _batches(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def land(cfg: EtlConfig, fetch: Fetch = _fetch, fs=None) -> dict:
    """Ensure `Files/csv` holds at least `cfg.files` CSVs. Idempotent; returns a small summary."""
    fs = fs if fs is not None else onelake.file_system(cfg)
    present = list_csvs(fs, cfg)
    if len(present) >= cfg.files:
        scrub.safe_print(
            f"Files/csv already has {len(present)} file(s) (>= {cfg.files}). No download needed."
        )
        return {"present": len(present), "landed": 0, "bytes": 0, "skipped": True}

    available = listing(present, cfg.files, fetch)
    todo = select(available, present, cfg.files)
    scrub.safe_print(
        f"Files/csv has {len(present)}, want {cfg.files}: {len(todo)} new file(s) to land "
        f"({len(available)} listed)"
    )
    if len(todo) < cfg.files - len(present):
        raise RuntimeError(
            f"only {len(todo)} file(s) available to land; {cfg.files - len(present)} needed"
        )

    directory = fs.get_directory_client(cfg.csv_relative)
    if not directory.exists():
        directory.create_directory()

    def land_one(item: tuple[str, str]) -> list[tuple[str, int]]:
        name, url = item
        uploaded = []
        for csv_name, payload in _csv_members(fetch(url)):
            fs.get_file_client(f"{cfg.csv_relative}/{csv_name}").upload_data(
                payload, overwrite=True
            )
            uploaded.append((csv_name, len(payload)))
        if not uploaded:
            raise RuntimeError(f"{name}.zip holds no CSV")
        return uploaded

    landed, nbytes = 0, 0
    started = time.perf_counter()
    for number, batch in enumerate(_batches(todo, BATCH_SIZE), start=1):
        with ThreadPoolExecutor(WORKERS) as pool:
            for uploaded in pool.map(land_one, batch):
                for _csv_name, size in uploaded:
                    landed += 1
                    nbytes += size
        scrub.safe_print(
            f"  batch {number}: {landed} file(s), {nbytes / 2**30:.2f} GB, "
            f"{time.perf_counter() - started:.0f}s"
        )
    return {"present": len(present) + landed, "landed": landed, "bytes": nbytes, "skipped": False}
