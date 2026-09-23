"""Can Firebolt Core read the bench's OneLake Iceberg tables at all? One run of ~5 minutes answers.

WHY A PROBE BEFORE AN ENGINE. Firebolt's docs say Iceberg data files must live on S3: the only
documented STORAGE_CREDENTIALS are AWS keys, and BEARER_TOKEN is documented for Databricks Unity
alone. Azure Blob appears only as the k8s operator's MANAGED storage. The engine is C++ with an
Azure client inside it, so an undocumented path may still work -- but building a full engine
module on that guess is the expensive way to find out. Each probe isolates one layer:

  1. the server answers at all, and which build it is
  2. READ_ICEBERG over the OneLake REST catalog with our bearer -- the path an engine would use
  3. READ_ICEBERG on the table's metadata.json directly -- storage reads, no catalog
  4. READ_PARQUET on one data file, abfss:// and https:// -- storage alone, the smallest step
  5. CREATE LOCATION on an abfss URL -- its error names the credential keys it would accept

A FAIL on 2 with a PASS on 3/4 says the catalog auth is the problem; FAILs on 3 and 4 say Azure
storage is. Every probe runs regardless of the others, because the pattern is the result.

Every statement carries the token in its text, and Firebolt quotes statements back in errors, so
every line printed goes through `scrub`.
"""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS, Config

FIREBOLT = "http://127.0.0.1:3473/?output_format=psql"


def _sql(statement: str, timeout: int = 300) -> str:
    request = urllib.request.Request(FIREBOLT, data=statement.encode(), method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode(errors="replace")
    except urllib.error.HTTPError as exc:
        # The body is Firebolt's error message; the status line alone says nothing.
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')}") from None


def _wait_ready(seconds: int = 180) -> None:
    deadline = time.time() + seconds
    while True:
        try:
            _sql("SELECT 1", timeout=10)
            return
        except Exception:  # noqa: BLE001 - not up yet
            if time.time() > deadline:
                raise
            time.sleep(3)


def _probe(label: str, statement: str) -> bool:
    print(f"\n[{label}]", flush=True)
    started = time.perf_counter()
    try:
        out = _sql(statement)
    except Exception as exc:  # noqa: BLE001 - this script's whole job is reporting failures
        print(f"    FAIL  ({time.perf_counter() - started:.1f}s)  {scrub.scrub_exc(exc, 1500)}")
        return False
    print(f"    PASS  ({time.perf_counter() - started:.1f}s)\n{scrub.scrub(out[:1500])}")
    return True


def main() -> int:
    cfg = Config.from_env()
    _wait_ready()
    token = auth.onelake_token()

    # The table's own paths come from pyiceberg, so probes 3 and 4 read files that certainly exist.
    table = auth.catalog(cfg).load_table(f"{cfg.schema}.nation")
    metadata = table.metadata_location
    data_file = next(iter(table.scan().plan_files())).file.file_path
    https_file = data_file.replace(f"abfss://{cfg.workspace_id}@{ONELAKE_DFS}/",
                                   f"https://{ONELAKE_DFS}/{cfg.workspace_id}/")
    print(f"metadata  {metadata}\ndata file {data_file}")

    results = {
        "1 version": _probe("1 version", "SELECT version()"),
        "2 rest catalog": _probe("2 READ_ICEBERG, OneLake REST catalog, bearer", f"""
            SELECT count(*) FROM READ_ICEBERG(
              URL => '{ICEBERG_ENDPOINT}', WAREHOUSE => '{cfg.warehouse}',
              NAMESPACE => '{cfg.schema}', TABLE => 'nation', BEARER_TOKEN => '{token}')"""),
        "3 metadata.json": _probe("3 READ_ICEBERG, metadata.json on abfss",
                                  f"SELECT count(*) FROM READ_ICEBERG(URL => '{metadata}')"),
        "4a parquet abfss": _probe("4a READ_PARQUET, abfss",
                                   f"SELECT count(*) FROM READ_PARQUET(URL => '{data_file}')"),
        "4b parquet https": _probe("4b READ_PARQUET, https",
                                   f"SELECT count(*) FROM READ_PARQUET(URL => '{https_file}')"),
        "5 location": _probe("5 CREATE LOCATION on abfss", f"""
            CREATE LOCATION onelake_probe WITH SOURCE = CLOUD_STORAGE
              URL = '{cfg.base_path}/Tables/' CREDENTIALS = (BEARER_TOKEN = '{token}')"""),
    }

    # ROUND 2, from round 1's answer. Every Azure-shaped URL above failed on the SCHEME, and the
    # CREATE LOCATION error named the one Firebolt takes: "azure:// for Azure Blob Storage". Its
    # authority layout and credential keys are undocumented, so try both common layouts, and let
    # a LOCATION error name the credential keys it accepts.
    rel = data_file.split(f"/{cfg.lakehouse_id}/", 1)[1]
    blob = "onelake.blob.fabric.microsoft.com"
    layouts = {
        "container@host": f"azure://{cfg.workspace_id}@{blob}/{cfg.lakehouse_id}/{rel}",
        "account/container": f"azure://onelake/{cfg.workspace_id}/{cfg.lakehouse_id}/{rel}",
    }
    for name, url in layouts.items():
        results[f"6 parquet azure {name}"] = _probe(
            f"6 READ_PARQUET, azure:// {name}", f"SELECT count(*) FROM READ_PARQUET(URL => '{url}')")
    for i, creds in enumerate(["", f"CREDENTIALS = (BEARER_TOKEN = '{token}')",
                               f"CREDENTIALS = (AZURE_BEARER_TOKEN = '{token}')"]):
        loc = f"onelake_probe_{i}"
        results[f"7 location {i}"] = _probe(f"7 CREATE LOCATION azure:// #{i}", f"""
            CREATE LOCATION {loc} WITH SOURCE = CLOUD_STORAGE
              URL = '{layouts["container@host"].rsplit("/", 1)[0]}/' {creds}""")
        if results[f"7 location {i}"]:
            results[f"8 parquet via {loc}"] = _probe(
                f"8 READ_PARQUET via {loc}",
                f"SELECT count(*) FROM READ_PARQUET(LOCATION => '{loc}', PATTERN => '*.parquet')")

    print("\nSUMMARY")
    for name, ok in results.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    # Green only when the catalog path an engine would use works; the rest is diagnosis.
    return 0 if results["2 rest catalog"] else 1


if __name__ == "__main__":
    sys.exit(main())
