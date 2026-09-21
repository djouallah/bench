# Running it yourself

You need a Microsoft Fabric workspace you own and a GitHub account. At the end of this you have a
public fork that generates TPC-H data into your lakehouse with pyiceberg and benchmarks four
engines against it on a schedule, with no secret stored anywhere.

## 1. Fork it, and make the fork public

Required, not a preference: 4-vCPU/16 GB runners and unlimited minutes are **public-repo only**. A
private fork gets 2 vCPU / 7 GB, and SF 10 will be OOM-killed.

## 2. Register an Entra app

Azure portal → Entra ID → App registrations → New. **No client secret, no API permissions.**
Record the Application (client) ID and Directory (tenant) ID.

## 3. Add a federated credential

App → Certificates & secrets → Federated credentials → Add:

| Field | Value |
|---|---|
| Scenario | GitHub Actions deploying Azure resources |
| Entity type | Branch |
| Branch | `main` |
| Subject | `repo:<you>/bench:ref:refs/heads/main` |
| Audience | `api://AzureADTokenExchange` — the default, do not change it |

This is the entire OIDC setup. No secret is ever stored.

## 4. Turn on two Fabric tenant settings

Needs a Fabric admin:

- **Service principals can use Fabric APIs** — on, scoped to a group containing the app.
- **Users can access data stored in OneLake with apps external to Fabric** — on.

Without both you get a 401/403 that looks like a role problem and is not. Probes 2 and 3 of the
auth smoke test tell them apart.

## 5. Grant the app Contributor on the workspace

Workspace → Manage access → add the service principal as **Contributor**. Viewer is not enough:
the `prepare` job creates a namespace, creates Iceberg tables and commits snapshots.

## 6. Use a lakehouse with schemas enabled

The dataset lives in namespaces (`CH0001`, `CH0010`, …), so the lakehouse must have been created
with the **Lakehouse schemas** option. It cannot be enabled afterwards.

The schema naming is deliberately identical to the Fabric notebook this was ported from, so a
notebook and this workflow can point at the same lakehouse and share generated data.

## 7. Set secrets and variables

| Kind | Name | What |
|---|---|---|
| Secret | `AZURE_CLIENT_ID` | from step 2 |
| Secret | `AZURE_TENANT_ID` | from step 2 |
| Secret | `FABRIC_WORKSPACE_ID` | workspace GUID |
| Secret | `FABRIC_LAKEHOUSE_ID` | lakehouse GUID |
| Variable | `TPCH_SF` | default scale factor, optional (default 10) |

No client secret, no storage account key, no SAS token anywhere — OneLake refuses account keys and
SAS outright.

## 8. Run it, in this order

Each step is designed to fail cheaply and tell you *which* of steps 2–6 is wrong.

| Run | Time | What it proves |
|---|---|---|
| **auth smoke** | ~1 min | the whole credential chain, one probe per setting |
| **tpch bench** · sf=1 · `duckdb_iceberg` | ~6 min | generate → attach → 22 queries → publish, end to end |
| **tpch bench** · sf=1 · all four | ~10 min | the matrix and all four attach paths |
| **tpch bench** · sf=10 · all four | ~25 min | the real sizing |

Then grep the run logs and `results/*.json` for your token prefix, and only then uncomment the
`schedule:` block at the top of [`bench.yml`](.github/workflows/bench.yml).

## Cost warning

OneLake storage, egress and any Fabric CU are billed to **your** workspace, not to GitHub. A
nightly SF 10 four-engine run reads tens of GB out of OneLake. Start at SF 1.

## Gotchas

Four things that are invisible from inside a Fabric notebook, because a notebook's own identity
and defaults paper over every one of them. All four are already configured in this repo — this is
here so a failure is recognisable, not so you have to do anything.

**DuckDB needs the curl transport on Linux.** If `AZURE_TRANSPORT_OPTION_TYPE` is unset, the
Iceberg `ATTACH` succeeds and then every data-file read fails with `AzureStorageFileSystem could
not open file`. That reads like a missing credential and is not one — a bad credential says
`Unauthorized`. The catalog is reached by the iceberg extension; data files go through the azure
extension, whose default transport fails the OneLake TLS handshake on Linux. On Windows it is the
reverse. `bench.yml` sets it; `config.azure_transport()` handles a local run.

**Credential vending is deliberately off.** The catalog will vend a per-table storage credential,
and it works — including on tables pyiceberg registered externally. It is not used because the
fetch happens per table inside the first query that touches it, which lands entirely on the cold
pass and cannot be cached away. chDB and LakeSail authenticate storage with a single bearer token
and pay nothing comparable, so leaving it on would time DuckDB on Fabric's catalog latency while
the others are timed on execution. `ACCESS_DELEGATION_MODE 'none'` plus a `CREATE SECRET`.

**Every engine gets a catalog metadata cache.** chDB `iceberg_metadata_staleness_ms`, LakeSail
`table_cache_ttl_secs`, DuckDB `MAX_TABLE_STALENESS`. An engine re-resolving every table per query
is timed on catalog round-trips instead of execution.

**pyiceberg needs `adlfs`, and fails late without it.** It picks a FileIO per scheme; `abfss://`
gets `FsspecFileIO`, which imports adlfs lazily — so `pyiceberg[pyarrow]` installs cleanly,
creates tables over REST happily, then dies inside `add_files` after the whole dataset has been
uploaded. Hence `pyiceberg[pyarrow,adlfs]`.

Probes 4 and 5 of the auth smoke test exercise **different HTTP stacks** — adlfs and DuckDB's
azure extension. Probe 4 passed happily while DuckDB could not read a byte, which is why both
exist.

## The smoke test, and what it is for

`smoke.yml` runs before anything expensive, in two phases:

1. **`sql`** — generates TPC-H locally and runs all 22 queries against plain parquet. No
   credentials, no Fabric, about a minute. It answers *can this engine speak the dialect*.
2. **`catalog`** — dispatch-only. Calls the engine's real `setup()` against OneLake and reads two
   queries, proving the credential and storage path.

Because phase 1 hands every engine identical local data, `compare` also checks that they all
return the **same row count for every query**. That is a correctness signal the benchmark itself
cannot produce, and it is not theoretical: it caught chDB returning 41 rows for Q13 where the
other three returned 42. ClickHouse defaults `join_use_nulls=0`, filling an unmatched outer-join
cell with the column's default value (`0`) instead of `NULL`, so `COUNT(o_orderkey)` counted them
and the `c_count=0` group vanished. Wrong in every published chDB result until then, at every
scale factor, and completely invisible to a timing chart.

Run one engine locally with no credentials at all:

```bash
python .github/scripts/smoke_sql.py duckdb_iceberg
```

## Adding an engine, and what Daft cost to find out

A new engine is a requirements file, a class implementing `setup`/`execute`/`close`, entries in
`bench/config.py`, `bench/engines/__init__.py`, `bench/queries.py` and `bench/charts.py`, and a
local-registration adapter in `smoke_sql.py`. Then `smoke.yml` before `bench.yml`, always.

**Daft is the worked example, and it did not make it in.** It looked ideal — Rust, in-process,
takes a PyIceberg `Table` directly, and `AzureConfig(bearer_token=…, use_fabric_endpoint=True)`
needs none of the credential archaeology DuckDB and LakeSail did. The engine
(`bench/engines/daft_iceberg.py`) works and is kept so the smoke test can track it. It is **not**
in `bench.yml`'s engine list, because at 0.7.25 it runs **16 of the 22 queries**:

| Q | failure |
|---|---|
| 1, 9 | `Cannot infer supertypes for multiply/subtract` — `Decimal[38,4] × Decimal[23,2]` wants precision 61, and Daft's ceiling is 38 |
| 8, 14 | `if_true and if_false ... Decimal[38,4] and literal#Int64` — the same coercion gap, seen inside a `CASE` |
| 11 | `Unsupported join type: CrossJoin(None)` |
| 22 | `` `SUBSTRING(expr [FROM start] [FOR len])` syntax`` |

Nothing exotic is missing: correlated subqueries, `EXISTS`, `IN (SELECT …)` and CTEs all work.
The blocker is decimal arithmetic. TPC-H's `l_extendedprice * (1 - l_discount)` overflows Daft's
precision ceiling where DuckDB, chDB, Polars and LakeSail all widen or fall back to float.

Daft also rejects backticks outright — *"Daft only supports delimited identifiers with
double-quotes"* — which is why `IDENT_STYLE` has a third value, `quoted`. That failed all 22
queries on its own and looked exactly like a total dialect failure until the error was read.

Rewriting the six queries would get Daft to 22/22, but a per-engine rewrite is not a benchmark —
and casting the decimals to double changes what every other engine computes too. So the gaps stay
visible, and the smoke test will say when Daft closes them.

### Comet: blocked on `abfss://`, not on anything you would guess

[Apache DataFusion Comet](https://datafusion.apache.org/comet/) was evaluated as a native
execution plugin for the Spark engine and rejected. Both things you would expect to stop it do
not: Spark 4.1 **is** supported (`org.apache.datafusion:comet-spark-spark4.1_2.13:1.0.0`), and
Comet **does** ship a native Iceberg reader that is enabled by default, tested against Iceberg
1.5–1.11, and documented as working with REST catalogs.

It fails on storage. From `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala`:

```scala
private val icebergReadableSchemes: Set[String] = Set("file", "s3", "s3a", "gs", "oss")
```

No `abfss`, and the comment above that line says the omission is deliberate — admitting Azure
*"turns a clean JVM fallback into a native runtime 'Unsupported storage scheme' error"*, because
iceberg-rust's OpenDAL storage factory cannot build it. OneLake scans are declined at planning
time and run on the JVM exactly as they do without Comet, so the plugin would cost off-heap
memory out of an already tight 16GB and accelerate nothing.

It would also break the credential path if it ever did take over. Comet's native scans run
outside the JVM and bypass Hadoop's ABFS driver entirely, reimplementing auth in Rust from a
fixed allowlist of `fs.azure.*` keys — a custom `fs.azure.account.oauth2.token.provider.type` is
ignored. `WorkloadIdentityTokenProvider`, the only reason Spark reaches OneLake without a client
secret, would never be called. Separately, Comet probes account-scoped keys only under
`dfs.core.windows.net` and `blob.core.windows.net`, and derives the account from the first host
label, so `onelake.dfs.fabric.microsoft.com` is outside its tested URL shape regardless.

**The trigger to revisit is one line:** `abfss` appearing in `icebergReadableSchemes`.
iceberg-rust has landed experimental `storage-azdls` support, so it is plausible — though
Comet's roadmap names HDFS, not Azure, as the next storage gap. Nothing else would need to
change; the Spark version and the catalog already line up.

### LakeSail: the table cache caches listings, not tables

`bench/config.py` sets one catalog-cache lifetime for every engine, 15 minutes, on the theory that
an engine re-resolving tables over REST on every statement is being timed on round trips to Fabric
rather than on execution. For DuckDB (`MAX_TABLE_STALENESS`), chDB and Spark (Iceberg's
`CachingCatalog`) that is what the setting does. For LakeSail it is not.

Sail's `table_cache_type` / `table_cache_ttl_secs` are a cache of the **list of tables in a
namespace**, consulted by `list_tables` only. Its own docs say "table listing cache"; the bench
misread the name. The two calls a query makes, `get_table` and `begin_table_access`, pass straight
through to the REST provider, which calls `loadTable` every time
(`sail-catalog/src/provider/cache.rs:314`, `sail-catalog-iceberg/src/provider.rs:1434` at v0.7.1).
Only `/v1/config` is fetched once.

You can see it in the log artifact: every statement emits one
`advertises vended storage credentials, which is not implemented yet` line per table it touches,
both passes, because that warning is built from a fresh `loadTable` result. Two consequences:

- Sail's small queries have a floor of 1.5–3s (Q11, Q16, Q22 at SF=10) where the engines that do
  cache metadata answer in under a second against the same tables. That floor is REST, not Sail.
- A stalled `loadTable` lands in whatever statement runs next. Every Sail run so far has had one
  20–25s stall in the warm pass on a different query each time (Q9, Q16, Q20) — the log shows the
  gap between resolving one table and the next — which is why Sail is the engine whose warm total
  can come out worse than its cold one.

The setting stays in the engine because it is harmless and will matter the day Sail caches the
loaded table. Filed upstream: https://github.com/lakehq/sail/issues/2629.

---

Back to the [README](README.md).
