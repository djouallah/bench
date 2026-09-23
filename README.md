
**Write with pyiceberg, read Back using Onelake Catalog.** 

OneLake is the example here, not the requirement. Any catalog that exposes an Iceberg REST
endpoint should work, and the raw data can sit on `abfss://`, `s3://` or anything else the engine
can read.

**TPC-H, scale factor 10: the 22 queries.** Full numbers in [docs/RESULTS.md](docs/RESULTS.md).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/charts/totals-dark.png">
  <img alt="Total seconds for all 22 queries, per engine" src="docs/charts/totals.png">
</picture>

**TPC-DS, scale factor 10: the 99 queries, same catalog.** Full numbers in
[docs/tpcds/RESULTS.md](docs/tpcds/RESULTS.md).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/tpcds/charts/totals-dark.png">
  <img alt="TPC-DS total seconds for all 99 queries, per engine" src="docs/tpcds/charts/totals.png">
</picture>

**ETL: read the CSVs from OneLake, transform, write Iceberg.** 1000 daily files are landed once in the lakehouse's `Files/` section; each engine
then reads all of them, filters and casts, and writes one Iceberg table of 149,146,763 rows into
`Tables/` through the same OneLake REST catalog.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/etl/charts/totals-dark.png">
  <img alt="Seconds to read 1000 CSVs and write one Iceberg table, per engine, fastest first" src="docs/etl/charts/totals.png">
</picture>
