"""The Light ETL benchmark: read N AEMO daily CSVs from OneLake, write one Iceberg table per engine.

The second benchmark in this repo, ported from `Light_ETL_Python_Notebook.ipynb` the way `bench/`
was ported from the TPC-H notebook. Same scaffolding -- OIDC auth, one engine per job, immutable
JSON results -- with the notebook's Delta writers replaced by Iceberg and Spark added as a seventh
engine. Every module docstring names the notebook cell it replaces.
"""
