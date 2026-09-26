"""Spark with Gluten + Velox: the pyspark_iceberg ETL, on the TPC-H Gluten engine's session.

Everything but the session is pyspark_iceberg's -- the read split, the transform shared with
LakeSail, the create through the Iceberg Java API, the one append. The session is
`bench.tpch.engines.pyspark_gluten_iceberg.PysparkGlutenIceberg.setup()`: the nightly bundle,
the off-heap split, the CA link, the Velox cache.

ONE DIFFERENCE, THE SAS. The read benchmarks' SAS is read+list; here Spark writes the data files
and manifests over hadoop-azure with that same token and DROP ... PURGE deletes them, so this
engine asks for create, write and delete too. The token lives ~55 minutes and the ETL job is
capped at 50, so the restart-on-expiry the read benchmarks need never fires here.

THE CSV SCAN IS NOT VELOX'S. Open-source Gluten on Spark 4.x has no CSV reader, so the scan falls
back to the JVM (`Unsupported file format TextReadFormat`) and Velox only runs what comes after it.
Fabric's Native Execution Engine does read CSV natively; that parser is Microsoft's own, not in
Gluten. LEARNING.md has the details and the upstream PRs. The time is the load, as for every other
engine, whichever side ran it.
"""

from __future__ import annotations

from bench.etl.engines.pyspark_iceberg import PysparkIceberg
from bench.tpch.engines.pyspark_gluten_iceberg import PysparkGlutenIceberg as _TpchGluten


class _WritingGluten(_TpchGluten):
    sas_write = True


class PysparkGlutenIceberg(PysparkIceberg):
    name = "pyspark_gluten_iceberg"
    _session_engine = _WritingGluten
