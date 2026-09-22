"""Spark exactly as pyspark_iceberg, with Gluten + Velox executing the plan natively.

NO RELEASED PACKAGE, BUT A NIGHTLY ONE. Gluten's releases stop at Spark 3.5; Apache's nightly
builds carry a Velox bundle for Spark 4.1 (since 1.7.0-SNAPSHOT, 2026-06). Compiling Velox in
CI was the alternative -- hours cold, tens of GB of disk -- and is deliberately not done: if the
nightly cannot read OneLake, Gluten is out.

SPARK 4.1.1, NOT 4.1.3. The bundle warns "Spark runtime version 4.1.3 is not matched with
Gluten's fully tested version 4.1.1", so this engine pins its own pyspark to what Gluten tests
(requirements/pyspark_gluten_iceberg.txt). Iceberg's runtime and hadoop-azure are per-minor, so
pyspark_iceberg's PACKAGES serve 4.1.1 unchanged.

THE QUESTION THIS ENGINE ANSWERS FIRST is abfss://. Velox reads files itself, through its own
ABFS connector, not hadoop-azure -- so whether the bundle was built with that connector, and
whether it accepts the credential Spark's Hadoop reads use, decides everything. Operators
Gluten cannot run natively fall back to the JVM; that is normal Gluten behaviour, not a failure.

THE SNAPSHOT IS OVERWRITTEN NIGHTLY under the same name, so the jar's sha256 is printed at
setup: that, not the file name, is what identifies the build a run measured.

MEMORY. Velox allocates OFF-HEAP, so the 11g heap every other Spark run gets would leave it
nothing on a 16GB runner. The heap shrinks to 4g and Velox gets 8g; the total is the same.
"""

from __future__ import annotations

import hashlib
import os
import urllib.request
from pathlib import Path

from bench import scrub
from bench.tpch.engines.pyspark_iceberg import PysparkIceberg

# The JDK 17 directory, because every Spark job sets up Temurin 17. amd64: the hosted runners.
GLUTEN_JAR_URL = (
    "https://nightlies.apache.org/gluten/nightly-release-jdk17/"
    "gluten-velox-bundle-spark4.1_2.13-linux_amd64-1.8.0-SNAPSHOT.jar"
)

HEAP = "4g"
OFF_HEAP = "8g"


def fetch_gluten_jar() -> Path:
    """Download the bundle once per job and print which build it is."""
    target = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / Path(GLUTEN_JAR_URL).name
    if not target.exists():
        urllib.request.urlretrieve(GLUTEN_JAR_URL, target)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    scrub.safe_print(f"  gluten bundle {target.name} sha256 {digest[:16]}")
    return target


def gluten_conf() -> dict[str, str]:
    """The Gluten session keys. Shared with smoke_sql.py, so phase 1 runs the same plugin.

    Also shrinks the driver heap, which only works because it runs before the JVM starts:
    spark-submit reads SPARK_DRIVER_MEMORY when pyspark launches the gateway.
    """
    os.environ["SPARK_DRIVER_MEMORY"] = HEAP
    return {
        # THE APP CLASSPATH, NOT spark.jars. ColumnarShuffleManager lives in Spark's own
        # org.apache.spark.shuffle.sort package and calls package-private classes there; loaded
        # through spark.jars' child classloader it is a different runtime package, and every
        # shuffle died with IllegalAccessError on BypassMergeSortShuffleWriter.
        "spark.driver.extraClassPath": str(fetch_gluten_jar()),
        "spark.plugins": "org.apache.gluten.GlutenPlugin",
        # ANSI OFF. It is Spark 4's default, and Gluten's answer to it is to fall back WHOLESALE:
        # every node of every plan tagged "does not support ansi mode", so Velox ran nothing.
        # The cost: doubleQuotedIdentifiers only works under ANSI, so the TPC-DS statements that
        # alias `AS "order count"` will not parse here. TPC-H has none.
        "spark.sql.ansi.enabled": "false",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": OFF_HEAP,
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
        # Gluten's Arrow/netty buffers need reflective access on JDK 17.
        "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
    }


class PysparkGlutenIceberg(PysparkIceberg):
    name = "pyspark_gluten_iceberg"

    def _extra_config(self) -> dict[str, str]:
        return gluten_conf()
