"""Spark exactly as pyspark_iceberg, with Gluten + Velox executing the plan natively.

NO RELEASED PACKAGE, BUT A NIGHTLY ONE. Gluten's releases stop at Spark 3.5; Apache's nightly
builds carry a Velox bundle for Spark 4.1 (since 1.7.0-SNAPSHOT, 2026-06). Compiling Velox in
CI was the alternative -- hours cold, tens of GB of disk -- and is deliberately not done: if the
nightly cannot read OneLake, Gluten is out.

SPARK 4.1.1, NOT 4.1.3. The bundle warns "Spark runtime version 4.1.3 is not matched with
Gluten's fully tested version 4.1.1", so this engine pins its own pyspark to what Gluten tests
(requirements/pyspark_gluten_iceberg.txt). Iceberg's runtime and hadoop-azure are per-minor, so
pyspark_iceberg's PACKAGES serve 4.1.1 unchanged.

THE CREDENTIAL IS A OneLake SAS, not the OIDC token file every other Spark engine uses. Velox
reads data files itself, through its own ABFS connector, and that connector knows three auth
types: SharedKey, OAuth WITH A CLIENT SECRET, and SAS (velox/.../abfs/AzureClientProviderImpl.cpp).
This app registration has no secret and OneLake has no account key, so SAS it is: a user
delegation SAS minted at setup from the same OIDC credential, read+list, scoped to the lakehouse.
Velox's key, `fs.azure.sas.fixed.token.<account>`, is also hadoop-azure's, so the JVM side
(Iceberg metadata, any fallback scan) reads with the same token. OneLake caps its lifetime at one
hour -- fine for a smoke, a limit for a long benchmark.

ACCOUNT-SCOPED KEYS ONLY. Velox registers a provider for every key starting with
`fs.azure.account.auth.type` by cutting the account off the end, so the base engine's unscoped
copy crashed the native backend at startup: `substr: __pos (which is 27) > __size (which is 26)`.

THE SNAPSHOT IS OVERWRITTEN NIGHTLY under the same name, so the jar's sha256 is printed at
setup: that, not the file name, is what identifies the build a run measured.

MEMORY. Velox allocates OFF-HEAP, so the 11g heap every other Spark run gets would leave it
nothing on a 16GB runner. The heap shrinks to 4g and Velox gets 8g; the total is the same.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import urllib.request
from pathlib import Path

from bench import auth, scrub
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
        # ANSI STAYS ON, as in stock Spark. Gluten's default answer to it is to fall back
        # WHOLESALE -- every node tagged "does not support ansi mode", so Velox ran nothing -- and
        # this asks Velox to execute ANSI instead. Not ANSI off: doubleQuotedIdentifiers only
        # works under ANSI, and eight TPC-DS statements alias `AS "order count"`.
        "spark.gluten.sql.ansiFallback.enabled": "false",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": OFF_HEAP,
        "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
        # Gluten's Arrow/netty buffers need reflective access on JDK 17.
        "spark.driver.extraJavaOptions": "-Dio.netty.tryReflectionSetAccessible=true",
    }


# OneLake's ceiling for a user delegation key, and so for the SAS signed with it.
SAS_LIFETIME = dt.timedelta(hours=1)


def onelake_sas(workspace_id: str, lakehouse_id: str) -> str:
    """A read+list user delegation SAS on the lakehouse directory, signed via Entra."""
    from azure.storage.filedatalake import (
        DataLakeServiceClient,
        DirectorySasPermissions,
        generate_directory_sas,
    )

    from bench.config import ONELAKE_DFS

    start = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)
    expiry = start + SAS_LIFETIME
    service = DataLakeServiceClient(f"https://{ONELAKE_DFS}", credential=auth.credential())
    key = service.get_user_delegation_key(start, expiry)
    sas = generate_directory_sas(
        account_name="onelake",
        file_system_name=workspace_id,
        directory_name=lakehouse_id,
        credential=key,
        permission=DirectorySasPermissions(read=True, list=True),
        expiry=expiry,
        start=start,
    )
    scrub.register(sas)
    return sas


class PysparkGlutenIceberg(PysparkIceberg):
    name = "pyspark_gluten_iceberg"

    def _storage_conf(self, abfs: dict[str, str], account: str) -> dict[str, str]:
        sas = onelake_sas(self.cfg.workspace_id, self.cfg.lakehouse_id)
        scrub.safe_print(f"  onelake SAS for Velox and hadoop-azure, valid {SAS_LIFETIME}")
        return {
            f"spark.hadoop.fs.azure.account.auth.type.{account}": "SAS",
            f"spark.hadoop.fs.azure.sas.fixed.token.{account}": sas,
        }

    def _extra_config(self) -> dict[str, str]:
        return gluten_conf()
