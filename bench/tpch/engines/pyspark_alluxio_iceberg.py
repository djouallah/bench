"""Spark exactly as pyspark_iceberg, plus a LOCAL DISK FILE CACHE -- the thing stock Spark lacks.

WHY THIS ENGINE EXISTS. DuckDB and chDB keep the bytes they read from OneLake on local disk, so
their warm pass reads locally. Open-source Spark has nothing equivalent: the vendor runtimes each
built one in-house (Databricks' disk cache, Fabric's Intelligent Cache -- per-node SSD, stale
check against the remote tag, LRU eviction) and none of it was upstreamed. pyspark_iceberg stays
the STOCK number; this is the "what if Spark had a cache" number beside it, and the gap between
the two is the cache and nothing else.

THE CACHE IS ALLUXIO'S CLIENT, NOT AN ALLUXIO CLUSTER. `alluxio.hadoop.LocalCacheFileSystem`
(Apache-2.0, the page cache Presto and Trino embed) is a Hadoop FileSystem that checks local disk
pages before calling the FileSystem it wraps. No master, no worker, no daemon. Its one obstacle
is that it has no no-arg constructor, so `fs.abfss.impl` cannot name it; java/bench/*.java is
the ~20-line shim that builds the ABFS client and hands it over. It is compiled HERE, at setup,
against pyspark's own hadoop-client-api and the same hadoop-azure the base engine pins, so there
is no build step and no second Hadoop on the classpath.

Every other setting -- catalog, credentials, readahead, AQE, the catalog cache -- is inherited
unchanged, so nothing but the file cache differs between the two Spark lines.
"""

from __future__ import annotations

import os
import subprocess
import urllib.request
from pathlib import Path

import pyspark

from bench import scrub
from bench.tpch.engines.pyspark_iceberg import PysparkIceberg

MAVEN = "https://repo1.maven.org/maven2"

# Alluxio's own version string -- "313" is a release, not a typo (2024-06). The shaded client
# relocates Alluxio's dependencies (grpc, guava, netty) and links plain org.apache.hadoop, which
# is what lets it share Spark's Hadoop.
ALLUXIO_JAR = "org/alluxio/alluxio-shaded-client/313/alluxio-shaded-client-313.jar"
# Compile-time only; at runtime the base engine's spark.jars.packages supplies the same jar.
HADOOP_AZURE_JAR = "org/apache/hadoop/hadoop-azure/3.4.2/hadoop-azure-3.4.2.jar"

SHIM_SRC = Path(__file__).resolve().parents[3] / "java" / "bench"

# Half the runner's free disk at most: TPC-DS SF=10 is ~3 GB of parquet and TPC-H ~2.6 GB, so the
# whole working set fits and nothing is evicted between the passes -- same as DuckDB's cache.
CACHE_SIZE = "8GB"


class PysparkAlluxioIceberg(PysparkIceberg):
    name = "pyspark_alluxio_iceberg"

    def _fetch(self, relpath: str, into: Path) -> Path:
        target = into / Path(relpath).name
        if not target.exists():
            urllib.request.urlretrieve(f"{MAVEN}/{relpath}", target)
        return target

    def _build_shim(self, work: Path) -> tuple[Path, Path]:
        """javac + jar the shim. Seconds; runs once per job."""
        work.mkdir(parents=True, exist_ok=True)
        alluxio = self._fetch(ALLUXIO_JAR, work)
        azure = self._fetch(HADOOP_AZURE_JAR, work)
        spark_jars = Path(pyspark.__file__).parent / "jars"
        classes = work / "classes"
        classes.mkdir(exist_ok=True)
        classpath = os.pathsep.join([str(spark_jars / "*"), str(azure), str(alluxio)])
        sources = [str(p) for p in sorted(SHIM_SRC.glob("*.java"))]
        subprocess.run(
            ["javac", "--release", "17", "-cp", classpath, "-d", str(classes), *sources],
            check=True,
        )
        shim = work / "bench-caching-abfs.jar"
        subprocess.run(["jar", "cf", str(shim), "-C", str(classes), "."], check=True)
        return shim, alluxio

    def _extra_config(self) -> dict[str, str]:
        scratch = Path(os.environ.get("RUNNER_TEMP", "/tmp"))
        shim, alluxio = self._build_shim(scratch / "alluxio-shim")
        self._cache_dir = scratch / "alluxio-cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        conf = {
            "spark.jars": f"{shim},{alluxio}",
            "spark.hadoop.fs.abfss.impl": "bench.CachingAbfss",
            "spark.hadoop.fs.abfs.impl": "bench.CachingAbfs",
            "spark.hadoop.alluxio.user.client.cache.enabled": "true",
            "spark.hadoop.alluxio.user.client.cache.dirs": str(self._cache_dir),
            "spark.hadoop.alluxio.user.client.cache.size": CACHE_SIZE,
        }
        scrub.safe_print(f"  alluxio local cache {CACHE_SIZE} at {self._cache_dir}")
        return conf

    def close(self) -> None:
        """Report what the cache holds before tearing down: the proof it engaged at all."""
        cache = getattr(self, "_cache_dir", None)
        if cache is not None and cache.exists():
            files = [p for p in cache.rglob("*") if p.is_file()]
            size = sum(p.stat().st_size for p in files) / 2**20
            scrub.safe_print(f"  alluxio cache: {len(files)} pages, {size:,.1f} MiB")
        super().close()
