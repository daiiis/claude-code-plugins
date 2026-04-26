"""Load a JDBC driver JAR into a running AIDP Spark session at runtime.

Spark's normal mechanism for adding a JDBC driver is to set ``spark.jars`` at
session creation time, which means stopping the kernel and re-bootstrapping the
notebook context. That's awkward in AIDP because the notebook itself owns the
SparkSession lifecycle.

This helper does the next-best thing: it builds a Java URLClassLoader around
the driver JAR, registers the driver instance with ``java.sql.DriverManager``,
and sets the JVM's thread-context class loader so Spark's
``Utils.classForName`` resolves the driver class. After calling this, you can
do ``spark.read.format("jdbc").option("driver", "<class>").load()`` exactly as
documented in the SQL Spark guide — no cluster restart, no library tab.

Live-validated on the AIDP `tpcds` cluster (Spark 3.5.0) with
``org.sqlite.JDBC`` from Maven Central. Maven Central is reachable from AIDP
clusters; PyPI is not, but the JAR coordinates work fine.
"""

from __future__ import annotations

from typing import Optional


def add_jdbc_jar_at_runtime(
    spark,
    *,
    jar_path: str,
    driver_class: str,
) -> None:
    """Make a JDBC driver class loadable in the current Spark session.

    Args:
        spark: The active ``SparkSession`` (the variable AIDP notebooks expose
            as ``spark``).
        jar_path: Filesystem path to the JDBC driver JAR. Must be visible to
            the JVM driver process — usually under ``/tmp/`` after a fresh
            download or under ``/Volumes/...``.
        driver_class: The JDBC driver class name, e.g. ``org.sqlite.JDBC`` or
            ``com.clickhouse.jdbc.ClickHouseDriver``.

    Raises:
        Exception: Propagates any Java-side exception (FileNotFound,
            ClassNotFound, etc.) without wrapping. Inspect the message — it's
            usually a misspelled class name or a JAR file that's missing from
            the JVM's view (a path under ``/Workspace/`` is unreliable; use
            ``/tmp/`` or ``/Volumes/...``).

    Example:
        >>> import urllib.request, os
        >>> jar = "/tmp/sqlite-jdbc-3.46.0.0.jar"
        >>> if not os.path.exists(jar):
        ...     urllib.request.urlretrieve(
        ...         "https://repo1.maven.org/maven2/org/xerial/"
        ...         "sqlite-jdbc/3.46.0.0/sqlite-jdbc-3.46.0.0.jar", jar
        ...     )
        >>> add_jdbc_jar_at_runtime(spark, jar_path=jar,
        ...                         driver_class="org.sqlite.JDBC")
        >>> df = (spark.read.format("jdbc")
        ...         .option("url", "jdbc:sqlite::memory:")
        ...         .option("driver", "org.sqlite.JDBC")
        ...         .option("dbtable", "(SELECT 1 AS c1)").load())
    """
    jvm = spark._jvm
    gw = spark.sparkContext._gateway

    file_url = jvm.java.io.File(jar_path).toURI().toURL()
    arr = gw.new_array(jvm.java.net.URL, 1)
    arr[0] = file_url

    parent = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    loader = jvm.java.net.URLClassLoader(arr, parent)

    cls = loader.loadClass(driver_class)
    driver = cls.newInstance()
    jvm.java.sql.DriverManager.registerDriver(driver)

    jvm.java.lang.Thread.currentThread().setContextClassLoader(loader)


def download_jdbc_jar(
    *,
    maven_url: str,
    target_path: str,
    overwrite: bool = False,
) -> str:
    """Convenience wrapper around urllib to fetch a driver JAR.

    Args:
        maven_url: Full Maven Central URL to the JAR.
        target_path: Where to write it — must be JVM-readable (``/tmp/...`` is
            recommended).
        overwrite: If False (default) and the target already exists, skip the
            download.

    Returns:
        ``target_path`` for chaining into ``add_jdbc_jar_at_runtime``.
    """
    import os
    import urllib.request

    if overwrite or not os.path.exists(target_path):
        urllib.request.urlretrieve(maven_url, target_path)
    return target_path
