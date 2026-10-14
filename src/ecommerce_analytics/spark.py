"""Local Spark setup; Databricks supplies its own Spark and Delta runtime."""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_spark(
    master: str = "local[2]", shuffle_partitions: int = 8, show_progress: bool = True
) -> SparkSession:
    cache = Path(os.environ.get("SPARK_IVY_DIR", "/tmp/ecommerce-spark-ivy"))
    builder = (
        SparkSession.builder.appName("ecommerce-customer-analytics")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.jars.ivy", str(cache))
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", str(show_progress).lower())
    )
    jars = os.environ.get("SPARK_DELTA_JARS")
    if jars:
        # Proxy-restricted machines can prefetch verified jars instead of JVM/Ivy downloads.
        for jar in jars.split(","):
            if not Path(jar).is_file():
                raise FileNotFoundError(f"Delta jar does not exist: {jar}")
        builder = builder.config("spark.jars", jars)
    else:
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
