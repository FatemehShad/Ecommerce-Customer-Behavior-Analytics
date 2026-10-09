# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce customer behaviour analytics
# MAGIC Install the project wheel on a compatible Spark 4.0 / Delta 4.0 cluster with
# MAGIC `pip install --no-deps /Volumes/<catalog>/<schema>/<volume>/<wheel>.whl`.
# MAGIC The cluster provides PySpark and Delta: do not replace its runtime packages.
# MAGIC Upload a CSV to a Unity Catalog Volume and choose a distinct writable output directory.

# COMMAND ----------
dbutils.widgets.text("input_path", "/Volumes/main/analytics/events/events.csv")  # noqa: F821
dbutils.widgets.text("output_path", "/Volumes/main/analytics/events/lakehouse")  # noqa: F821
dbutils.widgets.text("session_minutes", "30")  # noqa: F821

# COMMAND ----------
from ecommerce_analytics.pipeline import run_pipeline  # noqa: E402

summary = run_pipeline(
    spark,  # noqa: F821
    dbutils.widgets.get("input_path"),  # noqa: F821
    dbutils.widgets.get("output_path"),  # noqa: F821
    int(dbutils.widgets.get("session_minutes")),  # noqa: F821
)
print(summary)

# COMMAND ----------
root = dbutils.widgets.get("output_path").rstrip("/")  # noqa: F821
for name in ["sessions", "session_products", "product_metrics", "daily_funnel"]:
    spark.read.format("delta").load(f"{root}/gold/{name}").createOrReplaceTempView(name)  # noqa: F821
display(spark.sql("SELECT * FROM daily_funnel ORDER BY session_date"))  # noqa: F821
