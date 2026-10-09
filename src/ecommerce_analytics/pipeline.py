"""Full-refresh bronze/silver/gold Delta Lake pipeline."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_analytics.quality import validate_silver
from ecommerce_analytics.transform import (
    normalize_events,
    product_journeys,
    session_summary,
    sessionize,
    split_valid_events,
)

DAILY_FUNNEL_SQL = """
WITH per_session AS (
  SELECT session_id, CAST(session_start AS DATE) AS session_date,
         MAX(CASE WHEN first_view IS NOT NULL THEN 1 ELSE 0 END) AS viewed,
         MAX(CASE WHEN first_cart_after_view IS NOT NULL THEN 1 ELSE 0 END) AS carted,
         MAX(CASE WHEN first_purchase_after_cart IS NOT NULL THEN 1 ELSE 0 END) AS purchased
  FROM session_products
  GROUP BY session_id, CAST(session_start AS DATE)
)
SELECT session_date, COUNT(*) AS total_sessions,
       SUM(viewed) AS viewed_sessions, SUM(carted) AS carted_sessions,
       SUM(purchased) AS purchased_sessions,
       CASE WHEN SUM(viewed) > 0 THEN SUM(carted) * 1.0 / SUM(viewed) ELSE NULL END
         AS view_to_cart_rate,
       CASE WHEN SUM(carted) > 0 THEN SUM(purchased) * 1.0 / SUM(carted) ELSE NULL END
         AS cart_to_purchase_rate,
       CASE WHEN SUM(viewed) > 0 THEN SUM(purchased) * 1.0 / SUM(viewed) ELSE NULL END
         AS view_to_purchase_rate
FROM per_session GROUP BY session_date
"""


def run_pipeline(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    session_minutes: int = 30,
) -> dict[str, int]:
    """Replace tables using the complete input corpus; never append partial batches."""
    if session_minutes <= 0:
        raise ValueError("session_minutes must be positive")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    root = output_path.rstrip("/")

    def write(frame, name, partition=None):
        writer = frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition:
            writer = writer.partitionBy(partition)
        writer.save(f"{root}/{name}")

    def read(name):
        return spark.read.format("delta").load(f"{root}/{name}")

    raw = spark.read.option("header", "true").option("mode", "FAILFAST").csv(input_path)
    normalized = normalize_events(raw)
    valid, rejected = split_valid_events(normalized)
    # Validate before any output is replaced, including empty or wholly invalid inputs.
    counts = validate_silver(valid)
    raw_count = raw.count()
    rejected_count = rejected.count()
    write(raw, "bronze/events")
    write(rejected, "silver/quarantine")
    write(valid, "silver/events", "event_date")
    session_events = sessionize(read("silver/events"), session_minutes)
    write(session_events, "gold/session_events", "event_date")
    persisted_events = read("gold/session_events")
    write(session_summary(persisted_events), "gold/sessions")
    write(product_journeys(persisted_events), "gold/session_products")
    journeys = read("gold/session_products")
    journeys.createOrReplaceTempView("session_products")
    write(spark.sql(DAILY_FUNNEL_SQL), "gold/daily_funnel")
    product_metrics = journeys.groupBy("product_id").agg(
        F.count("*").alias("interacting_sessions"),
        F.count("first_view").alias("viewed_sessions"),
        F.count("first_cart_after_view").alias("carted_sessions"),
        F.count("first_purchase_after_cart").alias("converted_sessions"),
        F.sum("purchase_events").alias("purchase_events"),
    )
    write(product_metrics, "gold/product_metrics")
    return {
        "raw_rows": raw_count,
        "valid_rows": counts["rows"],
        "rejected_rows": rejected_count,
        "duplicate_rows": raw_count - rejected_count - counts["rows"],
        "sessions": read("gold/sessions").count(),
    }
