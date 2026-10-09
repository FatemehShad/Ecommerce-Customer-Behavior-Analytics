"""Fail the batch if core curated-data contracts are violated."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_silver(events: DataFrame) -> dict[str, int]:
    invalid = (
        F.col("user_id").isNull()
        | (F.col("user_id") == "")
        | F.col("product_id").isNull()
        | (F.col("product_id") == "")
        | F.col("event_timestamp").isNull()
        | F.col("event_type").isNull()
        | ~F.col("event_type").isin("view", "cart", "purchase")
        | F.col("event_id").isNull()
    )
    row = events.agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("unique_events"),
        F.coalesce(F.sum(invalid.cast("long")), F.lit(0)).alias("invalid_rows"),
    ).first()
    result = {key: int(value) for key, value in row.asDict().items()}
    if result["rows"] == 0:
        raise ValueError("No valid events remain after validation")
    if result["invalid_rows"] or result["rows"] != result["unique_events"]:
        raise ValueError(f"Silver data quality failed: {result}")
    return result
