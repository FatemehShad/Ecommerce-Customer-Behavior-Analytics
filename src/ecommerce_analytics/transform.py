"""Distributed, deterministic transformations; no driver-side event collection."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {"timestamp", "visitorid", "event", "itemid"}


def normalize_events(raw: DataFrame) -> DataFrame:
    """Parse Retailrocket IDs, epoch milliseconds or ISO timestamps, and event aliases."""
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {sorted(missing)}")
    timestamp = F.trim(F.col("timestamp").cast("string"))
    # try_cast/try_to_timestamp deliberately quarantine malformed input under Spark ANSI mode.
    millis = F.expr("try_cast(trim(cast(timestamp as string)) as bigint)")
    event_time = F.when(timestamp.rlike(r"^[0-9]{13}$"), F.timestamp_millis(millis)).otherwise(
        F.try_to_timestamp(timestamp)
    )
    event = F.lower(F.trim(F.col("event")))
    kind = (
        F.when(event == "view", "view")
        .when(event.isin("cart", "addtocart"), "cart")
        .when(event.isin("purchase", "transaction"), "purchase")
    )
    transaction = F.col("transactionid") if "transactionid" in raw.columns else F.lit(None)
    normalized = raw.select(
        "*",
        F.trim(F.col("visitorid").cast("string")).alias("user_id"),
        F.trim(F.col("itemid").cast("string")).alias("product_id"),
        event_time.alias("event_timestamp"),
        kind.alias("event_type"),
        F.when(F.length(F.trim(transaction)) > 0, F.trim(transaction)).alias("transaction_id"),
    )
    reasons = F.concat_ws(
        ";",
        F.when(F.col("user_id").isNull() | (F.col("user_id") == ""), "missing_user_id"),
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing_product_id"),
        F.when(F.col("event_timestamp").isNull(), "invalid_timestamp"),
        F.when(F.col("event_type").isNull(), "unsupported_event_type"),
    )
    return normalized.withColumn("rejection_reason", reasons)


def split_valid_events(normalized: DataFrame) -> tuple[DataFrame, DataFrame]:
    canonical = ["user_id", "product_id", "event_timestamp", "event_type", "transaction_id"]
    valid = normalized.filter(F.col("rejection_reason") == "").select(*canonical)
    # JSON preserves field boundaries and nulls, avoiding delimiter-based hash collisions.
    valid = valid.withColumn(
        "event_id", F.sha2(F.to_json(F.struct(*canonical), {"ignoreNullFields": "false"}), 256)
    ).dropDuplicates(["event_id"])
    return (
        valid.withColumn("event_date", F.to_date("event_timestamp")),
        normalized.filter(F.col("rejection_reason") != ""),
    )


def sessionize(events: DataFrame, session_minutes: int = 30) -> DataFrame:
    if session_minutes <= 0:
        raise ValueError("session_minutes must be positive")
    order = Window.partitionBy("user_id").orderBy("event_timestamp", "event_id")
    cumulative = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous = F.lag("event_timestamp").over(order)
    boundary = F.when(
        previous.isNull()
        | ((F.col("event_timestamp").cast("long") - previous.cast("long")) > session_minutes * 60),
        1,
    ).otherwise(0)
    numbered = events.withColumn("_new_session", boundary).withColumn(
        "_session_number", F.sum("_new_session").over(cumulative)
    )
    session = Window.partitionBy("user_id", "_session_number")
    return (
        numbered.withColumn("session_start", F.min("event_timestamp").over(session))
        .withColumn(
            "session_id",
            F.sha2(F.to_json(F.struct("user_id", "session_start")), 256),
        )
        .drop("_new_session", "_session_number")
    )


def product_journeys(events: DataFrame) -> DataFrame:
    """Find nondecreasing view -> cart -> purchase timestamps for each session/product."""
    keys = ["user_id", "session_id", "session_start", "product_id"]
    window = Window.partitionBy(*keys)
    viewed = events.withColumn(
        "first_view",
        F.min(F.when(F.col("event_type") == "view", F.col("event_timestamp"))).over(window),
    )
    carted = viewed.withColumn(
        "first_cart_after_view",
        F.min(
            F.when(
                (F.col("event_type") == "cart") & (F.col("event_timestamp") >= F.col("first_view")),
                F.col("event_timestamp"),
            )
        ).over(window),
    )
    return carted.groupBy(*keys).agg(
        F.min("first_view").alias("first_view"),
        F.min("first_cart_after_view").alias("first_cart_after_view"),
        F.min(
            F.when(
                (F.col("event_type") == "purchase")
                & (F.col("event_timestamp") >= F.col("first_cart_after_view")),
                F.col("event_timestamp"),
            )
        ).alias("first_purchase_after_cart"),
        F.count("*").alias("event_count"),
        F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchase_events"),
    )


def session_summary(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("user_id", "session_id", "session_start")
        .agg(
            F.max("event_timestamp").alias("session_end"),
            F.count("*").alias("event_count"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum((F.col("event_type") == "view").cast("long")).alias("views"),
            F.sum((F.col("event_type") == "cart").cast("long")).alias("carts"),
            F.sum((F.col("event_type") == "purchase").cast("long")).alias("purchases"),
        )
        .withColumn(
            "duration_seconds",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
    )
