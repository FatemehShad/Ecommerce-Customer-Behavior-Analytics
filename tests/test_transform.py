from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecommerce_analytics.pipeline import DAILY_FUNNEL_SQL
from ecommerce_analytics.quality import validate_silver
from ecommerce_analytics.transform import (
    normalize_events,
    product_journeys,
    sessionize,
    split_valid_events,
)


def test_validation_quarantine_and_deduplication(raw_events):
    valid, rejected = split_valid_events(normalize_events(raw_events))
    assert validate_silver(valid) == {"rows": 7, "unique_events": 7, "invalid_rows": 0}
    assert {r.rejection_reason for r in rejected.collect()} == {
        "invalid_timestamp",
        "missing_user_id",
        "unsupported_event_type",
        "missing_product_id",
    }
    assert {r.event_type for r in valid.select("event_type").distinct().collect()} == {
        "view",
        "cart",
        "purchase",
    }


def test_epoch_milliseconds_and_optional_transaction(spark):
    raw = spark.createDataFrame(
        [("1735689600000", "001", " VIEW ", "002")],
        "timestamp string, visitorid string, event string, itemid string",
    )
    valid, rejected = split_valid_events(normalize_events(raw))
    row = valid.first()
    assert row.event_timestamp.isoformat() == "2025-01-01T00:00:00"
    assert row.user_id == "001" and row.product_id == "002"
    assert row.transaction_id is None
    assert rejected.count() == 0


def test_session_boundary_and_stable_ids(spark):
    raw = spark.createDataFrame(
        [
            (time, "u", "view", "p")
            for time in ["2025-01-01T00:00:00Z", "2025-01-01T00:30:00Z", "2025-01-01T01:00:01Z"]
        ],
        "timestamp string, visitorid string, event string, itemid string",
    )
    valid, _ = split_valid_events(normalize_events(raw))
    rows = sessionize(valid).orderBy("event_timestamp").collect()
    assert rows[0].session_id == rows[1].session_id
    assert rows[1].session_id != rows[2].session_id
    rebuilt = sessionize(valid.repartition(2)).orderBy("event_timestamp").collect()
    assert [r.session_id for r in rebuilt] == [r.session_id for r in rows]
    with pytest.raises(ValueError, match="positive"):
        sessionize(valid, 0)


def test_ordered_funnel_and_daily_rates(spark, raw_events):
    valid, _ = split_valid_events(normalize_events(raw_events))
    journeys = product_journeys(sessionize(valid))
    by_user_product = {(r.user_id, r.product_id): r for r in journeys.collect()}
    assert by_user_product["u1", "p1"].first_purchase_after_cart is not None
    assert by_user_product["u2", "p1"].first_purchase_after_cart is None
    assert by_user_product["u2", "p1"].purchase_events == 1
    journeys.createOrReplaceTempView("session_products")
    row = spark.sql(DAILY_FUNNEL_SQL).first()
    assert (
        row.total_sessions,
        row.viewed_sessions,
        row.carted_sessions,
        row.purchased_sessions,
    ) == (3, 3, 2, 1)
    assert row.cart_to_purchase_rate == Decimal("0.5")
    assert float(row.view_to_purchase_rate) == pytest.approx(1 / 3)


def test_cross_product_events_do_not_complete_funnel(spark):
    raw = spark.createDataFrame(
        [
            ("2025-01-01T00:00:00Z", "u", "view", "a"),
            ("2025-01-01T00:01:00Z", "u", "cart", "b"),
            ("2025-01-01T00:02:00Z", "u", "purchase", "b"),
        ],
        "timestamp string, visitorid string, event string, itemid string",
    )
    valid, _ = split_valid_events(normalize_events(raw))
    journeys = product_journeys(sessionize(valid))
    assert journeys.filter(F.col("first_purchase_after_cart").isNotNull()).count() == 0


def test_missing_schema_and_quality_failures(spark, raw_events):
    with pytest.raises(ValueError, match="missing required"):
        normalize_events(raw_events.drop("itemid"))
    valid, _ = split_valid_events(normalize_events(raw_events))
    with pytest.raises(ValueError, match="No valid events"):
        validate_silver(valid.limit(0))
    with pytest.raises(ValueError, match="quality failed"):
        validate_silver(valid.unionByName(valid))
    with pytest.raises(ValueError, match="quality failed"):
        validate_silver(valid.withColumn("event_type", F.lit("unknown")))
