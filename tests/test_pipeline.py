import csv

from ecommerce_analytics.pipeline import run_pipeline


def test_delta_pipeline_and_repeatable_full_refresh(spark, raw_events, tmp_path):
    source = tmp_path / "events.csv"
    with source.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(raw_events.columns)
        writer.writerows(tuple(row) for row in raw_events.collect())
    output = str(tmp_path / "lakehouse")
    expected = {
        "raw_rows": 12,
        "valid_rows": 7,
        "rejected_rows": 4,
        "duplicate_rows": 1,
        "sessions": 3,
    }
    assert run_pipeline(spark, str(source), output) == expected
    assert run_pipeline(spark, str(source), output) == expected
    for table in [
        "bronze/events",
        "silver/events",
        "silver/quarantine",
        "gold/session_events",
        "gold/sessions",
        "gold/session_products",
        "gold/daily_funnel",
        "gold/product_metrics",
    ]:
        assert (tmp_path / "lakehouse" / table / "_delta_log").is_dir()
    funnel = spark.read.format("delta").load(f"{output}/gold/daily_funnel").first()
    assert funnel.purchased_sessions == 1
    sessions = spark.read.format("delta").load(f"{output}/gold/sessions")
    assert sum(row.event_count for row in sessions.collect()) == 7
    products = spark.read.format("delta").load(f"{output}/gold/product_metrics")
    metrics = {row.product_id: row for row in products.collect()}
    assert metrics["p1"].converted_sessions == 1
    assert metrics["p1"].purchase_events == 2
