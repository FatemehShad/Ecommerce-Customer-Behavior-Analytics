"""Check curated Delta outputs and execute the repository's SQL analysis examples."""

import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from ecommerce_analytics.quality import validate_silver
from ecommerce_analytics.spark import local_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expected-valid-rows", type=int)
    args = parser.parse_args()
    spark = local_spark("local[4]", 16)
    try:
        root = args.output.rstrip("/")
        silver = spark.read.format("delta").load(f"{root}/silver/events")
        quality = validate_silver(silver)
        if args.expected_valid_rows is not None and quality["rows"] != args.expected_valid_rows:
            raise ValueError(f"Unexpected event count: {quality['rows']}")
        for name in ["sessions", "session_products", "product_metrics", "daily_funnel"]:
            spark.read.format("delta").load(f"{root}/gold/{name}").createOrReplaceTempView(name)
        funnel = spark.table("daily_funnel")
        invalid = funnel.filter(
            "purchased_sessions > carted_sessions OR carted_sessions > viewed_sessions "
            "OR viewed_sessions > total_sessions"
        ).count()
        if invalid:
            raise ValueError("Daily funnel stages violate monotonicity")
        columns = ["total_sessions", "viewed_sessions", "carted_sessions", "purchased_sessions"]
        totals = funnel.agg(*[F.sum(name).alias(name) for name in columns]).first().asDict()
        if totals["total_sessions"] != spark.table("sessions").count():
            raise ValueError("Daily funnel and session totals differ")
        if spark.table("sessions").agg(F.sum("event_count")).first()[0] != quality["rows"]:
            raise ValueError("Session event totals differ from curated event count")
        sql_file = Path(__file__).resolve().parents[1] / "sql" / "customer_behaviour.sql"
        # The examples use line comments and plain SELECT statements, not procedural SQL.
        sql = "\n".join(
            line for line in sql_file.read_text().splitlines() if not line.lstrip().startswith("--")
        )
        queries = [query.strip() for query in sql.split(";") if query.strip()]
        samples = [
            [row.asDict() for row in spark.sql(query).limit(5).collect()] for query in queries
        ]
        print(
            json.dumps(
                {
                    "silver_quality": quality,
                    "daily_funnel_totals": totals,
                    "sql_queries_executed": len(queries),
                    "query_samples": samples,
                },
                default=str,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
