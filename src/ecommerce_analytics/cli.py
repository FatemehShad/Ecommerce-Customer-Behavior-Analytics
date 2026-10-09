"""Command line entry point for generation and full-refresh analytics."""

import argparse
import json

from ecommerce_analytics.demo import generate_events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("generate", help="Generate labeled synthetic input")
    generate.add_argument("--output", required=True)
    generate.add_argument("--rows", type=int, default=10000)
    generate.add_argument("--seed", type=int, default=42)
    run = commands.add_parser("run", help="Run a full-refresh Delta Lake pipeline")
    run.add_argument("--input", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--session-minutes", type=int, default=30)
    run.add_argument("--master", default="local[2]")
    run.add_argument("--shuffle-partitions", type=int, default=8)
    args = parser.parse_args()
    if args.command == "generate":
        generate_events(args.output, args.rows, args.seed)
        print(json.dumps({"synthetic_rows": args.rows, "output": args.output}))
        return
    from ecommerce_analytics.pipeline import run_pipeline
    from ecommerce_analytics.spark import local_spark

    spark = local_spark(args.master, args.shuffle_partitions)
    try:
        summary = run_pipeline(spark, args.input, args.output, args.session_minutes)
        print(json.dumps(summary, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
