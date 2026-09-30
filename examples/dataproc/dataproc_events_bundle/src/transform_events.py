import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-table", required=True)
    parser.add_argument("--run-date", default="")
    parser.add_argument("--trigger-date", required=True)
    return parser.parse_args()


def resolve_run_date(run_date: str, trigger_date: str) -> str:
    selected_date = (
        date.fromisoformat(run_date)
        if run_date
        else date.fromisoformat(trigger_date) - timedelta(days=1)
    )
    return selected_date.isoformat()


def main() -> None:
    args = parse_args()
    run_date = resolve_run_date(args.run_date, args.trigger_date)
    spark = SparkSession.builder.getOrCreate()

    events = spark.read.json(args.input)
    daily_events = (
        events.filter(F.to_date("event_timestamp") == F.lit(run_date).cast("date"))
        .groupBy(F.to_date("event_timestamp").alias("event_date"), "event_type")
        .agg(F.count("*").alias("event_count"))
    )
    daily_events.write.mode("overwrite").saveAsTable(args.output_table)


if __name__ == "__main__":
    main()
