# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

run_date = dbutils.widgets.get("run_date")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_table = f"{catalog}.{schema}.silver_orders"

orders = spark.table(silver_table).where(F.col("order_date") == F.to_date(F.lit(run_date)))

checks = {
    "missing_customer_id": orders.where(F.col("customer_id").isNull()).count(),
    "negative_amount": orders.where(F.col("amount") < 0).count(),
    "duplicate_order_id": orders.groupBy("order_id").count().where(F.col("count") > 1).count(),
}

failed_checks = {name: count for name, count in checks.items() if count > 0}
if failed_checks:
    raise ValueError(f"Full validation failed for {run_date}: {failed_checks}")

dbutils.jobs.taskValues.set(key="validated_order_count", value=orders.count())
