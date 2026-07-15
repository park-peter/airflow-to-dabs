# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

run_date = dbutils.widgets.get("run_date")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_table = f"{catalog}.{schema}.silver_orders"
gold_table = f"{catalog}.{schema}.gold_daily_order_summary"

summary = (
    spark.table(silver_table)
    .where(F.col("order_date") == F.to_date(F.lit(run_date)))
    .groupBy("order_date")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").cast("decimal(18,2)").alias("gross_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("updated_at", F.current_timestamp())
)

if spark.catalog.tableExists(gold_table):
    spark.sql(f"DELETE FROM {gold_table} WHERE order_date = DATE '{run_date}'")
    summary.write.mode("append").saveAsTable(gold_table)
else:
    summary.write.mode("overwrite").partitionBy("order_date").saveAsTable(gold_table)
