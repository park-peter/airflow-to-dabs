# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

run_date = dbutils.widgets.get("run_date")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_orders_raw"
silver_table = f"{catalog}.{schema}.silver_orders"

orders = (
    spark.table(bronze_table)
    .where(F.col("_ingest_run_date") == F.to_date(F.lit(run_date)))
    .select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.to_timestamp("order_timestamp").alias("order_timestamp"),
        F.to_date("order_timestamp").alias("order_date"),
        F.col("status").cast("string").alias("status"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.col("_source_file"),
        F.col("_ingest_timestamp"),
    )
    .where(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

if spark.catalog.tableExists(silver_table):
    spark.sql(f"DELETE FROM {silver_table} WHERE order_date = DATE '{run_date}'")
    orders.write.mode("append").saveAsTable(silver_table)
else:
    orders.write.mode("overwrite").partitionBy("order_date").saveAsTable(silver_table)
