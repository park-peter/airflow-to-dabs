# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")
dbutils.widgets.text("landing_path", "/Volumes/main/landing/orders/")
dbutils.widgets.text("checkpoint_path", "/Volumes/main/checkpoints/customer_orders/bronze_ingest")

run_date = dbutils.widgets.get("run_date")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_path = dbutils.widgets.get("landing_path").rstrip("/") + "/"
checkpoint_path = dbutils.widgets.get("checkpoint_path").rstrip("/")

bronze_table = f"{catalog}.{schema}.bronze_orders_raw"
schema_location = f"{checkpoint_path}/schema"
checkpoint_location = f"{checkpoint_path}/stream"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.useManagedFileEvents", "true")
    .load(landing_path)
    .withColumn("_ingest_run_date", F.to_date(F.lit(run_date)))
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream.option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(bronze_table)
)

query.awaitTermination()
