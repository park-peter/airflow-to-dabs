# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw_table = f"{catalog}.{schema}.raw_orders"       # written by the ingestion pipeline
gold_table = f"{catalog}.{schema}.gold_orders"

spark.sql(f"""
    CREATE OR REPLACE TABLE {gold_table} AS
    SELECT order_date, count(*) AS order_count, sum(amount) AS total_amount
    FROM {raw_table}
    GROUP BY order_date
""")
