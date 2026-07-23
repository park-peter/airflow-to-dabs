# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw_table = f"{catalog}.{schema}.raw_orders"      # written by the ingestion pipeline
gold_table = f"{catalog}.{schema}.gold_orders"

# catalog/schema are overridable job parameters, so pass the fully-qualified table
# names as arguments and bind them with IDENTIFIER(:name) rather than interpolating
# identifiers into the SQL string.
spark.sql(
    """
    CREATE OR REPLACE TABLE IDENTIFIER(:gold_table) AS
    SELECT order_date, count(*) AS order_count, sum(amount) AS total_amount
    FROM IDENTIFIER(:raw_table)
    GROUP BY order_date
    """,
    args={"gold_table": gold_table, "raw_table": raw_table},
)
