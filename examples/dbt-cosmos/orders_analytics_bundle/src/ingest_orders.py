# Databricks notebook source
# Migrated from PythonOperator ingest_orders in DAG orders_analytics.

df = spark.read.json("s3://acme-orders/raw/")
df.write.mode("append").saveAsTable("main.analytics.raw_orders")
