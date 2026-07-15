# Databricks notebook source
# Migrated from PythonOperator publish_metrics in DAG orders_analytics.

daily = spark.read.table("main.analytics.fct_daily_orders")
daily.write.mode("overwrite").saveAsTable("main.analytics.orders_dashboard_feed")
