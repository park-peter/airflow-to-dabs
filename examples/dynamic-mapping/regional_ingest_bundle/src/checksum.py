# Databricks notebook source
# Body of the for_each nested task. `table` is the current element ({{input}});
# `catalog` is the constant from .partial(catalog="main").
dbutils.widgets.text("table", "")
dbutils.widgets.text("catalog", "main")

table = dbutils.widgets.get("table")
catalog = dbutils.widgets.get("catalog")

print(f"checksum {catalog}.{table}")
