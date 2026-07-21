# Databricks notebook source
dbutils.widgets.text("silver_table", "")

silver_table = dbutils.widgets.get("silver_table")
print(f"publish gold from {silver_table}")
