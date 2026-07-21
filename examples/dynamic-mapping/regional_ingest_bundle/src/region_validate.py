# Databricks notebook source
dbutils.widgets.text("silver_table", "")

silver_table = dbutils.widgets.get("silver_table")
print(f"validate {silver_table}")

dbutils.jobs.taskValues.set(key="return_value", value=silver_table)
