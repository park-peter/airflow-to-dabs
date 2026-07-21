# Databricks notebook source
dbutils.widgets.text("batch_size", "0")

batch_size = int(dbutils.widgets.get("batch_size"))
message = f"starting run over {batch_size} regions"
print(message)

# Single-value @task return -> the conventional return_value task value.
dbutils.jobs.taskValues.set(key="return_value", value=message)
