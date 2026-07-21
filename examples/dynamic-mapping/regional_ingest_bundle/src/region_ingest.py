# Databricks notebook source
# Child-job task. `region` arrives as a job parameter set by the parent's for_each
# iteration (job_parameters.region = "{{input}}").
dbutils.widgets.text("region", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "commerce")

region = dbutils.widgets.get("region")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver_table = f"{catalog}.{schema}.silver_orders_{region}"
print(f"ingest {region} -> {silver_table}")

# Pass the TaskFlow return value to downstream tasks in this child job run.
dbutils.jobs.taskValues.set(key="return_value", value=silver_table)
