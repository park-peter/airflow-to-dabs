# Databricks notebook source
import json

dbutils.widgets.text("regions", "[]")

regions = json.loads(dbutils.widgets.get("regions"))
batch_size = len(regions)

# multiple_outputs=True on the @task: one task value per returned dict key, each
# separately referenceable downstream as {{tasks.plan_run.values.<key>}}.
dbutils.jobs.taskValues.set(key="regions", value=regions)
dbutils.jobs.taskValues.set(key="batch_size", value=batch_size)
