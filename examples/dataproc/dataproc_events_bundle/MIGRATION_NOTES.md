# Dataproc events migration notes

## Conversion summary

| Airflow task | Dataproc behavior | Lakeflow result |
|---|---|---|
| `create_cluster` | Creates a four-worker Dataproc cluster | Removed; compatible settings moved to `events_compute` |
| `submit_events` | Submits `pyspark_job` asynchronously | Replaced by `transform_events` `spark_python_task` |
| `wait_for_events` | Polls the submitted Dataproc job ID | Removed; native task completion is the dependency boundary |
| `delete_cluster` | Deletes the ephemeral cluster under `all_done` | Removed; Jobs compute terminates with the run |

## Required configuration

- Set `var.spark_version` to a supported Databricks Runtime after validating application compatibility. Dataproc image `2.2-debian12` was not translated automatically.
- Set `var.node_type_id` to a workspace-supported GCP node type. Dataproc `n2-standard-8` was not copied or guessed.
- Set `var.input_path` to the original GCS prefix or a governed Unity Catalog volume/external location. Confirm that the run identity can read it.
- Set `var.output_table` to a fully qualified Unity Catalog table and grant the run identity permission to create or modify it.

## Compute and configuration changes

- Preserved four workers because the source has a static primary worker count and no secondary/preemptible workers.
- Preserved `spark.sql.adaptive.enabled=true` and job-level `spark.sql.shuffle.partitions=200` as Spark configuration.
- Removed `yarn.nodemanager.resource.memory-mb`; YARN does not manage Databricks compute. Validate memory behavior with the selected node type and runtime.
- The Airflow GCP connection and Dataproc project/region are not copied. Native Lakeflow cancellation replaces Dataproc cancellation-on-kill behavior.

## Source and identity

- The source example assumes `transform_events.py` was retrieved from `gs://customer-dataproc-artifacts/jobs/transform_events.py` and committed into the bundle.
- No Airflow connection credentials or service-account keys are included. Configure the Databricks run identity and Unity Catalog storage access separately.

## Retry and lifecycle differences

| Airflow retry boundary | Lakeflow retry boundary | Repeated-side-effect risk |
|---|---|---|
| `submit_events` retries separately from `wait_for_events` | `transform_events` reruns the full PySpark workload | A retry can repeat writes completed before failure |

- Airflow attempted cluster deletion under `all_done`. Jobs compute lifecycle is owned by the run; there is no explicit teardown task, and teardown cannot independently change the final job status.
- `catchup=True` maps to native backfill. Leave `run_date` empty and override `trigger_date` with `{{backfill.iso_date}}` so replayed runs apply the same previous-day offset as scheduled runs.

## Logical date semantics

This example deliberately targets Airflow 2 by using `schedule_interval`. For the 02:00 run on January 2, Airflow 2 renders `{{ ds }}` as January 1, the start date of the previous data interval. Lakeflow's `trigger_date` is January 2, so the script derives the previous calendar day when `run_date` is empty. An Airflow 3 raw-cron DAG uses fire-time semantics and must be evaluated separately instead of inheriting this offset.

| Run mode | Parameter values | Processed date |
|---|---|---|
| Scheduled | `run_date` remains empty; `trigger_date={{job.trigger.time.iso_date}}` | Previous calendar day |
| Native backfill | `run_date` remains empty; `trigger_date={{backfill.iso_date}}` | Previous calendar day |
| Explicit partition replay | `run_date=<date>`; `trigger_date` remains a valid ISO date | Exact `run_date` value |
