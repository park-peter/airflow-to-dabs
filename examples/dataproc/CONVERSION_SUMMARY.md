# Dataproc conversion

This example converts an Airflow DAG that creates an ephemeral Dataproc cluster, submits a PySpark job asynchronously, waits with `DataprocJobSensor`, and deletes the cluster.

The generated bundle contains one `spark_python_task`. Cluster creation/deletion and the wait sensor are absorbed into Lakeflow Jobs compute and native task completion. The example preserves compatible Spark properties, removes a YARN-only setting, leaves runtime and GCP node type as required variables, and documents GCS/identity and retry-boundary changes.

The source uses Airflow 2 `schedule_interval` semantics. The generated script derives the previous calendar day from the Lakeflow trigger date unless `run_date` is explicitly supplied, preserving the source `{{ ds }}` partition window. Native backfill leaves `run_date` empty and overrides `trigger_date` with `{{backfill.iso_date}}`, so it follows the same offset path as a scheduled run.

See `references/dataproc-migration.md` for the complete payload matrix and fail-closed rules.
