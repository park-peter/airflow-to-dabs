# Migration Notes

## Summary

Converted Airflow DAG: `customer_orders_airflow`

Generated Lakeflow Job resource: `customer_orders_lakeflow_job`

This bundle uses current Databricks bundle patterns for Lakeflow Jobs:

- Job resources are declared under `resources.jobs`.
- Notebook task source files are deployed with the bundle and referenced by relative path.
- The file sensor becomes a job-level `trigger.file_arrival`.
- The SQL check becomes a warehouse-backed `sql_task`.
- The simple Airflow branch becomes a native `condition_task`.

## Required Configuration

Set these values before deployment:

| Setting | Location | Required Action |
|---|---|---|
| SQL warehouse | `variables.warehouse_id` | Replace `<WAREHOUSE_ID>` or pass `--var warehouse_id=<WAREHOUSE_ID>`. |
| Landing path | `variables.landing_path` | Point to a Unity Catalog volume or external location path for arriving order files. |
| Checkpoint path | `variables.checkpoint_path` | Point to a writable Unity Catalog volume path outside the landing path. |
| Production identity | `targets.prod.run_as.service_principal_name` | Replace `<SERVICE_PRINCIPAL_NAME>` with the production service principal. |

## Airflow Behavior Changes

| Airflow Setting or Pattern | DABs/Lakeflow Outcome |
|---|---|
| `S3KeySensor(bucket_key="orders/{{ ds }}/*.json")` | Converted to `trigger.file_arrival` on `${var.landing_path}`. File arrival triggers cannot use wildcard paths, so the trigger monitors the parent UC volume/external location path. |
| File discovery scope | The trigger and Auto Loader both scan `${var.landing_path}` recursively. Auto Loader applies `pathGlobFilter="*.json"` so the broader prefix trigger does not broaden ingestion beyond the original sensor's JSON contract. |
| Files present before deployment | File arrival triggers start runs only for new arrivals. Perform an initial manual run after deployment; Auto Loader uses `cloudFiles.includeExistingFiles=true` with the durable checkpoint to ingest existing files once. |
| `schedule="0 6 * * *"` plus `S3KeySensor` | Replaced by event-driven file arrival. If a fixed 06:00 UTC schedule is required, use a schedule instead of the trigger and move file readiness into task code. |
| `{{ ds }}` in `op_kwargs["run_date"]` | Mapped to the `run_date` job parameter (tasks use it as a logical/partition date, `WHERE order_date = :run_date`). Because this job is **file-arrival triggered**, there is no scheduled logical date, so `{{job.trigger.time}}` is not reliable; `run_date` defaults to `{{job.start_time.iso_date}}` as an approximation for the latest-arrival case. For strict correctness, derive the date from the arriving file path (`orders/<date>/…`); for exact historical windows, use backfill (below). If this were a **cron-scheduled** job instead, the default would be `{{job.trigger.time.iso_date}}` (the scheduled date). |
| `catchup=False` | No backfill emitted. To replay history, use a native [Databricks backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs), overriding `run_date` with `{{backfill.iso_date}}` per replayed window (`run_date` is a job parameter for exactly this reason). |
| `depends_on_past=False` | No equivalent emitted because the Airflow DAG explicitly disables this behavior. |
| `email_on_retry=False` | No equivalent emitted. Job failure notification is preserved. |
| `trigger_rule="none_failed_min_one_success"` | `run_if: NONE_FAILED` on `publish_gold` — an approximation. Lakeflow has no combined predicate, so the "at least one succeeded" clause is dropped: `publish_gold` **also runs if every upstream skipped**. Confirm that over-run is acceptable, or gate with a `condition_task`. |
| `EmptyOperator(skip_full_validation)` | Removed and rewired through the `choose_validation` condition false outcome. |

## Databricks Prerequisites

- Unity Catalog must be enabled for the file arrival trigger path.
- `${var.landing_path}` must be a Unity Catalog volume path or a subpath of a Unity Catalog external location.
- Enable managed file events on the external location for better file arrival trigger and Auto Loader performance.
- Keep job queueing enabled so an arrival detected at the concurrency limit waits for execution instead of being skipped.
- Run the job once manually after deployment to bootstrap files already present in `${var.landing_path}`.
- The job's run identity needs read access to `${var.landing_path}` and write access to `${var.checkpoint_path}` and the target schema.
- The configured SQL warehouse must have access to `${var.catalog}.${var.schema}`.

## Validation

After replacing placeholders, run:

```bash
databricks bundle validate -t dev --var warehouse_id=<WAREHOUSE_ID>
databricks bundle deploy -t dev --var warehouse_id=<WAREHOUSE_ID>
```

If Databricks authentication is not configured locally, YAML and structural checks can still be run offline with `yq` or another YAML parser, but full bundle validation requires a Databricks workspace profile or environment variables.
