# Customer Orders Airflow to DABs Demo

This folder is a runnable demonstration of the `airflow-to-dabs` skill converting an Airflow DAG into a Databricks bundle for Lakeflow Jobs.

For the technical architecture of the skill itself, including how agents load and interpret it, see [`TECHNICAL_DEEP_DIVE.md`](TECHNICAL_DEEP_DIVE.md).

## Source DAG

`airflow/customer_orders_dag.py`

The example DAG models a common batch pipeline:

1. Wait for daily order files in S3.
2. Ingest raw JSON files into a bronze Delta table.
3. Transform and deduplicate records into a silver table.
4. Run a SQL data quality check.
5. Branch into full validation or skip validation.
6. Publish a daily gold aggregate.

## Generated Bundle

`customer_orders_bundle/`

```text
customer_orders_bundle/
  databricks.yml
  resources/
    customer_orders_job.yml
  src/
    ingest_bronze.py
    transform_silver.py
    dq_order_totals.sql
    full_validation.py
    publish_gold.py
  MIGRATION_NOTES.md
```

## Operator Mapping

| Airflow Task ID | Airflow Operator | Lakeflow/DABs Output | Tier | Notes |
|---|---|---|---|---|
| `wait_for_orders` | `S3KeySensor` | `trigger.file_arrival` | 3 | Sensor task removed; job starts when files arrive in a UC volume/external location. |
| `ingest_bronze` | `PythonOperator` | `notebook_task` | 1 | Callable extracted to `src/ingest_bronze.py`; uses Auto Loader. |
| `transform_silver` | `PythonOperator` | `notebook_task` | 1 | Callable extracted to `src/transform_silver.py`. |
| `dq_order_totals` | `SQLExecuteQueryOperator` | `sql_task` | 1 | Inline SQL extracted to `src/dq_order_totals.sql`. |
| `choose_validation` | `BranchPythonOperator` | `condition_task` | 2 | Simple boolean branch becomes a native Lakeflow condition task. |
| `full_validation` | `PythonOperator` | `notebook_task` | 1 | Callable extracted to `src/full_validation.py`. |
| `skip_full_validation` | `EmptyOperator` | Removed | 2 | False branch rewired directly to `publish_gold`. |
| `publish_gold` | `PythonOperator` | `notebook_task` | 1 | `trigger_rule="none_failed_min_one_success"` approximated as `run_if: NONE_FAILED` (also runs if all upstreams skip — see MIGRATION_NOTES). |

## Presentation Talking Points

- The generated job is event-driven instead of scheduler-plus-sensor driven.
- Airflow Jinja values such as `{{ ds }}` and `{{ params.catalog }}` become Lakeflow job parameters such as `{{job.parameters.run_date}}`.
- The SQL warehouse is externalized as `${var.warehouse_id}`.
- The file trigger uses a Unity Catalog volume/external location path instead of a raw wildcard S3 sensor path.
- Notebook tasks omit cluster definitions so they can run on serverless compute where enabled.

## Local QA

From `customer_orders_bundle/`:

```bash
databricks bundle validate -t dev
```

Set real values before deploying:

```bash
databricks bundle validate -t dev --var warehouse_id=<WAREHOUSE_ID>
databricks bundle deploy -t dev --var warehouse_id=<WAREHOUSE_ID>
```
