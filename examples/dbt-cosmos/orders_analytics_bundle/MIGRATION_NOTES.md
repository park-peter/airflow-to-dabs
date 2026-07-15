# Migration Notes: orders_analytics

Converted from Airflow DAG `orders_analytics` (astronomer-cosmos) using dbt factory mode.

## Conversion decisions

| Airflow construct | Conversion | Notes |
|---|---|---|
| `PythonOperator ingest_orders` | `notebook_task` (`src/ingest_orders.py`) | Callable body extracted. |
| cosmos `DbtTaskGroup dbt_transform` | Separate Python-generated job (`resources/orders_analytics_dbt_job.py`) triggered via `run_job_task` | Cosmos renders dbt models as Airflow tasks at runtime from `manifest.json`; databricks-dbt-factory regenerates the same per-model task graph natively from the same manifest. The group is not translated task-by-task — the generator is swapped. |
| `PythonOperator publish_metrics` | `notebook_task` (`src/publish_metrics.py`) | Callable body extracted. |
| `schedule_interval="@daily"` | `schedule.quartz_cron_expression: 0 0 0 * * ?` (UTC) | |
| `default_args.retries: 2` / `retry_delay: 5m` | Task-level `max_retries: 2`, `min_retry_interval_millis: 300000` | Applied to YAML-job notebook tasks. dbt job tasks rely on Lakeflow repair/rerun. |
| `default_args.email` | `email_notifications.on_failure` | |
| cosmos `DatabricksTokenProfileMapping` | `dbt_profiles/profiles.yml` with `DBT_HOST` / `DBT_ACCESS_TOKEN` injected by the runner notebook | No Airflow connection needed; the runner uses the notebook context token. |
| cosmos `RenderConfig(test_behavior=AFTER_EACH)` | Factory default: one test task per test node, downstream models gated on tests | Equivalent behavior. |

## Two-job layout

- `orders_analytics_job` (YAML): ingest → `run_job_task` → publish.
- `orders_analytics_dbt_job` (Python, generated at deploy time): 6 tasks — 1 seed, 3 models, 2 tests — dependencies wired from the dbt DAG. Per-model retry and repair happen inside this job.

## Action items before deploying

1. Replace `<WAREHOUSE_ID>` in `dbt_profiles/profiles.yml` (both targets) with the SQL warehouse that should execute dbt SQL.
2. Replace `<SERVICE_PRINCIPAL>` in `databricks.yml` for the prod target.
3. Confirm catalog/schema (`main.analytics`) matches your environment; adjust `dbt_profiles/profiles.yml`, `models/staging/schema.yml` (source), and the `src/` notebooks together.
4. Run `make setup && make manifest` before the first `databricks bundle deploy` — the PyDABs hook needs the venv and `target/manifest.json`.

## Caveats

- **Selectors:** factory mode explodes the entire manifest. This DAG's cosmos group ran the whole project (no `RenderConfig(select=...)`), so semantics are unchanged. If your DAG subsets the project with selectors, converting to factory mode runs more than Airflow did — confirm intentionally or fall back to a single `dbt_task`.
- **Serverless only:** generated tasks run the runner notebook on serverless with a pre-built base environment (`dbt_serverless_env.yaml`, pinned to the venv's dbt-databricks version). For classic compute, set `job_cluster_key` in `DbtTaskOptions` and define the cluster on the job — the cluster only runs dbt's parse/dispatch; SQL still executes on the warehouse from `profiles.yml`.
- **Large projects:** one task per dbt node, and a Databricks job can contain up to 1,000 tasks. For manifests approaching that, split by tag into multiple factory jobs or stay on a single `dbt_task`.
- **Generated files:** `dbt_serverless_env.yaml` and `src/run_dbt_command.py` are rewritten idempotently at validate/deploy time from the installed databricks-dbt-factory version; edit the pins in `pyproject.toml`, not the files.
