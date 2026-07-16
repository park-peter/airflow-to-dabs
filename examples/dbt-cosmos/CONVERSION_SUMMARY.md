# Cosmos dbt Airflow to DABs Demo (Factory Mode)

This folder demonstrates the `airflow-to-dabs` skill converting an Airflow DAG that uses
astronomer-cosmos (`DbtTaskGroup`) into a two-job Databricks bundle using **dbt factory
mode**: the dbt project is exploded into one Lakeflow task per dbt object via
[databricks-dbt-factory](https://github.com/mwojtyczka/databricks-dbt-factory) and PyDABs.

## Source DAG

`airflow/orders_analytics_dag.py`

1. Ingest raw order files into a raw table (`PythonOperator`).
2. Run the dbt project through cosmos `DbtTaskGroup` — cosmos renders one Airflow task
   per dbt model/seed/test at runtime from the dbt manifest.
3. Publish daily aggregates to a dashboard feed table (`PythonOperator`).

## Generated Bundle

`orders_analytics_bundle/`

```text
orders_analytics_bundle/
  databricks.yml                     # include: resources/*.yml + python: (PyDABs) + sync
  pyproject.toml                     # databricks-bundles, databricks-dbt-factory, dbt-databricks (uv.lock git-ignored)
  Makefile                           # setup / manifest / validate / deploy / run
  resources/
    orders_analytics_job.yml         # YAML job: ingest -> run_job_task -> publish
    orders_analytics_dbt_job.py      # PyDABs hook: manifest -> one task per dbt node
  dbt_profiles/profiles.yml          # dev/prod outputs; host/token injected by runner
  dbt_project.yml  models/  seeds/   # the dbt project, colocated at bundle root
  target/dev/manifest.json           # per-target; checked in so `bundle validate` works without dbt
  dbt_serverless_env.yaml            # written at deploy time; pins dbt-databricks + dbt-core
  src/
    ingest_orders.py                 # extracted from PythonOperator
    publish_metrics.py               # extracted from PythonOperator
    run_dbt_command.py               # owned runner (0.2.1 base + dbt_vars / per-target cache)
  MIGRATION_NOTES.md
```

## Operator Mapping

| Airflow Task | Airflow Construct | Lakeflow/DABs Output | Tier | Notes |
|---|---|---|---|---|
| `ingest_orders` | `PythonOperator` | `notebook_task` | 1 | Callable extracted to `src/ingest_orders.py`. |
| `dbt_transform` (group) | cosmos `DbtTaskGroup` (5 dbt nodes) | `run_job_task` → Python-generated job with 6 tasks (1 seed, 3 models, 2 tests) | 2 | Factory mode: the generator is swapped, not the tasks translated. |
| `publish_metrics` | `PythonOperator` | `notebook_task` | 1 | Callable extracted to `src/publish_metrics.py`. |

Generated dbt job task graph (from `databricks bundle summary`):

```text
seed_country_codes ──► model_dim_countries ─────────────┐
model_stg_orders ──► test_unique_order_id ──────────────┼──► model_fct_daily_orders
                 └─► test_not_null_order_id ────────────┘
```

## Presentation Talking Points

- **Swap the generator, don't translate tasks.** Cosmos and databricks-dbt-factory are
  independent projects with no integration — both read the same `manifest.json` (a stable
  dbt-core artifact) and render one orchestrator task per dbt node. Migration points the
  Databricks-side generator at the same manifest.
- Per-model observability carries over: model-level failures, retry-only-the-failed-model,
  and tests gating downstream models — natively in Lakeflow instead of via an external
  scheduler.
- The dbt job is not YAML: it's generated at `bundle deploy` time by the PyDABs hook, so
  the task graph tracks the dbt project automatically as models are added.
- Serverless tasks use a pre-built base environment pinned to the locally tested
  dbt-databricks AND dbt-core versions — no per-task pip install. Pinning dbt-core
  too keeps the runtime matcher identical to the one the glue checks against.

## Local QA

From `orders_analytics_bundle/`:

```bash
make setup      # uv sync --dev (venv for the PyDABs hook)
make manifest   # dbt deps + dbt parse (no warehouse connection needed)
databricks bundle validate -t dev
```

Set real values before deploying (see `MIGRATION_NOTES.md`):

```bash
make deploy
make run
```
