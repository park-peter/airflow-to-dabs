# Dynamic Mapping + Airflow 3 Demo

A runnable demonstration of the `airflow-to-dabs` skill converting an **Airflow 3** DAG that
exercises TaskFlow dataflow, dynamic task mapping, and a mapped task group over a collection.

## Source DAG

`airflow/regional_ingest_dag.py` — authored against the Airflow 3 surface:

- Task SDK imports (`from airflow.sdk import Asset, dag, task, task_group`).
- A standard-provider import (`airflow.providers.standard.operators.empty`).
- Asset-based scheduling with an explicit table binding
  (`Asset("orders-raw", extra={"databricks_table": "main.commerce.orders_raw"})`).

## Generated Bundle

`regional_ingest_bundle/`

```text
regional_ingest_bundle/
  databricks.yml
  resources/
    regional_ingest_job.yml     # parent: TaskFlow chain + dynamic mapping + mapped-group for_each
    region_pipeline_job.yml      # child: ingest -> validate -> publish subgraph (one run per region)
  src/
    plan_run.py                  # @task(multiple_outputs=True) -> one task value per key
    announce.py                  # consumes plan_run.batch_size; returns a single value
    checksum.py                  # for_each nested body; reads {{input}} + partial constant
    region_ingest.py
    region_validate.py
    region_publish.py
  MIGRATION_NOTES.md
```

## Pattern Mapping

| Airflow construct | Lakeflow/DABs output | Tier | Notes |
|---|---|---|---|
| `schedule=[Asset(..., extra={"databricks_table": ...})]` | `trigger.table_update` | 3 | Asset bound to a UC table via `extra`. |
| `plan_run` (`multiple_outputs=True`) → `announce` | two `notebook_task`s + task values | 1 | One task value per returned key; consumer reads `{{tasks.plan_run.values.batch_size}}`. |
| `checksum.partial(catalog=...).expand(table=[...])` | `for_each_task` (notebook body) | 2 | `{{input}}` = table; `.partial` → constant `base_parameters`. |
| `region_pipeline.expand(region=[...])` | `for_each_task` → `run_job_task` → child job | 2 | Multi-step subgraph moved into `region_pipeline_job`. |
| `EmptyOperator(start)` | Removed, edges rewired | 2 | Standard-provider import recognized. |

## Talking Points

- One bundle exercises **both** the Airflow 3 authoring surface (SDK + standard-provider imports,
  Asset scheduling) and the three mapping patterns.
- `multiple_outputs` is demonstrated on a non-mapped chain because a mapped task's per-iteration
  outputs cannot be consumed downstream.
- The mapped task group becomes a parent `for_each_task` that hops to a child job via
  `run_job_task` — the only way to fan a multi-step subgraph out over a collection.
- The child job explicitly sets `max_concurrent_runs` and `queue: { enabled: true }`, because
  bundle/API jobs do not inherit the UI's default-on queueing.

## Local QA

From `regional_ingest_bundle/`:

```bash
databricks bundle validate -t dev
```

Offline (no workspace profile), validate structure against the emitted schema:

```bash
databricks bundle schema > /tmp/bundle.schema.json
```
