# Migration Notes

## Summary

Converted Airflow **3** DAG: `regional_ingest` (authored with the Task SDK).

Generated a **two-job** bundle:
- `regional_ingest_job` (parent) — the TaskFlow chain, the dynamically-mapped `checksum`
  task, and the mapped task group driven via a for-each.
- `region_pipeline_job` (child) — the `ingest → validate → publish` subgraph that runs once
  per region.

## Airflow 3 recognition

| Airflow 3 construct | Handling |
|---|---|
| `from airflow.sdk import Asset, dag, task, task_group` | Recognized as the Task SDK; mapped like their Airflow 2 equivalents. |
| `from airflow.providers.standard.operators.empty import EmptyOperator` | Standard-provider import; `EmptyOperator` removed and its edges rewired (here `start` is dropped and `plan_run` becomes a root task). |
| `schedule=[Asset("orders-raw", extra={"databricks_table": "main.commerce.orders_raw"})]` | Asset resolves to a UC table via `extra["databricks_table"]`, so it maps to a `trigger.table_update` on `${var.orders_raw_table}`. |

## Pattern decisions

| Airflow pattern | Lakeflow/DABs output | Notes |
|---|---|---|
| `plan_run` (`@task(multiple_outputs=True)`) → `announce` | Two `notebook_task`s; dataflow via task values | `multiple_outputs` sets one task value per returned key (`regions`, `batch_size`); `announce` reads `{{tasks.plan_run.values.batch_size}}`. Shown on a NON-mapped chain because a mapped task's outputs cannot be consumed downstream. |
| `checksum.partial(catalog="main").expand(table=[...])` | `for_each_task` with a `notebook_task` body | Literal collection → JSON-array literal `inputs`; `{{input}}` is the table; `catalog` is the `.partial` constant. |
| `region_pipeline.expand(region=[...])` (mapped task group) | `for_each_task` → `run_job_task` → child `region_pipeline_job` | A `for_each_task` nests one task, not a subgraph, so the group's steps live in the child job. Element passed via `job_parameters.region = "{{input}}"`. |

## Concurrency, queueing, and nesting

- The parent `for_each_task.concurrency` is `3`. The child job's `max_concurrent_runs` is `6`
  (≥ the for-each concurrency, with headroom for overlapping parent runs) and it sets
  `queue: { enabled: true }` — bundle/API jobs do **not** inherit the UI's default-on
  queueing, so without it concurrent iterations beyond `max_concurrent_runs` would be
  **skipped** rather than queued.
- `for_each → run_job → child` uses one of the **3** allowed Run Job nesting levels. The child
  job does not call Run Job, so total depth is 2 — within the limit.

## Manual review / flags

- **No cross-iteration outputs.** The child job uses task values only to pass `ingest` and
  `validate` return values within the same child run; it does not persist cross-run results. To
  aggregate across regions, write each iteration's result to a table or volume keyed by run and
  region, then add a separate task that reads those persisted records — not the original
  `REGIONS` list.
- **Multi-argument `.expand(a=…, b=…)`** (Cartesian product) and **chained mapping**
  (a mapped task feeding another mapped task) are not present here; if introduced, precompute a
  single array of objects upstream or factor into a child job — a plain `for_each_task` takes
  one `inputs` array and cannot nest another `for_each_task`.
- **`inputs` size/transport.** Literal arrays are used here (small). For a large collection,
  produce it as an upstream task value and reference `{{tasks.<key>.values.<name>}}` (≤48 KiB);
  a job-parameter ref is capped at 10,000 chars and a literal at 5,000. All must be JSON.

## Validation

From `regional_ingest_bundle/`:

```bash
databricks bundle validate -t dev
```

If Databricks auth is not configured locally, run a structural check against the emitted schema:

```bash
databricks bundle schema > /tmp/bundle.schema.json
# then validate databricks.yml + resources/*.yml against /tmp/bundle.schema.json
```
