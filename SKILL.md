---
name: airflow-to-dabs
description: Converts Apache Airflow DAG files into Databricks Declarative Automation Bundles projects, formerly called Databricks Asset Bundles and commonly abbreviated DABs. Use when migrating Airflow DAGs to Databricks Lakeflow Jobs, converting Airflow operators to bundle task types, converting dbt-on-Airflow workloads (astronomer-cosmos DbtDag/DbtTaskGroup, dbt operators) to per-model Lakeflow jobs, or generating databricks.yml and job resource YAML from Airflow Python files. Triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, Airflow to Lakeflow, cosmos or dbt DAG migration, Asset Bundles, Declarative Automation Bundles, or DABs generation from Airflow.
---

# Airflow to Databricks Declarative Automation Bundles Converter

Convert Apache Airflow DAG files into complete Databricks Declarative Automation Bundles projects (formerly Databricks Asset Bundles; DABs), producing `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files ready for `databricks bundle deploy`.

## Capabilities

- Parse Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Map 40+ Airflow operator types (including all Databricks provider operators) to their DABs task type equivalents using a tiered mapping system
- Convert Airflow cron expressions and presets to Quartz cron format
- Convert Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (file_arrival, table_update)
- Extract inline Python callables, SQL strings, and bash commands into standalone source files
- Convert Airflow Jinja template variables to DABs dynamic value references
- Map `default_args` (retries, timeouts, email notifications) to DABs job/task settings
- **dbt factory mode (default for dbt workloads)**: convert dbt workloads — including astronomer-cosmos `DbtDag`/`DbtTaskGroup` — into a separate Lakeflow job with one task per dbt model/seed/snapshot/test, generated at deploy time from the dbt manifest via PyDABs and `databricks-dbt-factory`; single `dbt_task` remains as a fallback
- Generate `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- Handle TaskGroups, SubDAGs, branching operators, Airflow dynamic task mapping, and XCom patterns
- **Hadoop/HDFS migration**: detect `spark-submit` in BashOperator/SSHOperator, clean up YARN Spark configs, map HDFS paths, convert HiveQL to Spark SQL, handle SqoopOperator alternatives
- Bulk conversion guidance for DAGs with hundreds of Spark tasks

## Workflow

### Phase 1: Parse the Airflow DAG

Read the provided Airflow DAG file(s) and extract the following structure:

1. **DAG metadata**: `dag_id`, `schedule_interval`/`schedule`, `default_args`, `catchup`, `tags`, `params`, `dagrun_timeout`, `sla_miss_callback`, `max_consecutive_failed_dag_runs`
2. **Task inventory**: For each task, capture:
   - `task_id` and operator class (e.g., `PythonOperator`, `BashOperator`)
   - Operator-specific parameters (`python_callable`, `bash_command`, `sql`, `application`, `json`, etc.)
   - `op_kwargs`, `op_args`, `params`, `templates_dict`
   - **Airflow version signals**: recognize both Airflow 2 imports (`airflow.operators.*`, `airflow.sensors.*`) and **Airflow 3** imports (`airflow.sdk.*`, `airflow.providers.standard.{operators,sensors}.*`). The operator mappings are the same; only the import path differs. A task whose import path is unrecognized must be surfaced, never dropped. See `references/airflow3-migration.md`.
3. **Dependency graph**: Extract `>>` / `<<` chains, `set_upstream`/`set_downstream` calls, **and TaskFlow call wiring** (`b(a())` / passing one `@task`'s return into another implies `depends_on`; `.override(task_id=...)` sets the task key). Merge classic and TaskFlow edges into one graph.
4. **Sensors**: Identify sensor tasks and their trigger conditions (S3 path, table name, external DAG, time). Capture whether the sensor returns data consumed downstream and the exact predicate/listing contract, including prefix recursion, pagination, glob/suffix filters, sorting, and timeout/soft-fail behavior.
5. **TaskGroups / SubDAGs**: Identify grouped tasks and their internal structure, including **mapped task groups** (`@task_group.expand()` / `TaskGroup.partial().expand()`)
6. **Flags**: Note any custom operators (subclasses of BaseOperator), XCom usage (`xcom_push`/`xcom_pull`), Airflow Variables, Airflow Connections, dynamic task mapping (`.expand()`/`.expand_kwargs()`), **deferrable operators/sensors** (any Airflow version, 2.2+ — `deferrable=True`/`*DeferrableOperator`/`mode="reschedule"`; ignore the deferrability and map the underlying operation; a sensor converting to a job trigger generates no polling, a retained sensor keeps its poke_interval — see the deferrable note in `references/operator-mapping.md`), and custom timetable/dataset schedules. **Airflow 3-specific:** `Asset` scheduling (`schedule=[Asset(...)]`, boolean asset expressions, `AssetOrTimeSchedule`, and the Airflow 2.4–2.10 `DatasetOrTimeSchedule`), native async `@task` (3.2.0 `async def`; rewrite hooks to native async clients), and resumable external jobs (`ResumableJobMixin`, 3.3.0; preserve reattachment or flag) — see `references/airflow3-migration.md`.
7. **dbt workloads**: Detect and flag dbt usage — these are subject to the dbt conversion decision point in Phase 2:
   - cosmos imports: `from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig`, `cosmos.profiles.*` mappings, `cosmos.operators.*`
   - dbt CLI operator families: `airflow_dbt.operators.dbt_operator` and `airflow_dbt_python.operators.dbt` (`DbtRunOperator`, `DbtTestOperator`, `DbtSeedOperator`, `DbtSnapshotOperator`, `DbtDepsOperator`, `DbtBuildOperator`)
   - dbt Cloud provider: `airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator`, `DbtCloudJobRunSensor`
   - `BashOperator`/`SSHOperator` commands matching `dbt (deps|seed|snapshot|run|test|build|docs)`
   - Capture: `project_dir`/`dbt_project_path`, `profiles_dir`/profile mapping, `target`, `select`/`exclude`/`models`, `vars`, `full_refresh`, and whether the dbt project source or a `manifest.json` is available to the conversion
   - Summary-table convention: a cosmos `DbtDag`/`DbtTaskGroup` appears as one row with DABs Task Type `dbt factory job (or dbt_task)`, Tier 2, note "decision point -- see Phase 2". dbt CLI operator tasks keep their own rows (Tier 1) with the same DABs Task Type and note; multiple dbt tasks over the same project (e.g. seed >> run >> test) collapse into a single factory job in Phase 3

Present a summary table to the user before proceeding:

```
| Task ID           | Operator                | DABs Task Type      | Tier | Notes              |
|-------------------|-------------------------|---------------------|------|--------------------|
| extract_data      | PythonOperator          | notebook_task       | 1    |                    |
| check_env         | BranchPythonOperator    | condition_task      | 2    | Simple equality    |
| wait_for_file     | S3KeySensor             | trigger.file_arrival| 3    | Becomes job trigger|
| custom_step       | MyCustomOperator        | notebook_task       | 4    | MANUAL REVIEW      |
```

### Phase 2: Map Operators to DABs Task Types

Read `references/operator-mapping.md` in this skill's directory for the authoritative mapping table.

**Source-aware classification (before the Tier tables).** Operator class alone does not fix the mapping — the connection does. Resolve `operator → connection type → operation intent → data direction → destination contract → strategy` first: a Databricks SQL connection → `sql_task`; a remote federatable DB with a read-only SELECT → Lakehouse Federation over a foreign catalog; remote DML/DDL → connector notebook or migrate the target; recurring source→Delta ingestion from an eligible source → **Lakeflow Connect** (`references/lakeflow-connect.md`); files in cloud storage → Auto Loader; unsupported source → notebook/SDK + flag. **Connection resolution is fail-closed**: route automatically only from operator/provider certainty, the actual sanitized `conn_type`, or an explicit user mapping — a `conn_id` name/host is a hint only, and unresolved connections are manual review. Never inline credentials. **Never emit a guessed executable default** for an unresolved catalog, schema, table, path, connection, job, warehouse, or other identifier; use a required bundle variable with no default or a deliberately invalid `<REQUIRED_...>` placeholder and add a required action to `MIGRATION_NOTES.md`. (Athena/Trino/Presto are NOT federatable.)

**Snowflake & SQL checks.** Snowflake has no managed connector — route by intent: read-only SQL → federation over a Snowflake foreign catalog; recurring Snowflake→Delta → query-based Lakeflow Connect via foreign catalog; DML/DDL → connector notebook or rewrite. `SnowflakeOperator`/`S3ToSnowflakeOperator` are removed (use `SQLExecuteQueryOperator`/`CopyFromExternalStageToSnowflakeOperator`); recognize both `@task.snowpark` and `snowpark_task`. SQL data-quality **check** operators (`SQLColumnCheck`/`SQLTableCheck`/`SQLValueCheck`/`SQLThresholdCheck`/`SQLIntervalCheck`, `Snowflake*Check`) → a `sql_task` using `assert_true()`; preserve tolerance/interval/partition/null/dynamic-threshold semantics or flag. `@task.sql` is NOT a check — it's generic `SQLExecuteQueryOperator`; route by connection + SQL intent, `assert_true()` only if it's actually an assertion. See `references/operator-mapping.md`.

For each task in the inventory:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields per the reference.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: If the branching logic is a simple comparison, use `condition_task`. If complex, use a two-step pattern (notebook + condition).
   - `BranchDateTimeOperator` / `BranchDayOfWeekOperator`: use an evaluator `notebook_task` plus `condition_task`; preserve logical-date versus wall-clock choice, DAG timezone, inclusive/overnight range semantics, branch lists, and Airflow skip-propagation caveats per `references/operator-mapping.md`.
   - `DummyOperator`/`EmptyOperator`: Remove from the task list. Rewire `depends_on` so downstream tasks point to the dummy's upstream tasks.
   - `SubDagOperator`/`TaskGroup`: Flatten into the parent job with prefixed task keys, or extract to a separate job via `run_job_task`. (`SubDagOperator` is removed in Airflow 3.)
   - **Dynamic task mapping** (`.expand()`/`.expand_kwargs()`): map to `for_each_task` with `{{input}}` in the nested task; `.partial()` kwargs become constant `base_parameters`. Only when the collection is a literal or an upstream task-value/job-param ref (choose the transport by size — literal ≤5,000 chars, task value ≤48 KiB, job param ≤10,000 chars, all JSON). Flag multi-arg Cartesian products, chained/reduced mapping, and non-deterministic collections. See the Dynamic-task-mapping support matrix in `references/operator-mapping.md`.
   - **Mapped task group** (`@task_group.expand()`): map to `for_each_task` → `run_job_task` → a **child job** holding the group's subgraph (a `for_each_task` nests one task, not a subgraph). Set the parent `concurrency`, raise the child job's `max_concurrent_runs`, and set `queue: {enabled: true}` on the child (bundle jobs don't inherit UI queueing); keep total Run Job nesting ≤ 3. Per-iteration outputs can't be consumed downstream. See the Mapped-task-group section in `references/operator-mapping.md`.
   - **Cloud & messaging operator families** (AWS Athena/EMR/Glue/Lambda/Redshift/SageMaker/SQS/SNS; GCP BigQuery/Dataproc/Dataflow/PubSub; Azure ADF/Synapse; HTTP/SFTP/Kafka/Trino/etc.): no 1:1 task — route by intent per the classification step. Remote query → federation (federatable sources only; Athena/Trino/Presto are not); recurring source→Delta → Lakeflow Connect; remote compute → migrate to notebook/SQL/pipeline; retained remote orchestration → SDK notebook; Kafka→Delta → managed Kafka connector or Structured Streaming; messaging side-effects → SDK notebook. State exact import paths only if verified. See the Cloud & messaging section in `references/operator-mapping.md`.
3. **Tier 3 (sensors)**: Convert to job-level triggers. Read `references/schedule-trigger-mapping.md` in this skill's directory.
   - File sensors -> `trigger.file_arrival`; set `queue.enabled: true`, keep trigger and ingestion discovery recursive over the same root, preserve the original filter, and document the initial run needed for files that predate trigger creation
   - Table/SQL sensors -> `trigger.table_update`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table_update`
   - **Retained sensors that return file collections** stay in the task graph and must preserve the source listing semantics exactly. For a recursive source-prefix listing, copy the `list_files_recursive()` pattern from `references/operator-mapping.md` into the generated notebook and call it on every polling attempt before applying the original filters. Never replace recursive discovery with a single shallow `dbutils.fs.ls(root)` call.
   - `BashSensor` / `PythonSensor`: convert to a supported job trigger only when the complete root predicate is provably equivalent and its output is unused; otherwise retain a polling notebook with timeout, return-value, and side-effect semantics documented. A retained sensor keeps its `poke_interval` at every `mode`/`deferrable` setting. `soft_fail=True` always requires a `condition_task` on `sensor_satisfied` gating the downstream subgraph, because Airflow's skip propagates under the default `all_success` trigger rule.
   - Remove only sensors that become job-level triggers; retained polling/output sensors remain as tasks in the original graph.
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add entry to `MIGRATION_NOTES.md`.

**dbt decision point**: For any dbt workload flagged in Phase 1, **default to dbt factory mode** — read the dbt conversion decision point in `references/operator-mapping.md` (Tier 1 dbt CLI operators section) for the full rules and the cosmos section in Tier 2 for the generated artifacts. Key rules:
   - Enable only the factories matching the union of detected dbt commands (`run`->model, `seed`->seed, `snapshot`->snapshot, `test`->test, `build`->all); `deps`/`docs`-only workloads are not factory-eligible.
   - Multiple dbt operator tasks over the same project (e.g. seed >> run >> test) collapse into ONE factory job with ONE `run_job_task` hop.
   - **Task-count check (1,000-task per-job limit):** after `make manifest`, run `make task-count` to compare the unbundled vs bundled task counts. If the unbundled count exceeds the warn threshold (900), warn the user and offer to set `BUNDLE_TESTS = True` in the glue — this collapses each resource's single-model tests into one `<resource>_test` task, the biggest available reduction, at the cost of coarser retry granularity (a model's tests rerun together, not per individual test). Keep `BUNDLE_TESTS = False` (per-test observability) when the count is within budget. If even the bundled count exceeds 1,000, do NOT auto-fall-back: record the options in MIGRATION_NOTES (split the project by dbt tag into multiple factory jobs, await a dbt-factory sub-job-splitting API, or a user-chosen single `dbt_task`). The glue also fails closed at deploy time above 1,000 tasks.
   - Fall back to a single `dbt_task` on a disqualifier: dbt project **source** unavailable (a manifest alone is not enough — runtime needs the project files; source without a manifest is fine), unconfirmed selector subsetting, unresolved `full_refresh=True` (never applied automatically), graph-changing or conflicting per-operator vars (static vars live in one committed `dbt_vars.json`, read by `make manifest` at parse time and by the runner at run time), more than one dbt project in the bundle, or explicit user opt-out.
   - **dbt Cloud (`DbtCloudRunJobOperator`) never falls back to `dbt_task`** (dbt_task runs dbt Core and cannot trigger a dbt Cloud job) — route to Tier 4.
   - Surface the choice and its toolchain implications (PyDABs `python:` block, `pyproject.toml` + `.venv`, `Makefile`, `uv`) in the Phase 1 summary so the user can override before Phase 3.

For schedule conversion, read `references/schedule-trigger-mapping.md` in this skill's directory:
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week numbering, and normalize Sunday `0/7 -> 1`)
- Convert Airflow presets (`@daily`, `@hourly`, etc.) to Quartz equivalents
- Extract timezone from `default_args` or DAG `start_date` (a naive `start_date` means UTC, Airflow's default)
- Convert `@continuous` to job-level `continuous` mode
- For dataset/timetable schedules, map to `trigger.table_update` when deterministic, otherwise flag in `MIGRATION_NOTES.md`
- For Airflow `Asset`/`Dataset` scheduling (`schedule=[Asset(...)]`, boolean asset expressions, `AssetOrTimeSchedule`, and the Airflow 2.4–2.10 `DatasetOrTimeSchedule`), follow the Asset→UC-table resolution rule and boolean/time semantics in `references/schedule-trigger-mapping.md` — map asset-only schedules to `trigger.table_update` only when the asset resolves to a UC table; for a mixed time+asset schedule, emit a manual job with neither arm until the user chooses time, asset, or split jobs
- Map each task's `trigger_rule` to `run_if` per the table in `references/schedule-trigger-mapping.md`. Five rules map exactly; `none_failed*` are **approximations** (map `none_failed_min_one_success` → `NONE_FAILED`, never `AT_LEAST_ONE_SUCCESS`, and record that the task can now also run when all upstreams skipped); `always`/`dummy`/`none_skipped`/`all_skipped`/`one_done` and setup/teardown rules have no faithful mapping → `condition_task` or flag. **Never default an unrecognized `trigger_rule` to `ALL_SUCCESS`** — that is indistinguishable from a correct mapping and hides the loss
- Map a static positive `dagrun_timeout` to Job `timeout_seconds`. Treat explicit disabled policy (`depends_on_past=False`, email flags `False`, `sla_miss_callback=None`, `max_consecutive_failed_dag_runs=0`, empty `env`) as no-ops. Flag active cross-run dependencies, retry email, arbitrary SLA callbacks, automatic pause after repeated failures, dynamic timeouts, and non-empty task environments; do not substitute concurrency controls for prior-run or failure-history semantics.

### Phase 3: Generate the DABs Project

Produce the following output files. Read `references/dab-schema-reference.md` in this skill's directory for the complete YAML schema. Use `assets/templates/databricks.yml.tmpl` and `assets/templates/job-resource.yml.tmpl` as starting skeletons.

#### Output Modes

**Multi-DAG (default):** When converting multiple DAGs, produce a **single bundle** with one `databricks.yml` and a separate job resource file per DAG under `resources/`. This is the default because it enables cross-job references via `${resources.jobs.<name>.id}` and allows a single `databricks bundle deploy`.

**Single-DAG:** When converting one DAG, produce a standalone bundle directory.

**Split bundles (opt-in):** If the user explicitly requests separate bundles per DAG (e.g., "create a separate bundle for each DAG"), produce one bundle directory per DAG. Cross-DAG `TriggerDagRunOperator` references will require hardcoded job IDs and a note in MIGRATION_NOTES.md.

**dbt factory mode (default for dbt workloads):** Orthogonal to the modes above. For each dbt-bearing DAG (per the Phase 2 decision point), generate a second, Python-defined job — one task per dbt model/seed/snapshot/test, built at deploy time from the dbt manifest — and place a `run_job_task` in the DAG's YAML job where the dbt workload sat (upstream tasks → `run_job_task` → downstream tasks). See the factory-mode structure below and the artifact table in `references/operator-mapping.md` (Tier 2 cosmos section).

#### Multi-DAG Output Structure (default)

```
<bundle-name>/
  databricks.yml                    # Single bundle config with shared variables and targets
  resources/
    <dag_id_1>_job.yml              # One job resource per DAG
    <dag_id_2>_job.yml
    <dag_id_3>_job.yml
  src/
    <dag_id_1>/                     # Source files namespaced per DAG
      <task_id>.py
      <task_id>.sql
    <dag_id_2>/
      <task_id>.py
    <dag_id_3>/
      <task_id>.py
  MIGRATION_NOTES.md                # Consolidated migration notes for all DAGs
```

#### Single-DAG Output Structure

```
<dag_id>-bundle/
  databricks.yml
  resources/
    <dag_id>_job.yml
  src/
    <task_id>.py
    <task_id>.sql
  MIGRATION_NOTES.md
```

#### Factory-Mode Additions (per dbt-bearing DAG)

```
<bundle>/
  databricks.yml                  # + python: block and sync.include (additions template)
  pyproject.toml                  # pins databricks-bundles, databricks-dbt-factory; exact dbt-databricks + dbt-core
  Makefile                        # setup / manifest / validate / deploy
  resources/
    __init__.py                   # empty package marker
    <dag_id>_job.yml              # YAML job with run_job_task hop
    <dag_module>_dbt_job.py       # PyDABs hook (one per dbt-bearing DAG; dag_id sanitized to identifier)
  dbt_profiles/
    profiles.yml
  dbt_project.yml  models/  seeds/  ...   # the dbt project, colocated at bundle root
  target/dev/manifest.json        # per-target; git-ignored, produced by `make manifest TARGET=dev`
  dbt_vars.json                   # committed static vars ({} when none)
  dbt_serverless_env.yaml         # written at validate/deploy time by the hook
  src/
    run_dbt_command.py            # runner notebook (owned; from dbt-run-command.py.tmpl)
```

**File generation rules:**

1. **`databricks.yml`**: For multi-DAG, derive `bundle.name` from a user-provided name or the parent directory name. For single-DAG, derive from `dag_id` (kebab-case). Include `variables` for `spark_version`, `node_type_id`, `warehouse_id`. Define `dev` and `prod` targets. Use `include: - resources/*.yml` to pull in all job definitions. In factory mode, merge in `assets/templates/dbt-factory-databricks-additions.yml.tmpl` (the `python:` block — one `resources.<dag_module>_dbt_job:load_resources` entry per dbt-bearing DAG, where `<dag_module>` is the dag_id sanitized to a valid Python identifier — and `sync.include`).

2. **`resources/<dag_id>_job.yml`**: One job resource file per DAG, each containing:
   - `schedule` or `trigger` from Phase 2
   - `email_notifications` from `default_args.email`
   - Job-level `timeout_seconds` from a static positive DAG `dagrun_timeout`
   - `parameters` from DAG `params` and Jinja variables like `{{ ds }}` (map `{{ ds }}`/`execution_date` to a `run_date` job parameter — classify wall-clock vs logical/partition semantics; for a logical date on a cron/scheduled job default it `{{job.trigger.time.iso_date}}` so native backfill can override it, but on an event-triggered job derive the date from the event instead; ask the user when ambiguous — see `references/schedule-trigger-mapping.md`)
   - `job_clusters` with a shared cluster definition
   - `tasks` list with all mapped tasks, preserving the dependency graph via `depends_on`
   - Task-level `max_retries` and `min_retry_interval_millis` from `default_args.retries` and `retry_delay`
   - Task-level `timeout_seconds` from `default_args.execution_timeout`
   - Cross-DAG references via `TriggerDagRunOperator` resolve to `${resources.jobs.<target-dag-job-key>.id}` within the same bundle

3. **`src/<dag_id>/*.py` notebooks**: For each `notebook_task` or `spark_python_task`:
   - In multi-DAG mode, namespace source files under `src/<dag_id>/` to avoid collisions
   - In single-DAG mode, place directly in `src/`
   - Start with `# Databricks notebook source`
   - Add `dbutils.widgets.text()` and `dbutils.widgets.get()` for each `base_parameters` entry
   - Extract the `python_callable` function body (not the function signature itself)
   - Replace Airflow imports with Databricks equivalents (e.g., `from airflow.models import Variable` -> `dbutils.widgets.get()`)

4. **`src/<dag_id>/*.sql` files**: For each `sql_task` with inline SQL:
   - Extract the SQL string
   - Dynamic value references CANNOT be used inline in SQL files. Replace Airflow Jinja with named parameter markers and pass values through `sql_task.parameters` (that is where `{{job.parameters.*}}` references live):
     - `{{ ds }}` -> `:run_date` in the SQL, with `sql_task.parameters: {run_date: "{{job.parameters.run_date}}"}`
     - `{{ params.x }}` -> `:x` in the SQL, with `sql_task.parameters: {x: "{{job.parameters.x}}"}`
     - Parameter markers only substitute VALUES; for identifiers (catalog/schema/table) use `IDENTIFIER(:catalog || '.' || :schema || '.table_name')`

5. **`MIGRATION_NOTES.md`**: A single consolidated file documenting:
   - Tier 4 operators flagged for manual review
   - XCom patterns that need conversion to `dbutils.jobs.taskValues`
   - Airflow Connections that need Databricks secrets or UC connections
   - Airflow Variables that need bundle variables or job parameters
   - `catchup=True` → the backfill expectation and that a native [Databricks backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs) should override `run_date` with `{{backfill.iso_date}}`; active `depends_on_past`, retry email, `sla`/`sla_miss_callback`, `max_consecutive_failed_dag_runs`, and non-empty `default_args.env` settings that need explicit replacement
   - Sensor-to-trigger conversions with notes on external location setup
   - Every **collapsed retry envelope**: when multiple Airflow tasks or mapped stages become one Lakeflow task/job hop, identify the original retry boundaries, the new retry boundary, and the possible repeated side effects or expanded rerun scope
   - Setup/teardown lifecycle changes: Airflow teardown runs only after its setup succeeds, while an ordinary Lakeflow teardown task follows explicit dependencies and `run_if`; teardown failure affects the Lakeflow job result unless explicitly redesigned, whereas Airflow teardown failure is excluded from DAG-run status by default unless configured otherwise
   - **Cross-DAG dependency map**: which jobs reference other jobs via `run_job_task`, with resolved `${resources.jobs...}` substitutions
   - **Factory mode (when active)**: selector semantics (whole-manifest explosion vs any Airflow-side `--select`/`--exclude`), serverless-only note with the classic-cluster manual variation (`job_cluster_key` in `DbtTaskOptions`), the measured task count and the 1,000-task per-job limit (whether `BUNDLE_TESTS` was enabled and its retry-granularity tradeoff; if over the limit even bundled, the split-by-tag / sub-job / single-`dbt_task` options), retry mapping (apply Airflow retries to the YAML job's own tasks only; never on the `run_job_task` hop, which would re-run the whole dbt job — per-model reruns use Lakeflow repair), vars semantics (static vars from the committed `dbt_vars.json` at parse AND run time; runtime `dbt_vars` overrides are graph-invariant only and trigger a per-task re-parse since the parse cache is bypassed), `full_refresh` manual-review note, the classic-compute variation requiring dbt installed on the cluster with both dbt-databricks AND dbt-core pinned exactly, and the fail-closed guards (generated selectors that do not resolve to exactly their own node, checked with dbt's own matcher — covers equal FQNs, directory shadows, package-stripped overlaps, leaf shortcuts, versioned models, glob names; any FQN component — package, directory, or name — outside the `[A-Za-z0-9_.-]` allowlist, which dbt's selector grammar or the runner's shlex would misparse; and a belt-and-suspenders unique-task-key check, since 0.3.1 already guarantees readable keys `<resource>_<type>`/`<resource>_test` unique and ≤100 chars), the runner also rejecting a dbt command that carries its own `--vars` (vars must use the canonical `dbt_vars.json`/`dbt_vars` channel), `dbt_profiles/profiles.yml` values to fill (`<WAREHOUSE_ID>`, catalog/schema), and the `make setup && make manifest` prerequisite before the first deploy

6. **Factory-mode artifacts** (per dbt-bearing DAG, when the Phase 2 decision point selects factory mode):
   - `resources/<dag_module>_dbt_job.py` from `assets/templates/dbt-factory-resources.py.tmpl` (replace `<DAG_ID>`, `<DAG_MODULE>` = dag_id sanitized to a Python identifier, `<DAG_ID_KEBAB>`, and `<FACTORY_TYPES>` from the detected dbt commands)
   - `src/run_dbt_command.py` from `assets/templates/dbt-run-command.py.tmpl` (owned runner: dbt_vars + per-target parse cache)
   - `dbt_vars.json` at the bundle root: the DAG's static vars as a JSON object (`{}` when none) — single source for parse time (Makefile) and run time (runner fallback)
   - `resources/__init__.py` (empty), `pyproject.toml` from `dbt-pyproject.toml.tmpl`, `Makefile` from `dbt-Makefile.tmpl`, `dbt_profiles/profiles.yml` from `dbt-profiles.yml.tmpl` (profile name must match `profile:` in the customer's `dbt_project.yml`), and `tests/test_dbt_factory_glue.py` from `dbt-tests.py.tmpl` (regression tests for the glue's guards; run `make test`)
   - **Fill the `<DBT_DATABRICKS_VERSION>`/`<DBT_CORE_VERSION>` pins in `pyproject.toml`** — never leave either placeholder unresolved. One rule:
     - **Preserve every dbt constraint the customer already declares** (exact pins, ranges like `dbt-databricks<1.12`, or either package alone), add only the *missing* dbt package(s) unconstrained, run `uv` resolution, then exact-pin both to the resolved versions. This keeps `uv` inside the customer's declared ranges. (Note `dbt-databricks` depends on `dbt-core`, so an adapter constraint alone can select a compatible core; a core-only constraint cannot select an adapter — declaring both covers either case.)
     - **Only when the project declares no dbt constraints at all**, use the skill's tested default pair `dbt-databricks==1.12.2` / `dbt-core==1.11.12` (still run `uv` resolution on it).
     - **Failure handling (all cases), and never auto-change the resolved pins:**
       - *Unsatisfiable `uv` solve* — a real dependency-metadata conflict; stop and report for manual resolution, preserving the customer's declared constraints.
       - *Any other `uv` failure* (network, proxy, index auth, download/install) — environmental; surface and stop.
       - *`make manifest`/`bundle validate` failure* — `uv` only proves dependency-metadata compatibility, not compatibility with the dbt internals the hook imports at generation (e.g. `is_selected_node`), so these are NOT automatically "unrelated." Preserve the pins; fix only clearly version-independent causes (auth, profiles, project parsing, bundle schema) directly; otherwise stop and surface the evidence. Repinning or a `dbt_task` fallback is then an explicit user decision — never automatic. (Automatic `dbt_task` fallback happens only for the enumerated factory disqualifiers in Phase 2, not for unexpected failures here.)
     - Exact pins give dbt version/runtime parity without a committed lockfile. The hook propagates whatever versions end up installed into the serverless base environment, so the two always match.
   - Copy the customer's dbt project to the bundle root (v1: exactly one dbt project per bundle; multiple projects require split bundles)
   - In the DAG's YAML job, define a `dbt_vars` job parameter (default `"{}"`) and place a `run_job_task` with `job_id: ${resources.jobs.<dag_module>_dbt_job.id}` and `job_parameters: {dbt_vars: "{{job.parameters.dbt_vars}}"}` where the dbt workload sat
   - In factory mode, run the YAML job's companion notebook tasks on serverless too: omit cluster fields (classic `job_clusters` fail at deploy on serverless-only workspaces)
   - `.gitignore` additions: `.venv/`, `uv.lock`, `logs/`, `dbt_packages/`, `target/**`, `dbt_serverless_env.yaml`. Generated artifacts are git-ignored: `target/<target>/manifest.json` is a LOCAL input the hook reads at deploy time (not synced); `dbt_serverless_env.yaml` and `target/*/partial_parse.msgpack` ARE uploaded via `sync.include` despite being git-ignored (`load_resources` writes the env before sync). Committed source: the owned runner (`src/run_dbt_command.py`), `dbt_vars.json`, `pyproject.toml`, and the other templates. `uv.lock` is git-ignored so the bundle carries no package-index URLs; exact `dbt-databricks`/`dbt-core` pins in `pyproject.toml` give dbt version/runtime parity (transitive deps are not locked)

### Phase 4: Review and Validate

After generating all files:

1. **Dependency check**: Verify every `depends_on` reference points to a valid `task_key` in the same job
2. **Orphan check**: Verify no tasks are unreachable (disconnected from the DAG)
3. **Task type check**: Verify each task has exactly one task type field
4. **Compute check**: Serverless notebook tasks may omit ALL compute fields (an `environment_key` is optional, used to pin dependencies). For classic compute, verify `job_cluster_key`/`existing_cluster_id`/`new_cluster` is present, and that every referenced `job_cluster_key` or `environment_key` is defined on the job
5. **Parameter check**: Verify all `{{job.parameters.*}}` references have corresponding entries in the job `parameters` list
6. **Retained sensor semantics check**: Compare every retained file sensor's generated discovery with the original hook/callable. If the source prefix listing is recursive, a notebook that uses only a single shallow `dbutils.fs.ls(root)` is a validation error; require explicit directory traversal or equivalent paginated object-store discovery before accepting the bundle.
7. **Bundle schema check**: Run `databricks bundle validate -t <target>` and fix schema warnings/errors (if auth is unavailable, run `databricks bundle schema` validation checks offline and report the limitation). An unassigned required bundle variable is an expected validate failure: report it as a value the user must supply, never resolve it by adding a default. In factory mode, complete step 8's setup/manifest sequence BEFORE this command -- validate executes the PyDABs hook, which needs the venv and manifest
8. **Factory-mode validation** (when active): Run `make setup` (creates `.venv` via `uv`), then `make manifest` (`dbt deps` + `dbt parse` — no warehouse connection needed, and the recipe must fail unless `target/<target>/manifest.json` exists), then `databricks bundle validate -t dev`. Validation executes the PyDABs hook, so it requires the venv and `target/dev/manifest.json` (per-target: `make manifest TARGET=prod` before any prod deploy — never reuse a dev-parsed manifest). The generated glue must inspect dbt's `is_selected_node` signature and call either its legacy two-argument API or current three-argument API while continuing to use dbt's own matcher. A RuntimeError from the hook's fail-closed checks (a generated selector not resolving to exactly its own node, checked with dbt's own imported matcher, or a task-key collision) means fall back to single `dbt_task` for that workload; databricks-dbt-factory 0.3.1 selects every node by its full dot-joined FQN, derives readable keys (`<resource>_<type>`, bundled `<resource>_test`) guaranteed unique and ≤100 chars, and emits unit-test tasks natively, and bundled test tasks keep `--indirect-selection cautious` when `BUNDLE_TESTS = True`. Also run `make task-count` and act on the 1,000-task per-job limit per the Phase 2 task-count check; the hook additionally raises above 1,000 tasks so an over-limit job fails at validate rather than at the Jobs API. For any OTHER failure here, preserve the resolved pins, fix only clearly version-independent causes (auth, profiles, project parsing, bundle schema) directly, and otherwise stop and surface the evidence. Do NOT auto-fall-back to `dbt_task` for an unexpected failure — a dbt core/adapter incompatibility that failed `dbt parse` would recur under `dbt_task` anyway; repinning or a `dbt_task` fallback is an explicit user decision. Note the dbt pins were already resolved before this step (see Phase 3), so **skipping is allowed only when `uv`/`dbt` is unavailable at this validation step after pins resolved** — report the exact commands the user must run, same style as the offline-auth caveat in step 7; `uv` being unavailable during pin *resolution* must stop generation, not skip. Also check statically: every `python.resources` entry names an existing `resources/<module>.py` with a `load_resources` function, and each `run_job_task` reference `${resources.jobs.<key>.id}` matches the `JOB_KEY` passed to `resources.add_job`
9. **Present summary**: Show the user a final summary with file list, task count, and any MIGRATION_NOTES items requiring attention

## Resources

Progressive disclosure -- read these references as needed during each phase:

- `references/operator-mapping.md`: Complete Tier 1-4 mapping table with Airflow/DABs YAML examples for every operator type
- `references/dab-schema-reference.md`: Condensed DABs YAML schema covering all task types, triggers, clusters, variables, and dynamic value references
- `references/schedule-trigger-mapping.md`: Airflow cron-to-Quartz conversion table, preset mappings, sensor-to-trigger mappings, Airflow 3 Asset/`AssetOrTimeSchedule` scheduling with the Asset→UC-table resolution rule, default_args mappings, and Jinja variable conversions
- `references/conversion-examples.md`: 6 complete before/after examples (simple ETL, branching, sensor-triggered, multi-system, cosmos dbt factory mode, dynamic mapping + mapped task group)
- `references/airflow3-migration.md`: Airflow 3 recognition — `airflow.sdk` and `apache-airflow-providers-standard` import paths, Assets vs Datasets, asset scheduling, removed operators (`SubDagOperator`), and the recognize→safe-map→flag checklist
- `references/lakeflow-connect.md`: When to route recurring ingestion to Lakeflow Connect (vs a Jobs task), the three ingestion styles (CDC / query-based / foreign-catalog incl. Snowflake→Delta), eligibility, the DABs generation contract (`ingestion_definition`/`gateway_definition`/foreign catalog + `engine: direct`), continuous-vs-triggered orchestration, and the MIGRATION_NOTES checklist
- `references/hadoop-migration-guide.md`: HDFS path conversion, YARN Spark config cleanup, Hive-to-Unity-Catalog mapping, spark-submit detection in BashOperator/SSHOperator, Sqoop alternatives, and bulk conversion guidance for large DAGs
- `assets/templates/databricks.yml.tmpl`: Skeleton bundle configuration template
- `assets/templates/job-resource.yml.tmpl`: Skeleton job resource template
- `assets/templates/dbt-factory-resources.py.tmpl`: PyDABs hook module for factory mode (one per dbt-bearing DAG)
- `assets/templates/dbt-factory-databricks-additions.yml.tmpl`: `python:` block + `sync.include` to merge into `databricks.yml` in factory mode
- `assets/templates/dbt-pyproject.toml.tmpl`: Bundle Python deps for factory mode (databricks-bundles, databricks-dbt-factory, exact dbt-databricks + dbt-core)
- `assets/templates/dbt-Makefile.tmpl`: setup / manifest / validate / deploy targets for factory mode
- `assets/templates/dbt-profiles.yml.tmpl`: dbt profiles skeleton (host/token injected by the runner notebook)
- `assets/templates/dbt-run-command.py.tmpl`: owned runner notebook (0.3.1 base + `dbt_vars` and per-target parse cache)
- `assets/templates/dbt-tests.py.tmpl`: regression tests for the generated glue (selector exactness, `--vars` guard, fail-closed checks, pruning)
- `providers/flowx-gap-resolver/PROFILE.md`: flowx contract-v1 provider mode. Use this profile rather than the standalone DAG-to-bundle workflow when flowx supplies a fingerprint-bound `GapEnvelope` for one leaf placeholder.

## Examples

### Example: Convert a single DAG file

User says: "Convert this Airflow DAG to a Databricks Asset Bundles"
User provides: an Airflow DAG Python file (pasted or referenced via @file)

Result: Standalone DABs project with `databricks.yml`, `resources/<dag_id>_job.yml`, `src/` notebooks, and `MIGRATION_NOTES.md`.

### Example: Convert with specific target config

User says: "Migrate my_etl_dag.py to DABs targeting our dev workspace at https://my-workspace.databricks.com"

Result: DABs project with workspace URL pre-filled in `targets.dev.workspace.host`.

### Example: Convert multiple DAGs (default -- single bundle)

User says: "Convert all DAGs in the dags/ directory to Databricks Asset Bundles"

Result: A single bundle with one `databricks.yml`, a separate `resources/<dag_id>_job.yml` per DAG, source files namespaced under `src/<dag_id>/`, and a consolidated `MIGRATION_NOTES.md`. Cross-DAG `TriggerDagRunOperator` references resolve via `${resources.jobs.<name>.id}`.

### Example: Convert multiple DAGs into separate bundles (opt-in)

User says: "Convert all DAGs in the dags/ directory into separate bundles, one per DAG"

Result: One bundle directory per DAG, each with its own `databricks.yml`. Cross-DAG references use hardcoded job IDs with a note in each `MIGRATION_NOTES.md`.

### Example: Convert a dbt / cosmos DAG (factory mode)

User says: "Convert orders_analytics_dag.py to a Databricks Asset Bundle -- the dbt project is at ./dbt/orders_analytics"

Result: A two-job bundle — the YAML job with the non-dbt tasks and a `run_job_task` hop, plus a Python-generated dbt job (one task per dbt model/seed/snapshot/test) built at deploy time from the dbt manifest via PyDABs. See `examples/dbt-cosmos/` for a complete conversion.

<!-- Hardening contracts carried by this surface; tests/test_skill_contracts.py enforces the set. -->
<!-- contract: branch-datetime-dayofweek -->
<!-- contract: bundles-product-name -->
<!-- contract: constant-sensors -->
<!-- contract: dataset-or-time-schedule -->
<!-- contract: dbt-selector-arity -->
<!-- contract: file-arrival-queue -->
<!-- contract: lifecycle-retry-disclosure -->
<!-- contract: manifest-recipe-guard -->
<!-- contract: mixed-schedule-manual -->
<!-- contract: no-guessed-executable-default -->
<!-- contract: recursive-listing -->
<!-- contract: required-var-not-a-fix -->
<!-- contract: retained-sensor-poke -->
<!-- contract: soft-fail-condition-gate -->
