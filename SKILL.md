---
name: airflow-to-dabs
description: Converts Apache Airflow DAG files into Databricks Asset Bundles (DABs) projects. Use when migrating Airflow DAGs to Databricks Lakeflow Jobs, converting Airflow operators to DABs task types, converting dbt-on-Airflow workloads (astronomer-cosmos DbtDag/DbtTaskGroup, dbt operators) to per-model Lakeflow jobs, or generating databricks.yml and job resource YAML from Airflow Python files. Triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, Airflow to Lakeflow, cosmos or dbt DAG migration, or DABs generation from Airflow.
version: 1.0.0
author: park-peter
repository: https://github.com/park-peter/airflow-to-dabs
keywords:
  - airflow
  - databricks
  - migration
  - lakeflow
  - dabs
  - asset-bundles
  - dag-conversion
---

# Airflow to Databricks Asset Bundles Converter

Converts Apache Airflow DAG files into complete Databricks Asset Bundles (DABs) projects, producing `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files ready for `databricks bundle deploy`.

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

1. **DAG metadata**: `dag_id`, `schedule_interval`/`schedule`, `default_args`, `catchup`, `tags`, `params`
2. **Task inventory**: For each task, capture:
   - `task_id` and operator class (e.g., `PythonOperator`, `BashOperator`)
   - Operator-specific parameters (`python_callable`, `bash_command`, `sql`, `application`, `json`, etc.)
   - `op_kwargs`, `op_args`, `params`, `templates_dict`
3. **Dependency graph**: Extract `>>` / `<<` chains and `set_upstream`/`set_downstream` calls to build the task DAG
4. **Sensors**: Identify sensor tasks and their trigger conditions (S3 path, table name, external DAG, time)
5. **TaskGroups / SubDAGs**: Identify grouped tasks and their internal structure
6. **Flags**: Note any custom operators (subclasses of BaseOperator), XCom usage (`xcom_push`/`xcom_pull`), Airflow Variables, Airflow Connections, dynamic task mapping (`expand`), and custom timetable/dataset schedules
7. **dbt workloads**: Detect and flag dbt usage — these are subject to the dbt conversion decision point in Phase 2:
   - cosmos imports: `from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig`, `cosmos.profiles.*` mappings, `cosmos.operators.*`
   - dbt CLI operator families: `airflow_dbt.operators.dbt_operator` and `airflow_dbt_python.operators.dbt` (`DbtRunOperator`, `DbtTestOperator`, `DbtSeedOperator`, `DbtSnapshotOperator`, `DbtDepsOperator`, `DbtBuildOperator`)
   - dbt Cloud provider: `airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator`, `DbtCloudJobRunSensor`
   - `BashOperator`/`SSHOperator` commands matching `dbt (deps|seed|snapshot|run|test|build|docs)`
   - Capture: `project_dir`/`dbt_project_path`, `profiles_dir`/profile mapping, `target`, `select`/`exclude`/`models`, `vars`, `full_refresh`, and whether the dbt project source or a `manifest.json` is available to the conversion
   - Summary-table convention: a cosmos `DbtDag`/`DbtTaskGroup` appears as one row with DABs Task Type `dbt factory job (or dbt_task)`, Tier 2, note "decision point -- see Phase 2"

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

For each task in the inventory:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields per the reference.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: If the branching logic is a simple comparison, use `condition_task`. If complex, use a two-step pattern (notebook + condition).
   - `DummyOperator`/`EmptyOperator`: Remove from the task list. Rewire `depends_on` so downstream tasks point to the dummy's upstream tasks.
   - `SubDagOperator`/`TaskGroup`: Flatten into the parent job with prefixed task keys, or extract to a separate job via `run_job_task`.
3. **Tier 3 (sensors)**: Convert to job-level triggers. Read `references/schedule-trigger-mapping.md` in this skill's directory.
   - File sensors -> `trigger.file_arrival`
   - Table/SQL sensors -> `trigger.table_update`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table_update`
   - Remove sensor tasks from the task list (they become job-level configuration).
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add entry to `MIGRATION_NOTES.md`.

**dbt decision point**: For any dbt workload flagged in Phase 1, **default to dbt factory mode** — read the dbt conversion decision point in `references/operator-mapping.md` (Tier 1 dbt CLI operators section) for the full rules and the cosmos section in Tier 2 for the generated artifacts. Fall back to a single `dbt_task` only on a disqualifier: dbt project source/manifest unavailable, selector subsetting not confirmed by the user, dbt Cloud target (Tier 4), or explicit user opt-out. Surface the choice and its toolchain implications (PyDABs `python:` block, `pyproject.toml` + `.venv`, `Makefile`, `uv`) in the Phase 1 summary so the user can override before Phase 3.

For schedule conversion, read `references/schedule-trigger-mapping.md` in this skill's directory:
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week numbering, and normalize Sunday `0/7 -> 1`)
- Convert Airflow presets (`@daily`, `@hourly`, etc.) to Quartz equivalents
- Extract timezone from `default_args` or DAG `start_date`
- Convert `@continuous` to job-level `continuous` mode
- For dataset/timetable schedules, map to `trigger.table_update` when deterministic, otherwise flag in `MIGRATION_NOTES.md`

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
  pyproject.toml                  # pins databricks-bundles, databricks-dbt-factory, dbt-databricks
  Makefile                        # setup / manifest / validate / deploy
  resources/
    __init__.py                   # empty package marker
    <dag_id>_job.yml              # YAML job with run_job_task hop
    <dag_id>_dbt_job.py           # PyDABs hook (one per dbt-bearing DAG)
  dbt_profiles/
    profiles.yml
  dbt_project.yml  models/  seeds/  ...   # the dbt project, colocated at bundle root
  target/manifest.json            # produced by `make manifest` (dbt parse)
  dbt_serverless_env.yaml         # written at validate/deploy time by the hook
  src/
    run_dbt_command.py            # runner notebook, extracted from the pinned package
```

**File generation rules:**

1. **`databricks.yml`**: For multi-DAG, derive `bundle.name` from a user-provided name or the parent directory name. For single-DAG, derive from `dag_id` (kebab-case). Include `variables` for `spark_version`, `node_type_id`, `warehouse_id`. Define `dev` and `prod` targets. Use `include: - resources/*.yml` to pull in all job definitions. In factory mode, merge in `assets/templates/dbt-factory-databricks-additions.yml.tmpl` (the `python:` block — one `resources.<dag_id>_dbt_job:load_resources` entry per dbt-bearing DAG — and `sync.include`).

2. **`resources/<dag_id>_job.yml`**: One job resource file per DAG, each containing:
   - `schedule` or `trigger` from Phase 2
   - `email_notifications` from `default_args.email`
   - `parameters` from DAG `params` and Jinja variables like `{{ ds }}`
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
   - Replace `{{ ds }}` with `{{job.parameters.run_date}}`
   - Replace `{{ params.x }}` with `{{job.parameters.x}}`

5. **`MIGRATION_NOTES.md`**: A single consolidated file documenting:
   - Tier 4 operators flagged for manual review
   - XCom patterns that need conversion to `dbutils.jobs.taskValues`
   - Airflow Connections that need Databricks secrets or UC connections
   - Airflow Variables that need bundle variables or job parameters
   - Any `catchup`, `depends_on_past`, `sla` settings that have no DABs equivalent
   - Sensor-to-trigger conversions with notes on external location setup
   - **Cross-DAG dependency map**: which jobs reference other jobs via `run_job_task`, with resolved `${resources.jobs...}` substitutions
   - **Factory mode (when active)**: selector semantics (whole-manifest explosion vs any Airflow-side `--select`/`--exclude`), serverless-only note with the classic-cluster manual variation (`job_cluster_key` in `DbtTaskOptions`), a task-count warning for very large manifests (a job holds up to 1,000 tasks — split by tag or stay on single `dbt_task`), `dbt_profiles/profiles.yml` values to fill (`<WAREHOUSE_ID>`, catalog/schema), and the `make setup && make manifest` prerequisite before the first deploy

6. **Factory-mode artifacts** (per dbt-bearing DAG, when the Phase 2 decision point selects factory mode):
   - `resources/<dag_id>_dbt_job.py` from `assets/templates/dbt-factory-resources.py.tmpl` (replace `<DAG_ID>`; adjust `MANIFEST_PATH` if the dbt project is not at the bundle root)
   - `resources/__init__.py` (empty), `pyproject.toml` from `dbt-pyproject.toml.tmpl`, `Makefile` from `dbt-Makefile.tmpl`, `dbt_profiles/profiles.yml` from `dbt-profiles.yml.tmpl` (profile name must match `profile:` in the customer's `dbt_project.yml`)
   - Copy the customer's dbt project to the bundle root (or point `MANIFEST_PATH`/`--project-dir` at its location)
   - In the DAG's YAML job, place a `run_job_task` with `job_id: ${resources.jobs.<dag_id>_dbt_job.id}` where the dbt workload sat
   - `.gitignore` additions: `.venv/`, `logs/`, `dbt_packages/`, `target/*` with `!target/manifest.json`

### Phase 4: Review and Validate

After generating all files:

1. **Dependency check**: Verify every `depends_on` reference points to a valid `task_key` in the same job
2. **Orphan check**: Verify no tasks are unreachable (disconnected from the DAG)
3. **Task type check**: Verify each task has exactly one task type field
4. **Cluster check**: Verify every task that requires compute has `job_cluster_key`, `existing_cluster_id`, `new_cluster`, or a serverless `environment_key` (except `condition_task`, `run_job_task`, and similar clusterless tasks)
5. **Parameter check**: Verify all `{{job.parameters.*}}` references have corresponding entries in the job `parameters` list
6. **Bundle schema check**: Run `databricks bundle validate -t <target>` and fix schema warnings/errors (if auth is unavailable, run `databricks bundle schema` validation checks offline and report the limitation)
7. **Factory-mode validation** (when active): Run `make setup` (creates `.venv` via `uv`), then `make manifest` (`dbt deps` + `dbt parse` — no warehouse connection needed), then `databricks bundle validate -t dev`. Validation executes the PyDABs hook, so it requires the venv and `target/manifest.json`. If `uv` or `dbt` is unavailable, skip and report the exact commands the user must run — same style as the offline-auth caveat in step 6. Also check statically: every `python.resources` entry names an existing `resources/<module>.py` with a `load_resources` function, and each `run_job_task` reference `${resources.jobs.<key>.id}` matches the `JOB_KEY` passed to `resources.add_job`
8. **Present summary**: Show the user a final summary with file list, task count, and any MIGRATION_NOTES items requiring attention

## Resources

Progressive disclosure -- read these references as needed during each phase:

- `references/operator-mapping.md`: Complete Tier 1-4 mapping table with Airflow/DABs YAML examples for every operator type
- `references/dab-schema-reference.md`: Condensed DABs YAML schema covering all task types, triggers, clusters, variables, and dynamic value references
- `references/schedule-trigger-mapping.md`: Airflow cron-to-Quartz conversion table, preset mappings, sensor-to-trigger mappings, default_args mappings, and Jinja variable conversions
- `references/conversion-examples.md`: 5 complete before/after examples (simple ETL, branching, sensor-triggered, multi-system, cosmos dbt factory mode)
- `references/hadoop-migration-guide.md`: HDFS path conversion, YARN Spark config cleanup, Hive-to-Unity-Catalog mapping, spark-submit detection in BashOperator/SSHOperator, Sqoop alternatives, and bulk conversion guidance for large DAGs
- `assets/templates/databricks.yml.tmpl`: Skeleton bundle configuration template
- `assets/templates/job-resource.yml.tmpl`: Skeleton job resource template
- `assets/templates/dbt-factory-resources.py.tmpl`: PyDABs hook module for factory mode (one per dbt-bearing DAG)
- `assets/templates/dbt-factory-databricks-additions.yml.tmpl`: `python:` block + `sync.include` to merge into `databricks.yml` in factory mode
- `assets/templates/dbt-pyproject.toml.tmpl`: Bundle Python deps for factory mode (databricks-bundles, databricks-dbt-factory, dbt-databricks)
- `assets/templates/dbt-Makefile.tmpl`: setup / manifest / validate / deploy targets for factory mode
- `assets/templates/dbt-profiles.yml.tmpl`: dbt profiles skeleton (host/token injected by the runner notebook)

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
