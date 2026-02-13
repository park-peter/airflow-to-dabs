---
name: airflow-to-dabs
description: Converts Apache Airflow DAG files into Databricks Asset Bundles (DABs) projects. Use when migrating Airflow DAGs to Databricks Lakeflow Jobs, converting Airflow operators to DABs task types, or generating databricks.yml and job resource YAML from Airflow Python files. Triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, Airflow to Lakeflow, or DABs generation from Airflow.
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
- Map 30+ Airflow operator types to their DABs task type equivalents using a tiered mapping system
- Convert Airflow cron expressions and presets to Quartz cron format
- Convert Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (file_arrival, table update)
- Extract inline Python callables, SQL strings, and bash commands into standalone source files
- Convert Airflow Jinja template variables to DABs dynamic value references
- Map `default_args` (retries, timeouts, email notifications) to DABs job/task settings
- Generate `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- Handle TaskGroups, SubDAGs, branching operators, and XCom patterns
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
6. **Flags**: Note any custom operators (subclasses of BaseOperator), XCom usage (`xcom_push`/`xcom_pull`), Airflow Variables, or Airflow Connections

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

Read `references/operator-mapping.md` for the authoritative mapping table.

For each task in the inventory:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields per the reference.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: If the branching logic is a simple comparison, use `condition_task`. If complex, use a two-step pattern (notebook + condition).
   - `DummyOperator`/`EmptyOperator`: Remove from the task list. Rewire `depends_on` so downstream tasks point to the dummy's upstream tasks.
   - `SubDagOperator`/`TaskGroup`: Flatten into the parent job with prefixed task keys, or extract to a separate job via `run_job_task`.
3. **Tier 3 (sensors)**: Convert to job-level triggers. Read `references/schedule-trigger-mapping.md`.
   - File sensors -> `trigger.file_arrival`
   - Table/SQL sensors -> `trigger.table`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table`
   - Remove sensor tasks from the task list (they become job-level configuration).
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add entry to `MIGRATION_NOTES.md`.

For schedule conversion, read `references/schedule-trigger-mapping.md`:
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week numbering)
- Convert Airflow presets (`@daily`, `@hourly`, etc.) to Quartz equivalents
- Extract timezone from `default_args` or DAG `start_date`

### Phase 3: Generate the DABs Project

Produce the following output files. Read `references/dab-schema-reference.md` for the complete YAML schema. Use `assets/templates/databricks.yml.tmpl` and `assets/templates/job-resource.yml.tmpl` as starting skeletons.

#### Output Modes

**Multi-DAG (default):** When converting multiple DAGs, produce a **single bundle** with one `databricks.yml` and a separate job resource file per DAG under `resources/`. This is the default because it enables cross-job references via `${resources.jobs.<name>.id}` and allows a single `databricks bundle deploy`.

**Single-DAG:** When converting one DAG, produce a standalone bundle directory.

**Split bundles (opt-in):** If the user explicitly requests separate bundles per DAG (e.g., "create a separate bundle for each DAG"), produce one bundle directory per DAG. Cross-DAG `TriggerDagRunOperator` references will require hardcoded job IDs and a note in MIGRATION_NOTES.md.

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

**File generation rules:**

1. **`databricks.yml`**: For multi-DAG, derive `bundle.name` from a user-provided name or the parent directory name. For single-DAG, derive from `dag_id` (kebab-case). Include `variables` for `spark_version`, `node_type_id`, `warehouse_id`. Define `dev` and `prod` targets. Use `include: - resources/*.yml` to pull in all job definitions.

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
   - Add `dbutils.widgets.text()` and `dbutils.widgets.get()` for each `base_parameter`
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

### Phase 4: Review and Validate

After generating all files:

1. **Dependency check**: Verify every `depends_on` reference points to a valid `task_key` in the same job
2. **Orphan check**: Verify no tasks are unreachable (disconnected from the DAG)
3. **Task type check**: Verify each task has exactly one task type field
4. **Cluster check**: Verify every task that requires compute has `job_cluster_key`, `existing_cluster_id`, or `new_cluster` (except `condition_task`, `run_job_task`, and similar clusterless tasks)
5. **Parameter check**: Verify all `{{job.parameters.*}}` references have corresponding entries in the job `parameters` list
6. **Present summary**: Show the user a final summary with file list, task count, and any MIGRATION_NOTES items requiring attention

## Resources

Progressive disclosure -- read these references as needed during each phase:

- `references/operator-mapping.md`: Complete Tier 1-4 mapping table with Airflow/DABs YAML examples for every operator type
- `references/dab-schema-reference.md`: Condensed DABs YAML schema covering all task types, triggers, clusters, variables, and dynamic value references
- `references/schedule-trigger-mapping.md`: Airflow cron-to-Quartz conversion table, preset mappings, sensor-to-trigger mappings, default_args mappings, and Jinja variable conversions
- `references/conversion-examples.md`: 4 complete before/after examples (simple ETL, branching, sensor-triggered, multi-system)
- `references/hadoop-migration-guide.md`: HDFS path conversion, YARN Spark config cleanup, Hive-to-Unity-Catalog mapping, spark-submit detection in BashOperator/SSHOperator, Sqoop alternatives, and bulk conversion guidance for large DAGs
- `assets/templates/databricks.yml.tmpl`: Skeleton bundle configuration template
- `assets/templates/job-resource.yml.tmpl`: Skeleton job resource template

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
