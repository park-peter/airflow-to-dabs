# Airflow to Databricks Asset Bundles (DABs) Converter

When the user provides an Airflow DAG file or asks about Airflow-to-Databricks migration, follow the workflow below to convert it into a complete Databricks Asset Bundles (DABs) project.

## Output

Produce a deployable bundle: `databricks.yml`, `resources/*.yml` job definitions, extracted `src/` source files, and a `MIGRATION_NOTES.md` — ready for `databricks bundle deploy`.

## Capabilities

- Parse Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Map 40+ Airflow operator types (including all Databricks provider operators) to DABs task type equivalents using a tiered mapping system
- Convert Airflow cron expressions and presets to Quartz cron format
- Convert Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (file_arrival, table_update)
- Extract inline Python callables, SQL strings, and bash commands into standalone source files
- Convert Jinja template variables to DABs dynamic value references
- Map `default_args` (retries, timeouts, email notifications) to DABs job/task settings
- dbt factory mode (default for dbt workloads): convert dbt workloads — including astronomer-cosmos `DbtDag`/`DbtTaskGroup` — into a separate Lakeflow job with one task per dbt model/seed/snapshot/test, generated at deploy time from the dbt manifest via PyDABs and `databricks-dbt-factory`; single `dbt_task` as fallback
- Handle TaskGroups, SubDAGs, branching operators, Airflow dynamic task mapping, and XCom patterns
- Hadoop/HDFS migration: detect `spark-submit` in BashOperator/SSHOperator, clean up YARN Spark configs, map HDFS paths, convert HiveQL to Spark SQL, handle Sqoop alternatives

## Workflow

### Phase 1: Parse the Airflow DAG

Read the provided Airflow DAG file(s) and extract:

1. **DAG metadata**: `dag_id`, `schedule_interval`/`schedule`, `default_args`, `catchup`, `tags`, `params`
2. **Task inventory**: For each task — `task_id`, operator class, operator-specific parameters (`python_callable`, `bash_command`, `sql`, `application`, `json`, etc.), `op_kwargs`, `op_args`
3. **Dependency graph**: `>>` / `<<` chains, `set_upstream`/`set_downstream` calls
4. **Sensors**: Sensor tasks and their trigger conditions
5. **TaskGroups / SubDAGs**: Grouped tasks and internal structure
6. **Flags**: Custom operators, XCom usage, Airflow Variables, Airflow Connections, dynamic task mapping (`expand`), and custom timetable/dataset schedules
7. **dbt workloads**: cosmos imports (`DbtDag`, `DbtTaskGroup`, `ProjectConfig`, `ProfileConfig`, `RenderConfig`), dbt CLI operator families (`airflow_dbt`, `airflow_dbt_python`), dbt Cloud provider operators, and `BashOperator`/`SSHOperator` running `dbt (deps|seed|snapshot|run|test|build)`. Capture project_dir, profiles, target, selectors, vars, and whether the dbt project source / `manifest.json` is available. Cosmos groups appear as one summary row (Tier 2, "decision point — see the dbt rules below").

Present a summary table before proceeding:

```
| Task ID           | Operator                | DABs Task Type      | Tier | Notes              |
|-------------------|-------------------------|---------------------|------|--------------------|
| extract_data      | PythonOperator          | notebook_task       | 1    |                    |
| check_env         | BranchPythonOperator    | condition_task      | 2    | Simple equality    |
| wait_for_file     | S3KeySensor             | trigger.file_arrival| 3    | Becomes job trigger|
| custom_step       | MyCustomOperator        | notebook_task       | 4    | MANUAL REVIEW      |
```

### Phase 2: Map Operators to DABs Task Types

Use the embedded mapping rules below as authoritative in Copilot (do not rely on external file references).

#### Embedded operator mapping (authoritative)

<!-- Snapshot of references/operator-mapping.md — keep in sync on updates -->

**Tier 1 (direct mappings):**

- `PythonOperator` -> `spark_python_task` (extract callable into `src/<task_id>.py`)
- `BashOperator` -> `spark_python_task` with a thin wrapper calling `subprocess.run(...)`, or `notebook_task` for trivial commands
- `SparkSubmitOperator` -> `spark_python_task` or `spark_jar_task` (translate `spark-submit` args; remove YARN-only flags)
- `DatabricksNotebookOperator` -> `notebook_task`
- `DatabricksSqlOperator` / `DatabricksSQLStatementsOperator` / `SQLExecuteQueryOperator` -> `sql_task`
- `DatabricksCopyIntoOperator` -> `sql_task` using `COPY INTO`
- `DbtOperator` / `DbtRunOperator` / `DbtTestOperator` / `DbtSeedOperator` -> dbt factory mode (default) or single `dbt_task` (fallback — see dbt conversion rules below)
- `TriggerDagRunOperator` -> `run_job_task` with `job_id: ${resources.jobs.<target>.id}` when target is in the same bundle

**Tier 2 (semantic mappings):**

- `BranchPythonOperator` / `ShortCircuitOperator`:
  - Simple equality/threshold checks -> `condition_task`
  - Complex branching logic -> `spark_python_task` + downstream `condition_task`
- `KubernetesPodOperator` / `DockerOperator`:
  - Use a single-node job cluster with `docker_image` (Databricks Container Services)
  - Include valid single-node settings:
    - `num_workers: 0`
    - `spark_conf.spark.databricks.cluster.profile: singleNode`
    - `spark_conf.spark.master: local[*]`
    - `custom_tags.ResourceClass: SingleNode`
  - Use dedicated/single-user style access mode (`data_security_mode: SINGLE_USER`)
  - Image requirements: include DCS prerequisites and start as root (non-root startup can fail with `CONTAINER_LAUNCH_FAILURE`)
- `TaskGroup` / `SubDagOperator` -> flatten tasks with prefixed `task_key`s or extract to `run_job_task`
- cosmos `DbtDag` / `DbtTaskGroup` -> dbt factory mode (the only faithful mapping — cosmos renders dbt models as Airflow tasks at runtime from `manifest.json`, so the group is statically unparseable; swap the generator instead of translating tasks)
- `DummyOperator` / `EmptyOperator` -> remove task and rewire dependencies

**Tier 3 (sensor -> trigger mappings):**

- `S3KeySensor` / `HdfsSensor` / `FileSensor` -> job `trigger.file_arrival`
- `DatabricksSqlSensor` / `DatabricksPartitionSensor` / `DatabricksSQLStatementsSensor` / `SqlSensor` -> job `trigger.table_update`
- `ExternalTaskSensor` -> `run_job_task` or `depends_on` equivalent when deterministic
- Remove converted sensors from the task list (they become job-level trigger config)

**Tier 4 (unsupported fallback):**

- Unknown/custom operators -> fallback `notebook_task` and add a manual-review entry in `MIGRATION_NOTES.md`
- `DbtCloudRunJobOperator` / `DbtCloudJobRunSensor` -> `notebook_task` calling the dbt Cloud API (dbt Cloud owns orchestration; factory mode applies only if the dbt project itself migrates to Databricks)
- Include explicit notes for XCom-heavy logic, unsupported orchestration features, and any non-deterministic schedule/timetable constructs

The mapping process follows four tiers:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: Simple comparison -> `condition_task`. Complex -> notebook + condition.
   - `KubernetesPodOperator`/`DockerOperator`: Use Databricks Container Services with `docker_image` on a single-node cluster. Decision depends on whether the image is Python-based, JVM-based, or another runtime.
   - `DummyOperator`/`EmptyOperator`: Remove, rewire `depends_on`.
   - `SubDagOperator`/`TaskGroup`: Flatten with prefixed keys, or extract via `run_job_task`.
3. **Tier 3 (sensors)**: Convert to job-level triggers.
   - File sensors -> `trigger.file_arrival`
   - Table/SQL sensors -> `trigger.table_update`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table_update`
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add to `MIGRATION_NOTES.md`.

#### dbt conversion rules (embedded)

**Default to dbt factory mode for every dbt workload**: generate a separate, Python-defined Lakeflow job with one task per dbt object (model/seed/snapshot/test), built at deploy time from `target/manifest.json` by a PyDABs hook using the `databricks-dbt-factory` PyPI package. The DAG's YAML job triggers it with `run_job_task: {job_id: ${resources.jobs.<dag_id>_dbt_job.id}}` where the dbt workload sat. This gives per-model observability, retry-from-failed-model, parallelism, and tests gating downstream models.

Fall back to a single `dbt_task` only when: (a) the dbt project source / `manifest.json` is unavailable; (b) the invocation subsets the project (`--select`/`--exclude`, cosmos `RenderConfig(select=...)`) and the user does not confirm whole-project runs — **selector caveat**: factory mode explodes the entire manifest, so converting silently would change semantics; (c) the target is dbt Cloud; or (d) the user explicitly wants minimal toolchain change. Record the decision in `MIGRATION_NOTES.md`.

Factory-mode artifacts (per dbt-bearing DAG): `resources/<dag_id>_dbt_job.py` (PyDABs hook: reads the manifest, builds tasks with `DbtTaskOptions(task_type=NOTEBOOK, environment_key="Default", notebook_path="src/run_dbt_command.py", project_directory="..", profiles_directory="dbt_profiles")`, writes `dbt_serverless_env.yaml` — `environment_version: "5"` pinning the venv's dbt-databricks — and extracts the runner notebook from the installed package, both idempotently); `resources/__init__.py` (empty); `pyproject.toml` (pins `databricks-bundles`, `databricks-dbt-factory`, `dbt-databricks`); `Makefile` (`setup`: uv sync --dev; `manifest`: dbt deps + dbt parse — no warehouse needed; `validate`/`deploy` depend on manifest); `dbt_profiles/profiles.yml` (dev/prod outputs named after bundle targets; host/token injected by the runner via `DBT_HOST`/`DBT_ACCESS_TOKEN` env vars); the dbt project copied to the bundle root; `databricks.yml` gains `python: {venv_path: .venv, resources: ["resources.<dag_id>_dbt_job:load_resources"]}` and `sync.include: [dbt_serverless_env.yaml, target/partial_parse.msgpack]`; `.gitignore` gains `.venv/`, `logs/`, `dbt_packages/`, `target/*` with `!target/manifest.json`.

For schedule conversion:
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week, normalize Sunday `0/7 -> 1`)
- Convert presets (`@daily`, `@hourly`) to Quartz equivalents
- Extract timezone from `default_args` or `start_date`

For Hadoop/on-prem migrations:
- Convert HDFS paths to Unity Catalog volumes or cloud storage
- Remove YARN-specific Spark configs, translate to Databricks equivalents
- Map Hive `database.table` to `catalog.schema.table`
- Detect `spark-submit` in BashOperator/SSHOperator and extract to proper task types

### Phase 3: Generate the DABs Project

#### Output Modes

**Multi-DAG (default):** Single bundle with one `databricks.yml` and a separate `resources/<dag_id>_job.yml` per DAG. Enables cross-job references via `${resources.jobs.<name>.id}`.

**Single-DAG:** Standalone bundle directory for one DAG.

**Split bundles (opt-in):** Separate bundle per DAG, only when user explicitly requests it.

**dbt factory mode (default for dbt workloads):** Orthogonal to the above. Per dbt-bearing DAG, generate a second Python-defined job (one task per dbt node) and place a `run_job_task` in the YAML job where the dbt workload sat — see the dbt conversion rules above for the artifact list.

#### File Generation Rules

1. **`databricks.yml`**: Bundle name from user input or directory name. Include `variables` for `spark_version`, `node_type_id`, `warehouse_id`. Define `dev`/`prod` targets. Use `include: - resources/*.yml`.
2. **`resources/<dag_id>_job.yml`**: One per DAG with schedule/trigger, email_notifications, parameters, job_clusters, tasks with `depends_on`. Cross-DAG `TriggerDagRunOperator` -> `${resources.jobs.<target>.id}`.
3. **`src/` notebooks**: `# Databricks notebook source` header, `dbutils.widgets` for parameters, extracted callable body, replaced Airflow imports.
4. **`src/*.sql`**: Extracted SQL with `{{ ds }}` -> `{{job.parameters.run_date}}`, `{{ params.x }}` -> `{{job.parameters.x}}`.
5. **`MIGRATION_NOTES.md`**: Tier 4 items, XCom patterns, Connections needing secrets, Variables needing parameters, settings without DABs equivalents, cross-DAG dependency map. Factory mode: selector semantics, serverless-only note (classic via `job_cluster_key` in `DbtTaskOptions`), task-count warning for very large manifests, profiles values to fill, `make setup && make manifest` prerequisite.

### Phase 4: Review and Validate

1. **Dependency check**: Every `depends_on` references a valid `task_key`
2. **Orphan check**: No unreachable tasks
3. **Task type check**: Each task has exactly one task type field
4. **Cluster check**: Compute-requiring tasks have `job_cluster_key`, `existing_cluster_id`, `new_cluster`, or a serverless `environment_key`
5. **Parameter check**: All `{{job.parameters.*}}` have corresponding entries in `parameters`
6. **Factory-mode validation** (when active): `make setup` -> `make manifest` -> `databricks bundle validate -t dev` (validate executes the PyDABs hook; needs `.venv` + `target/manifest.json`). If `uv`/`dbt` is unavailable, skip and report the exact commands. Statically check `python.resources` entries resolve and `run_job_task` references match the `JOB_KEY`s.
7. **Present summary**: File list, task count, MIGRATION_NOTES items
