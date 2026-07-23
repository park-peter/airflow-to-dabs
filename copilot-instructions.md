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
2. **Task inventory**: For each task — `task_id`, operator class, operator-specific parameters (`python_callable`, `bash_command`, `sql`, `application`, `json`, etc.), `op_kwargs`, `op_args`. Recognize both Airflow 2 imports (`airflow.operators.*`/`airflow.sensors.*`) and **Airflow 3** imports (`airflow.sdk.*`, `airflow.providers.standard.{operators,sensors}.*`) — same mappings, different import paths; never drop a task whose import path is unrecognized (see the Airflow 3 rules below).
3. **Dependency graph**: `>>` / `<<` chains, `set_upstream`/`set_downstream` calls, **and TaskFlow call wiring** (`b(a())` implies `depends_on`; `.override(task_id=...)` sets the key) — merge classic + TaskFlow edges
4. **Sensors**: Sensor tasks and their trigger conditions
5. **TaskGroups / SubDAGs**: Grouped tasks and internal structure, including **mapped task groups** (`@task_group.expand()`)
6. **Flags**: Custom operators, XCom usage, Airflow Variables, Airflow Connections, dynamic task mapping (`.expand()`/`.expand_kwargs()`), **Airflow 3 `Asset` scheduling** (`schedule=[Asset(...)]`, boolean asset expressions, `AssetOrTimeSchedule`), and custom timetable/dataset schedules
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

**Source-aware classification (apply before the Tier tables).** Operator class alone does not fix the mapping — the connection does. Resolve `operator -> connection type -> operation intent -> data direction -> destination contract -> strategy`: Databricks SQL connection -> `sql_task`; remote federatable DB with read-only SELECT -> Lakehouse Federation `sql_task` over a foreign catalog; remote DML/DDL -> connector notebook or migrate the target to Delta; recurring source->Delta ingestion from an eligible source -> Lakeflow Connect ingestion pipeline (see dbt/Lakeflow Connect rules); files in cloud storage -> Auto Loader; unsupported source -> notebook/SDK + flag. Federatable sources: MySQL, PostgreSQL, SQL Server, Oracle, Teradata, Redshift, Snowflake, BigQuery, Synapse, Salesforce Data 360, Databricks (Athena/Trino/Presto are NOT federatable). **Fail-closed connection resolution**: auto-route only from operator/provider certainty, the actual sanitized Airflow `conn_type`, or an explicit user `conn_id -> {type, target}` mapping; a `conn_id` name/host is a hint only; unresolved connections are manual review; never inline credentials (use a UC connection or `dbutils.secrets`).

#### Embedded operator mapping (authoritative)

<!-- Snapshot of references/operator-mapping.md — keep in sync on updates -->

**Tier 1 (direct mappings):**

- `PythonOperator` / `@task` (TaskFlow) -> `notebook_task` / `spark_python_task` (extract callable into `src/<task_id>.py`). TaskFlow dataflow: a `@task` return -> `dbutils.jobs.taskValues.set(key="return_value", value=...)`, consumed via `{{tasks.<key>.values.return_value}}`; `multiple_outputs=True` (or a dict return) -> one task value per dict key. `.override(task_id=...)` sets the task key. Variant decorators: `@task.branch`/`@task.short_circuit` -> `condition_task`; `@task.bash` -> BashOperator rules; `@task.virtualenv`/`@task.external_python` -> `notebook_task` + env note; `@task.sensor` -> Tier-3 trigger only if a root sensor with unused `PokeReturnValue`, else flag; `@task.run_if`/`@task.skip_if` -> `run_if` only for status-equivalent predicates, else `condition_task` or flag; `@setup`/`@teardown` -> flag (no native lifecycle)
- `BashOperator` -> `spark_python_task` with a thin wrapper calling `subprocess.run(...)`, or `notebook_task` for trivial commands
- `SparkSubmitOperator` -> `spark_python_task` or `spark_jar_task` (translate `spark-submit` args; remove YARN-only flags)
- `DatabricksNotebookOperator` -> `notebook_task`
- `DatabricksSqlOperator` / `DatabricksSQLStatementsOperator` -> `sql_task`
- `SQLExecuteQueryOperator` / `PostgresOperator` / `MySqlOperator` -> `sql_task` **only for a Databricks SQL connection** (apply the source-aware classification above); a remote federatable DB read-only SELECT -> Lakehouse Federation `sql_task` over a foreign catalog; remote DML/DDL -> connector notebook or migrate target; unresolved connection -> flag (do not assume `sql_task`)
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
- `TaskGroup` / `SubDagOperator` -> flatten tasks with prefixed `task_key`s or extract to `run_job_task` (`SubDagOperator` is removed in Airflow 3)
- Dynamic task mapping (`.expand()` / `.expand_kwargs()`) -> `for_each_task` with `{{input}}` (whole element) / `{{input.<key>}}` (field) in the nested task; `.partial()` kwargs -> constant `base_parameters`. Map only when the collection is a literal or an upstream task-value/job-param ref; choose the `inputs` transport by size (JSON-array literal ≤5,000 chars, task-value ref ≤48 KiB, job-param ref ≤10,000 chars — all JSON-serializable). Flag multi-arg Cartesian products, chained mapping, mapped-output reduction (downstream can't read for-each iteration outputs — persist per-iteration then aggregate separately), and non-deterministic collections
- Mapped task group (`@task_group.expand()`) -> `for_each_task` whose nested task is a `run_job_task` targeting a **child job** that holds the group's subgraph (a `for_each_task` nests exactly one task, not a subgraph). Pass the element via `job_parameters: {..: "{{input}}"}`. Set parent `for_each_task.concurrency`, raise the child job's `max_concurrent_runs` to match, and set `queue: {enabled: true}` on the child job (bundle/API jobs don't inherit the UI's default-on queueing — excess iterations are otherwise skipped). Keep total Run Job nesting ≤ 3 levels
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

**Lakeflow Connect ingestion (decision point, not a generic fallback).** For a **recurring ingestion** task from an **eligible** source, emit a `resources.pipelines` entry with an `ingestion_definition` instead of a hand-rolled notebook. Styles: CDC (MySQL/PostgreSQL/SQL Server), query-based direct (Oracle/Teradata/…), and query-based **foreign-catalog** for federated sources (Snowflake/BigQuery/Redshift/Synapse via `ingest_from_uc_foreign_catalog: true` + `source_catalog/schema/table`). A foreign catalog is a `resources.catalogs` entry (`connection_name` + source-specific `options` e.g. `options.database`) and requires `bundle.engine: direct`; the UC connection is a manual prerequisite (not a bundle resource). `ingestion_definition` can't combine with a normal pipeline's `libraries`/`schema`/`target`/`catalog`. Orchestration: triggered pipeline -> `pipeline_task` at the original position; continuous (streaming Kafka/RabbitMQ, or any continuous-only connector) -> standalone pipeline + downstream job-level `trigger.table_update`. Gateway CDC uses a separate `gateway_definition` pipeline joined by `ingestion_gateway_id` and is **Private Preview / `doNotSuggest`** — only with confirmed enrollment; never the default. Eligibility (all): recurring; connector exists; Connect creates+owns the destination table (fails if it already exists); objects/cursor/keys/deletes representable; UC connection + networking known. Files from cloud storage -> Auto Loader, NOT Connect. Verify a connector's current release state and run mode; do not hardcode GA status or assume triggered-vs-continuous.

The mapping process follows four tiers:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: Simple comparison -> `condition_task`. Complex -> notebook + condition.
   - `KubernetesPodOperator`/`DockerOperator`: Use Databricks Container Services with `docker_image` on a single-node cluster. Decision depends on whether the image is Python-based, JVM-based, or another runtime.
   - `DummyOperator`/`EmptyOperator`: Remove, rewire `depends_on`.
   - `SubDagOperator`/`TaskGroup`: Flatten with prefixed keys, or extract via `run_job_task` (`SubDagOperator` removed in Airflow 3).
   - **Dynamic task mapping** / **mapped task group**: see the embedded Tier-2 rules above (`for_each_task`; mapped groups go `for_each_task` -> `run_job_task` -> child job with `concurrency`/`max_concurrent_runs`/`queue` set and Run Job nesting ≤ 3).
3. **Tier 3 (sensors)**: Convert to job-level triggers.
   - File sensors -> `trigger.file_arrival`
   - Table/SQL sensors -> `trigger.table_update`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table_update`
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add to `MIGRATION_NOTES.md`.

#### dbt conversion rules (embedded)

**Default to dbt factory mode for every dbt workload**: generate a separate, Python-defined Lakeflow job with one task per dbt object, built at deploy time from `target/<target>/manifest.json` by a PyDABs hook using the `databricks-dbt-factory` PyPI package. Enable only factories matching the union of detected dbt commands (run->model, seed->seed, snapshot->snapshot, test->test, build->all; deps/docs-only -> not factory-eligible); prune depends_on refs to omitted node types after generation. The DAG's YAML job triggers it with `run_job_task: {job_id: ${resources.jobs.<dag_module>_dbt_job.id}, job_parameters: {dbt_vars: "{{job.parameters.dbt_vars}}"}}` where the dbt workload sat (`<dag_module>` = dag_id sanitized to a Python identifier). This gives per-model observability, retry-from-failed-model, parallelism, and tests gating downstream models.

Fall back to a single `dbt_task` only when: (a) the dbt project SOURCE is unavailable (a manifest alone is not enough — runtime needs the project files; source without a manifest is fine, `make manifest` generates one); (b) the invocation subsets the project (`--select`/`--exclude`, cosmos `RenderConfig(select=...)`) and the user does not confirm whole-project runs — **selector caveat**: factory mode explodes the entire manifest, so converting silently would change semantics; (c) `full_refresh=True` is detected and not manually resolved (never apply `--full-refresh` automatically — invalid for `dbt test`); (d) vars change the dbt graph or operators pass conflicting vars dicts (static vars live in the committed dbt_vars.json, read at parse and run time; runtime `dbt_vars` REPLACES the whole dict and is safe only for graph-invariant vars); (e) more than one dbt project in the bundle (v1: one project, colocated at bundle root; else split bundles); or (f) the user explicitly wants minimal toolchain change. **dbt Cloud never falls back to `dbt_task`** (dbt_task runs dbt Core, cannot trigger a dbt Cloud job) — Tier 4. Record decisions in `MIGRATION_NOTES.md`. Multiple dbt tasks over the same project (seed >> run >> test) collapse into ONE factory job with ONE `run_job_task` hop. Run the YAML job's companion notebook tasks serverless (omit cluster fields); map Airflow retries onto the YAML job's own tasks only, never the `run_job_task` hop. The glue rewrites every generated selector to the node's full FQN (`--select fqn:<pkg>.<path>.<name>` — bare names also match dbt FQN/path components and over-select) and pins test commands to `--indirect-selection empty`; it fails closed on 0.2.1 limitations (dbt unit tests dropped; sanitized task-key collisions; any generated selector not resolving to exactly its own node (checked with dbt's own is_selected_node imported at deploy time), or any FQN component (package, directory, or name) outside the [A-Za-z0-9_.-] allowlist (unsafe for dbt selector or shell parsing); the runner also rejects a command carrying its own --vars in either spelling (--vars <yaml> or --vars=<yaml>)) — fall back to `dbt_task` if hit. **Task-count check (1,000-task per-job limit):** after `make manifest`, run `make task-count` (unbundled vs bundled). If unbundled > 900, warn and offer `BUNDLE_TESTS = True` in the glue — collapses each resource's single-model tests into one `tests_<resource>` task (`dbt test --select <resource> --indirect-selection cautious`, kept as-is, not rewritten to empty); biggest task reduction but coarser retry granularity (a model's tests rerun together). Keep `False` when within budget. If bundled still > 1,000, no auto-fallback — record options in MIGRATION_NOTES (split by dbt tag, await a dbt-factory sub-job API, or user-chosen single `dbt_task`); the glue also fails closed above 1,000 tasks at deploy.

Factory-mode artifacts (per dbt-bearing DAG; `<dag_module>` = dag_id sanitized to a Python identifier, e.g. `sales.daily` -> `sales_daily`): `resources/<dag_module>_dbt_job.py` (PyDABs hook: reads `target/<target>/manifest.json`, enables only `FACTORY_TYPES` matching detected commands, rewrites selectors to full FQNs (tests pinned to --indirect-selection empty; `BUNDLE_TESTS = True` instead collapses a resource's single-model tests into one `tests_<resource>` task kept at --indirect-selection cautious, and selector-exactness then skips test nodes), prunes dangling deps, asserts unique task keys and selector exactness (each fqn selector resolves to exactly its own node, via dbt's imported matcher), fails closed on unit tests and above the 1,000-task per-job limit (`TASK_LIMIT`; `count_tasks` powers `make task-count`), builds tasks with `DbtTaskOptions(task_type=NOTEBOOK, environment_key="Default", notebook_path="src/run_dbt_command.py", project_directory="..", profiles_directory="dbt_profiles")`, defines job parameters `dbt_vars` (default "{}" = use the committed dbt_vars.json) and `dbt_target`, and writes `dbt_serverless_env.yaml` — `environment_version: "5"` pinning the venv's EXACT dbt-databricks AND dbt-core (dbt-core parity is required because the selector-exactness check imports the local dbt-core) — idempotently); `src/run_dbt_command.py` (OWNED runner: 0.2.1 base + dbt_vars appended as `--vars` argv + per-target parse-cache lookup; not extracted from the package); `resources/__init__.py` (empty); `pyproject.toml` (pins `databricks-bundles`, `databricks-dbt-factory`, and EXACT `dbt-databricks`/`dbt-core`, filling both `<DBT_DATABRICKS_VERSION>`/`<DBT_CORE_VERSION>` placeholders (never unresolved): preserve every dbt constraint the customer declares (exact, range, or either package alone), add only the missing dbt package(s) unconstrained, run `uv` resolution, then exact-pin both to the result; use the tested default `dbt-databricks==1.12.2`/`dbt-core==1.11.12` only when no dbt constraint exists. Never auto-change the resolved pins. Stop on an unsatisfiable `uv` solve (preserve constraints, manual resolution) or an environmental `uv` error; for a make manifest/bundle validate failure, preserve the pins, fix only clearly version-independent causes (auth/profiles/parsing/schema) directly, otherwise stop and surface evidence — repinning or a `dbt_task` fallback is then a user decision (automatic `dbt_task` fallback is only for the enumerated Phase 2 disqualifiers). dbt version/runtime parity since uv.lock is git-ignored); `tests/test_dbt_factory_glue.py` (from `dbt-tests.py.tmpl`; regression tests for the glue, run via `make test`); `Makefile` (`TARGET ?= dev`; `manifest`: dbt deps + dbt parse `--target $(TARGET)` `--target-path target/$(TARGET)` with `--vars` read from dbt_vars.json — no warehouse needed; `validate`/`deploy` depend on manifest; `task-count`: prints unbundled vs bundled task counts against the 1,000-task limit); `dbt_vars.json` at bundle root (committed static vars, `{}` when none — the runner falls back to it when the dbt_vars parameter is empty/{}); `dbt_profiles/profiles.yml` (dev/prod outputs named after bundle targets; host/token injected by the runner via `DBT_HOST`/`DBT_ACCESS_TOKEN` env vars); the dbt project copied to the bundle root (ONE project per bundle); `databricks.yml` gains `python: {venv_path: .venv, resources: ["resources.<dag_module>_dbt_job:load_resources"]}` and `sync.include: [dbt_serverless_env.yaml, target/*/partial_parse.msgpack, dbt_packages/**]` (use the project's `packages-install-path` instead of `dbt_packages` when set; the packages include is required whenever packages come from packages.yml OR dependencies.yml); `.gitignore` gains `.venv/`, `uv.lock`, `logs/`, `dbt_packages/` (or the custom packages-install-path), `target/**`, `dbt_serverless_env.yaml` — `target/*/manifest.json` is a git-ignored LOCAL input the hook reads at deploy time (NOT synced); `dbt_serverless_env.yaml` and `target/*/partial_parse.msgpack` are git-ignored but uploaded via `sync.include` (`load_resources` writes the env before sync); the owned runner and `dbt_vars.json` are committed source; exact `dbt-databricks`/`dbt-core` pins give dbt version/runtime parity (transitive deps not locked). Static vars live in the committed dbt_vars.json only — never inline JSON in shell/Python; operators passing conflicting vars are a disqualifier.

For schedule conversion:
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week, normalize Sunday `0/7 -> 1`)
- Convert presets (`@daily`, `@hourly`) to Quartz equivalents
- Extract timezone from `default_args` or `start_date`
- Airflow 3 `Asset`/`Dataset` scheduling -> `trigger.table_update` only when the asset resolves to a UC table via explicit `extra={"databricks_table": "catalog.schema.table"}`, a user-supplied mapping, or the skill-local `x-databricks-table:` URI scheme — else flag (an `Asset` URI is an arbitrary string; never infer a table from it). `schedule=[a, b]` (list) -> `ALL_UPDATED`; `a | b` -> `ANY_UPDATED`; `a & b` -> `ALL_UPDATED`. `AssetOrTimeSchedule` (time+asset) -> flag (a Lakeflow job takes a schedule OR a trigger, not both 1:1)

For Airflow 3 authoring surface (recognize -> safe-map -> flag):
- Imports moved: `airflow.sdk` (`dag`, `task`, `task_group`, `Asset`, `BaseOperator`, `Variable`, `Connection`, `chain`) and `apache-airflow-providers-standard` (`airflow.providers.standard.operators.{python,bash,trigger_dagrun,...}`, `airflow.providers.standard.sensors.{external_task,filesystem,time,time_delta,...}`). Same operator mappings, different paths. Airflow 3.0–3.1 still accepts legacy `airflow.operators.*`/`airflow.sensors.*` with deprecation warnings — recognize both
- `schedule_interval=` is removed (use `schedule=`); `SubDagOperator` is removed
- Flag (no clean mapping): the `@asset` decorator, `AssetWatcher`, asset aliases, DAG versioning/bundles, deadline alerts

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
4. **`src/*.sql`**: Extracted SQL. Dynamic refs cannot appear inline in SQL files: use named parameter markers (`{{ ds }}` -> `:run_date`, `{{ params.x }}` -> `:x`) and pass values via `sql_task.parameters` (e.g. `run_date: "{{job.parameters.run_date}}"`); identifiers via `IDENTIFIER(:catalog || '.' || :schema || '.table')`.
5. **`MIGRATION_NOTES.md`**: Tier 4 items, XCom patterns, Connections needing secrets, Variables needing parameters, settings without DABs equivalents, cross-DAG dependency map. Factory mode: selector semantics, serverless-only note (classic via `job_cluster_key` in `DbtTaskOptions`), the measured task count and 1,000-task per-job limit (whether `BUNDLE_TESTS` was enabled + its retry-granularity tradeoff; over-limit-even-bundled options), profiles values to fill, `make setup && make manifest` prerequisite.

### Phase 4: Review and Validate

1. **Dependency check**: Every `depends_on` references a valid `task_key`
2. **Orphan check**: No unreachable tasks
3. **Task type check**: Each task has exactly one task type field
4. **Compute check**: Serverless notebook tasks may omit ALL compute fields (`environment_key` optional); classic tasks need `job_cluster_key`/`existing_cluster_id`/`new_cluster`; referenced keys must be defined
5. **Parameter check**: All `{{job.parameters.*}}` have corresponding entries in `parameters`
6. **Factory-mode validation** (when active): `make setup` -> `make manifest` -> `databricks bundle validate -t dev`, in that order — validate executes the PyDABs hook and fails without `.venv` + `target/dev/manifest.json` (per-target manifests; never reuse dev artifacts for prod). Hook fail-closed RuntimeErrors (unit tests, task-key collisions, non-exact selectors, disallowed FQN characters) mean fall back to `dbt_task`. Any OTHER failure: preserve the resolved pins, fix only clearly version-independent causes directly, otherwise stop and surface evidence. Do NOT auto-fall-back to `dbt_task` (a parse-time dbt incompatibility recurs under it anyway); repinning or fallback is a user decision. Automatic `dbt_task` fallback is only for the enumerated factory disqualifiers. Skipping is allowed only when `uv`/`dbt` is unavailable at THIS step (pins already resolved) — report the exact commands the user must run. Statically check `python.resources` entries resolve and `run_job_task` references match the `JOB_KEY`s.
7. **Present summary**: File list, task count, MIGRATION_NOTES items
