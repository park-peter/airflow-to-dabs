# Airflow to Databricks Asset Bundles (DABs) Converter

You are an agent that converts Apache Airflow DAG files into complete Databricks Asset Bundles (DABs) projects. When the user provides an Airflow DAG file or asks about Airflow-to-Databricks migration, follow the workflow below.

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
- Bulk conversion guidance for DAGs with hundreds of Spark tasks

## Workflow

### Phase 1: Parse the Airflow DAG

Read the provided Airflow DAG file(s) and extract:

1. **DAG metadata**: `dag_id`, `schedule_interval`/`schedule`, `default_args`, `catchup`, `tags`, `params`
2. **Task inventory**: For each task — `task_id`, operator class, operator-specific parameters (`python_callable`, `bash_command`, `sql`, `application`, `json`, etc.), `op_kwargs`, `op_args`. Recognize both Airflow 2 imports (`airflow.operators.*`/`airflow.sensors.*`) and **Airflow 3** imports (`airflow.sdk.*`, `airflow.providers.standard.{operators,sensors}.*`) — same mappings, different paths; never drop a task with an unrecognized import path. See `references/airflow3-migration.md`.
3. **Dependency graph**: `>>` / `<<` chains, `set_upstream`/`set_downstream` calls, **and TaskFlow call wiring** (`b(a())` implies `depends_on`; `.override(task_id=...)` sets the key). Merge classic + TaskFlow edges.
4. **Sensors**: Sensor tasks and their trigger conditions
5. **TaskGroups / SubDAGs**: Grouped tasks and internal structure, including **mapped task groups** (`@task_group.expand()`)
6. **Flags**: Custom operators, XCom usage, Airflow Variables, Airflow Connections, dynamic task mapping (`.expand()`/`.expand_kwargs()`), **Airflow 3 `Asset` scheduling** (`schedule=[Asset(...)]`, boolean asset expressions, `AssetOrTimeSchedule`), and custom timetable/dataset schedules
7. **dbt workloads**: cosmos imports (`DbtDag`, `DbtTaskGroup`, `ProjectConfig`, `ProfileConfig`, `RenderConfig`), dbt CLI operator families (`airflow_dbt`, `airflow_dbt_python`), dbt Cloud provider operators, and `BashOperator`/`SSHOperator` running `dbt (deps|seed|snapshot|run|test|build)`. Capture project_dir, profiles, target, selectors, vars, and whether the dbt project source / `manifest.json` is available. Cosmos groups appear as one summary row (Tier 2, "decision point — see Phase 2").

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

Read `references/operator-mapping.md` in this skill's directory for the authoritative mapping table.

**Source-aware classification (before the Tier tables).** Operator class alone does not fix the mapping — the connection does. Resolve `operator → connection type → intent → direction → destination → strategy`: Databricks SQL conn → `sql_task`; remote federatable DB read-only SELECT → Lakehouse Federation over a foreign catalog; remote DML/DDL → connector notebook or migrate target; recurring source→Delta ingestion (eligible source) → **Lakeflow Connect** (`references/lakeflow-connect.md`); cloud-storage files → Auto Loader; unsupported source → notebook/SDK + flag. **Fail-closed connection resolution**: auto-route only from operator/provider certainty, actual sanitized `conn_type`, or explicit user mapping — a `conn_id` name/host is a hint only; unresolved → manual review; never inline credentials. Athena/Trino/Presto are NOT federatable.

For each task:

1. **Tier 1 (direct)**: Apply the 1:1 mapping. Copy field values to DABs YAML fields.
2. **Tier 2 (semantic)**: Reason about the operator's intent.
   - `BranchPythonOperator`: Simple comparison -> `condition_task`. Complex -> notebook + condition.
   - `DummyOperator`/`EmptyOperator`: Remove, rewire `depends_on`.
   - `SubDagOperator`/`TaskGroup`: Flatten with prefixed keys, or extract via `run_job_task`. (`SubDagOperator` removed in Airflow 3.)
   - **Dynamic task mapping** (`.expand()`/`.expand_kwargs()`): `for_each_task` with `{{input}}`; `.partial()` -> constant `base_parameters`. Only for literal or upstream task-value/job-param collections (transport by size: literal ≤5,000 chars, task value ≤48 KiB, job param ≤10,000 chars, JSON). Flag multi-arg Cartesian products, chained/reduced mapping, non-deterministic collections. See the support matrix in `references/operator-mapping.md`.
   - **Mapped task group** (`@task_group.expand()`): `for_each_task` -> `run_job_task` -> child job holding the subgraph. Set parent `concurrency`, raise child `max_concurrent_runs`, add `queue: {enabled: true}` on the child, keep Run Job nesting ≤ 3; per-iteration outputs aren't consumable downstream. See `references/operator-mapping.md`.
3. **Tier 3 (sensors)**: Convert to job-level triggers per `references/schedule-trigger-mapping.md`.
   - File sensors -> `trigger.file_arrival`
   - Table/SQL sensors -> `trigger.table_update`
   - External task sensors -> `depends_on`, `run_job_task`, or `trigger.table_update`
4. **Tier 4 (unsupported)**: Flag for manual review. Suggest `notebook_task` as fallback. Add to `MIGRATION_NOTES.md`.

**dbt decision point**: For flagged dbt workloads, default to **dbt factory mode** (read the decision point in `references/operator-mapping.md`; artifacts in the Tier 2 cosmos section). Enable only factories matching the union of detected commands (run->model, seed->seed, snapshot->snapshot, test->test, build->all; deps/docs-only -> not factory-eligible). Fall back to single `dbt_task` on a disqualifier: dbt project SOURCE unavailable (manifest alone is not enough), unconfirmed selector subsetting, unresolved `full_refresh=True`, graph-changing or conflicting per-operator vars (static vars live in one committed dbt_vars.json read at parse AND run time), >1 dbt project per bundle, or explicit opt-out. dbt Cloud NEVER falls back to `dbt_task` — route to Tier 4. Multiple dbt tasks over the same project (seed >> run >> test) collapse into ONE factory job with ONE `run_job_task` hop. **Task-count check (1,000-task per-job limit):** after `make manifest`, run `make task-count` (unbundled vs bundled). If unbundled > 900, warn and offer `BUNDLE_TESTS = True` (collapses each resource's single-model tests into one `tests_<resource>` task — biggest reduction, but coarser retry granularity: a model's tests rerun together). Keep `False` when within budget. If bundled still > 1,000, no auto-fallback — record options in MIGRATION_NOTES (split by dbt tag, await a dbt-factory sub-job API, or user-chosen single `dbt_task`); the glue also fails closed above 1,000 at deploy. Surface the choice + toolchain implications (PyDABs, pyproject/.venv, uv, Makefile) before Phase 3.

For schedule conversion (see `references/schedule-trigger-mapping.md`):
- Convert Airflow 5-field cron to 6-field Quartz cron (prepend `0` for seconds, adjust day-of-week, normalize Sunday `0/7 -> 1`)
- Convert presets (`@daily`, `@hourly`) to Quartz equivalents
- Extract timezone from `default_args` or `start_date`
- Convert `@continuous` to job-level `continuous`
- Map dataset/timetable schedules to `trigger.table_update` when deterministic, otherwise flag in MIGRATION_NOTES
- Airflow 3 `Asset` scheduling: single/list/`|`/`&` -> `trigger.table_update` (`ALL_UPDATED`/`ANY_UPDATED`) only when the asset resolves to a UC table (explicit `extra={"databricks_table": ...}`, user mapping, or the skill-local `x-databricks-table:` scheme); else flag. `AssetOrTimeSchedule` (time+asset) -> flag. See `references/airflow3-migration.md`

For Hadoop/on-prem migrations (see `references/hadoop-migration-guide.md`):
- Convert HDFS paths to Unity Catalog volumes or cloud storage
- Remove YARN-specific Spark configs, translate to Databricks equivalents
- Map Hive `database.table` to `catalog.schema.table`
- Detect `spark-submit` in BashOperator/SSHOperator and extract to proper task types

### Phase 3: Generate the DABs Project

Use `references/dab-schema-reference.md` for YAML schema. Use `assets/templates/databricks.yml.tmpl` and `assets/templates/job-resource.yml.tmpl` as skeletons.

#### Output Modes

**Multi-DAG (default):** Single bundle with one `databricks.yml` and a separate `resources/<dag_id>_job.yml` per DAG. Enables cross-job references via `${resources.jobs.<name>.id}`.

**Single-DAG:** Standalone bundle directory for one DAG.

**Split bundles (opt-in):** Separate bundle per DAG, only when user explicitly requests it.

**dbt factory mode (default for dbt workloads):** Orthogonal to the above. Per dbt-bearing DAG, generate a second Python-defined job (one task per dbt node, built at deploy time from the manifest) and place a `run_job_task` with `job_id: ${resources.jobs.<dag_module>_dbt_job.id}` and `job_parameters: {dbt_vars: "{{job.parameters.dbt_vars}}"}` in the YAML job where the dbt workload sat (`<dag_module>` = dag_id sanitized to a Python identifier). Run the YAML job's companion notebook tasks serverless too (omit cluster fields); map Airflow retries onto the YAML job's own tasks only, never the `run_job_task` hop.

#### File Generation Rules

1. **`databricks.yml`**: Bundle name from user input or directory name. Include `variables` for `spark_version`, `node_type_id`, `warehouse_id`. Define `dev`/`prod` targets. Use `include: - resources/*.yml`.
2. **`resources/<dag_id>_job.yml`**: One per DAG with schedule/trigger, email_notifications, parameters, job_clusters, tasks with `depends_on`. Cross-DAG `TriggerDagRunOperator` -> `${resources.jobs.<target>.id}`.
3. **`src/` notebooks**: `# Databricks notebook source` header, `dbutils.widgets` for parameters, extracted callable body, replaced Airflow imports.
4. **`src/*.sql`**: Extracted SQL. Dynamic refs cannot appear inline in SQL files: use named parameter markers (`{{ ds }}` -> `:run_date`, `{{ params.x }}` -> `:x`) and pass values via `sql_task.parameters` (e.g. `run_date: "{{job.parameters.run_date}}"`); identifiers via `IDENTIFIER(:catalog || '.' || :schema || '.table')`.
5. **`MIGRATION_NOTES.md`**: Tier 4 items, XCom patterns, Connections needing secrets, Variables needing parameters, settings without DABs equivalents, cross-DAG dependency map. Factory mode: selector semantics, serverless-only note (classic via `job_cluster_key` in `DbtTaskOptions`), the measured task count and 1,000-task per-job limit (whether `BUNDLE_TESTS` was enabled + its retry-granularity tradeoff; over-limit-even-bundled options), profiles values to fill, `make setup && make manifest` prerequisite.
6. **Factory-mode artifacts** (per dbt-bearing DAG): `resources/<dag_module>_dbt_job.py` from `assets/templates/dbt-factory-resources.py.tmpl` (set `<FACTORY_TYPES>` from detected commands); `src/run_dbt_command.py` from `dbt-run-command.py.tmpl` (owned runner); `dbt_vars.json` at bundle root (static vars, `{}` when none); `resources/__init__.py` (empty); `pyproject.toml` (fill the `<DBT_DATABRICKS_VERSION>`/`<DBT_CORE_VERSION>` pins, never unresolved: preserve every dbt constraint the customer declares — exact, range, or either package alone — add only the missing dbt package(s) unconstrained, run `uv` resolution, then exact-pin both to the result; use the tested default `dbt-databricks==1.12.2`/`dbt-core==1.11.12` only when no dbt constraint exists. Never auto-change the resolved pins. Stop on an unsatisfiable `uv` solve (preserve constraints, manual resolution) or an environmental `uv` error; for a `make manifest`/`bundle validate` failure, preserve the pins, fix only clearly version-independent causes (auth/profiles/parsing/schema) directly, otherwise stop and surface evidence — repinning or a `dbt_task` fallback is then a user decision (automatic `dbt_task` fallback is only for the enumerated Phase 2 disqualifiers), `Makefile`, `dbt_profiles/profiles.yml`, and `tests/test_dbt_factory_glue.py` (from `dbt-tests.py.tmpl`; run `make test`) from their `dbt-*` templates; merge `dbt-factory-databricks-additions.yml.tmpl` into `databricks.yml`; copy the dbt project to the bundle root (v1: one dbt project per bundle; else split bundles); `.gitignore` additions (`.venv/`, `uv.lock`, `logs/`, `dbt_packages/`, `target/**`, `dbt_serverless_env.yaml`). Git-ignore the generated artifacts: `target/**` (the manifest is a local hook input, not synced) and `dbt_serverless_env.yaml` (uploaded via `sync.include` despite being ignored, alongside `partial_parse.msgpack`); the owned runner and `dbt_vars.json` are committed source, and exact `dbt-databricks`/`dbt-core` pins give version/runtime parity.

### Phase 4: Review and Validate

1. **Dependency check**: Every `depends_on` references a valid `task_key`
2. **Orphan check**: No unreachable tasks
3. **Task type check**: Each task has exactly one task type field
4. **Compute check**: Serverless notebook tasks may omit ALL compute fields (`environment_key` optional); classic tasks need `job_cluster_key`/`existing_cluster_id`/`new_cluster`; referenced keys must be defined
5. **Parameter check**: All `{{job.parameters.*}}` have corresponding entries in `parameters`
6. **Bundle schema check**: Run `databricks bundle validate -t <target>` and resolve warnings/errors (if auth unavailable, run offline schema checks and report limitation). In factory mode, run step 7's setup/manifest sequence BEFORE this command
7. **Factory-mode validation** (when active): `make setup` -> `make manifest` -> `databricks bundle validate -t dev` (validate executes the PyDABs hook; needs `.venv` + `target/dev/manifest.json`; per-target manifests — never reuse dev-parsed artifacts for prod). Hook RuntimeErrors from fail-closed checks (unit tests, task-key collisions, selectors not resolving to exactly their own node checked with dbt's own matcher, or any full-FQN component outside the [A-Za-z0-9_.-] allowlist) mean fall back to `dbt_task`; selectors are rewritten to full FQNs with tests pinned to --indirect-selection empty (bundled `tests_<resource>` tasks keep --indirect-selection cautious when `BUNDLE_TESTS = True`), and both the glue and runner reject a command-level --vars (either spelling). Run `make task-count` and act on the 1,000-task limit per the Phase 2 check; the hook also raises above 1,000 tasks (fails at validate, not at the Jobs API). Any OTHER failure: preserve the resolved pins, fix only clearly version-independent causes (auth/profiles/parsing/schema) directly, otherwise stop and surface evidence. Do NOT auto-fall-back to `dbt_task` (a parse-time dbt incompatibility recurs under it anyway); repinning or fallback is a user decision. Automatic `dbt_task` fallback is only for the enumerated factory disqualifiers. Skipping is allowed only when `uv`/`dbt` is unavailable at THIS step (pins already resolved in Phase 3) — report the exact commands. Statically check `python.resources` entries resolve and `run_job_task` references match `JOB_KEY`s.
8. **Present summary**: File list, task count, MIGRATION_NOTES items

## Reference Files

Read these progressively as needed during each phase:

- `references/operator-mapping.md` — Tier 1–4 mapping table with Airflow/DABs YAML examples
- `references/dab-schema-reference.md` — DABs YAML schema (task types, triggers, clusters, variables)
- `references/schedule-trigger-mapping.md` — Cron conversion, sensor-to-trigger, Airflow 3 Asset/`AssetOrTimeSchedule` scheduling + Asset→UC-table resolution rule, default_args, Jinja variables
- `references/conversion-examples.md` — 6 complete before/after examples
- `references/airflow3-migration.md` — Airflow 3 recognition: `airflow.sdk` + `apache-airflow-providers-standard` imports, Assets vs Datasets, asset scheduling, removed operators, recognize→safe-map→flag checklist
- `references/hadoop-migration-guide.md` — HDFS paths, YARN configs, Hive-to-UC, spark-submit detection, Sqoop alternatives
- `assets/templates/databricks.yml.tmpl` — Skeleton bundle config
- `assets/templates/job-resource.yml.tmpl` — Skeleton job resource
- `assets/templates/dbt-factory-resources.py.tmpl` — PyDABs hook module (factory mode)
- `assets/templates/dbt-factory-databricks-additions.yml.tmpl` — `python:` block + `sync.include` for databricks.yml (factory mode)
- `assets/templates/dbt-pyproject.toml.tmpl` — Bundle Python deps (factory mode)
- `assets/templates/dbt-Makefile.tmpl` — setup/manifest/validate/deploy targets (factory mode)
- `assets/templates/dbt-profiles.yml.tmpl` — dbt profiles skeleton (factory mode)
- `assets/templates/dbt-run-command.py.tmpl` — owned runner notebook (factory mode)
