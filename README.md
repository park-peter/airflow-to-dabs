# airflow-to-dabs

A coding agent skill that converts Apache Airflow DAGs into [Databricks Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/) projects (formerly Databricks Asset Bundles; DABs).

Given an Airflow DAG file, the agent produces a complete bundle project — `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files — ready for `databricks bundle deploy`.

## Platform Support

`SKILL.md` follows the open [Agent Skills](https://agentskills.io) format, so one skill folder works across coding agents:

| Agent | Global | Project |
|-------|--------|---------|
| **Claude Code** | `~/.claude/skills/airflow-to-dabs/` or the Claude Code plugin | `.claude/skills/airflow-to-dabs/` |
| **Codex** | `~/.agents/skills/airflow-to-dabs/` | `.agents/skills/airflow-to-dabs/` |
| **Cursor** | `~/.agents/skills/airflow-to-dabs/` | `.agents/skills/airflow-to-dabs/` |
| **VS Code + GitHub Copilot** | `~/.agents/skills/airflow-to-dabs/` | `.agents/skills/airflow-to-dabs/` |
| **Gemini CLI, Windsurf, Kiro, Junie, Roo Code, OpenCode, and more** | via `npx skills` | via `npx skills` |

## What It Does

- Parses Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Maps **40+ Airflow operator types** (including all [Databricks provider operators](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/index.html)) to DABs task type equivalents using a [tiered mapping system](references/operator-mapping.md)
- **Source-aware routing**: maps by connection, not class alone (`operator → connection → intent → direction → destination → strategy`) — Databricks SQL → `sql_task`, remote federatable DB → Lakehouse Federation, recurring ingestion → Lakeflow Connect; fail-closed on unresolved connections
- **Lakeflow Connect ingestion**: routes recurring source→Delta ingestion (CDC, query-based, and foreign-catalog incl. Snowflake→Delta) to a DABs managed-ingestion pipeline (see [`references/lakeflow-connect.md`](references/lakeflow-connect.md))
- **Snowflake operators**: federation (read), query-based foreign-catalog ingestion (recurring copy), or connector notebook — by intent
- **Dataproc operators**: payload-aware conversion of Dataproc and Managed Spark jobs/batches to native Python, JAR, SQL, or R notebook tasks; cluster lifecycle and wait sensors collapse into Lakeflow Jobs semantics
- **Serverless by default**: tasks run on serverless jobs compute unless the workload needs classic compute (R, Spark properties outside the serverless allowlist, init scripts, GPUs, custom containers, a source-configured cluster); classic runtime and node type are required variables, never guessed
- Converts Airflow cron expressions and presets to Quartz cron format
- Converts Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (`file_arrival`, `table_update`)
- Extracts inline Python, SQL, and bash into standalone source files
- Converts Jinja template variables (`{{ ds }}`, `{{ params.x }}`) to DABs dynamic value references
- **dbt factory mode (default for dbt workloads)**: converts dbt workloads — including [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos) `DbtDag`/`DbtTaskGroup` — into a separate Lakeflow job with one task per dbt model/seed/snapshot/test, generated at deploy time from the dbt manifest via PyDABs and [databricks-dbt-factory](https://github.com/mwojtyczka/databricks-dbt-factory); single `dbt_task` as fallback
- Generates `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- **Hadoop/HDFS migration support**: detects `spark-submit` in BashOperator/SSHOperator, cleans up YARN configs, maps HDFS paths, converts HiveQL, handles Sqoop alternatives
- Covers Airflow edge patterns including TaskFlow dataflow, dynamic task mapping (`.expand()`) and mapped task groups (`@task_group.expand()` → `for_each_task` + child job), and timetable/dataset scheduling notes
- **Airflow 3 support**: recognizes the `airflow.sdk` Task SDK and `apache-airflow-providers-standard` import paths, and maps `Asset`-based scheduling (see [`references/airflow3-migration.md`](references/airflow3-migration.md))

## Operator Coverage

| Tier | Description | Examples |
|------|-------------|----------|
| **1 — Direct** | 1:1 mapping to a DABs task type | `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `DatabricksSubmitRunOperator`, `DatabricksRunNowOperator`, `DatabricksNotebookOperator`, `DatabricksSqlOperator`, `DatabricksSQLStatementsOperator`, `DatabricksCopyIntoOperator`, `SQLExecuteQueryOperator`, `DbtOperator`, `TriggerDagRunOperator`, `HiveOperator`, `SSHOperator` |
| **2 — Semantic** | Requires reasoning about intent | Dataproc/Managed Spark operators (`DataprocSubmitJobOperator`, `DataprocCreateBatchOperator`, cluster lifecycle, workflow templates), cosmos `DbtDag`/`DbtTaskGroup`†, dynamic task mapping (`.expand()`), mapped task groups (`@task_group.expand()`), Snowflake operators (`SnowflakeSqlApiOperator`, `snowpark_task`), SQL data-quality checks (`SQLColumnCheckOperator`/`SQLTableCheckOperator`/…), cloud & messaging families (AWS/GCP/Azure/HTTP/SFTP/Kafka/Trino), `KubernetesPodOperator`, `DockerOperator`, `BranchPythonOperator`, `BranchDateTimeOperator`, `BranchDayOfWeekOperator`, `ShortCircuitOperator`, `DatabricksWorkflowTaskGroup`, `DatabricksTaskOperator`, `DatabricksCreateJobsOperator`, `SubDagOperator`, `TaskGroup`, `DummyOperator`, `EmailOperator`, `DatabricksReposCreateOperator`* |
| **3 — Sensor** | Converted to job-level triggers or absorbed into native tasks | `DataprocJobSensor`, `DataprocBatchSensor`, `S3KeySensor`, `DatabricksSqlSensor`, `DatabricksPartitionSensor`, `DatabricksSQLStatementsSensor`, `HdfsSensor`, `FileSensor`, `ExternalTaskSensor`, `SqlSensor`, `TimeSensor`, `BashSensor`, `PythonSensor` |
| **4 — Unsupported** | Flagged for manual review | Custom operators, `DbtCloudRunJobOperator`, `SqoopOperator`, `PigOperator`, XCom-heavy patterns |

\* `DatabricksReposCreateOperator`, `DatabricksReposUpdateOperator`, and `DatabricksReposDeleteOperator` are infrastructure/repo-management operators with no DABs job task equivalent — they are omitted and noted in `MIGRATION_NOTES.md`.

† dbt workloads (cosmos, dbt CLI operators, bash `dbt run`) default to **dbt factory mode** — a separate Python-generated job with one task per dbt object — with a single `dbt_task` as the documented fallback. See the dbt conversion decision point in [`references/operator-mapping.md`](references/operator-mapping.md).

Full mapping details: [`references/operator-mapping.md`](references/operator-mapping.md)

Dataproc details: [`references/dataproc-migration.md`](references/dataproc-migration.md)

## Installation

### npx skills (recommended)

[`skills`](https://github.com/vercel-labs/skills) installs the skill into any supported coding agent:

```bash
npx skills add park-peter/airflow-to-dabs
```

It detects your agents and asks where to install. To choose them up front:

```bash
npx skills add park-peter/airflow-to-dabs -g -a claude-code -a codex -a cursor -a github-copilot -y
```

`-g` installs for all projects instead of the current one. Agent ids include `claude-code`, `codex`, `cursor`, `github-copilot`, `gemini-cli`, `windsurf`, `kiro-cli`, `junie`, `roo`, and `opencode`. `npx skills update` and `npx skills remove airflow-to-dabs` update and remove it.

### Claude Code plugin

This repository is its own Claude Code plugin marketplace. In a Claude Code session:

```
/plugin marketplace add park-peter/airflow-to-dabs
/plugin install airflow-to-dabs@airflow-to-dabs
```

Or from a shell:

```bash
claude plugin marketplace add park-peter/airflow-to-dabs
claude plugin install airflow-to-dabs@airflow-to-dabs
```

`--scope project` records the plugin in the project's `.claude/settings.json` so everyone who clones the project gets it. To preconfigure a project by hand, commit:

```json
{
  "extraKnownMarketplaces": {
    "airflow-to-dabs": {
      "source": { "source": "github", "repo": "park-peter/airflow-to-dabs" }
    }
  },
  "enabledPlugins": {
    "airflow-to-dabs@airflow-to-dabs": true
  }
}
```

To pin a release, add the marketplace at a tag (`park-peter/airflow-to-dabs#<tag>`), or set `"ref": "<tag>"` in the `source` object above. The plugin version is the git commit it was installed from. Update with `claude plugin marketplace update airflow-to-dabs` followed by `claude plugin update airflow-to-dabs@airflow-to-dabs`; remove with `claude plugin uninstall airflow-to-dabs@airflow-to-dabs`.

### install.sh (no Node.js)

`install.sh` clones the skill with `git`:

```bash
curl -fsSL https://raw.githubusercontent.com/park-peter/airflow-to-dabs/main/install.sh | sh
```

It asks which agents and scope to install for. To skip the prompts:

```bash
curl -fsSL https://raw.githubusercontent.com/park-peter/airflow-to-dabs/main/install.sh | sh -s -- --platform all --scope global
```

| `--platform` | Installs to |
|--------------|-------------|
| `claude` | `~/.claude/skills/airflow-to-dabs` (global) or `.claude/skills/airflow-to-dabs` (project) |
| `agents` (also `codex`, `cursor`, `copilot`) | `~/.agents/skills/airflow-to-dabs` (global) or `.agents/skills/airflow-to-dabs` (project) |
| `all` | Both |

Re-run the same command to update. Add `--uninstall` to remove.

### Manual

Clone the repo into a skills directory from the Platform Support table:

```bash
git clone https://github.com/park-peter/airflow-to-dabs.git ~/.agents/skills/airflow-to-dabs   # Codex, Cursor, VS Code Copilot
git clone https://github.com/park-peter/airflow-to-dabs.git ~/.claude/skills/airflow-to-dabs   # Claude Code
```

Update with `git -C <dir> pull --ff-only`.

## Usage

The agent follows a 4-phase workflow: **Parse** the DAG, **Map** operators to DABs task types, **Generate** the bundle project, and **Review** for correctness.

### Convert multiple DAGs (default)

> "Convert all DAGs in the dags/ directory to Databricks Asset Bundles"

Produces a single bundle with one `databricks.yml` and a separate job resource per DAG. Cross-DAG dependencies resolve within the same bundle.

### Convert a single DAG

> "Convert my_etl_dag.py to a Databricks Asset Bundle"

Produces a standalone bundle directory for that one DAG.

### Convert a dbt / cosmos DAG (factory mode)

> "Convert orders_analytics_dag.py to a Databricks Asset Bundle — the dbt project is at ./dbt/orders_analytics"

Produces a two-job bundle: a YAML job for the non-dbt tasks with a `run_job_task` hop, plus a Python-generated dbt job (one task per dbt model/seed/snapshot/test) built at deploy time from the dbt manifest via PyDABs. See [`examples/dbt-cosmos/`](examples/dbt-cosmos/) for a complete conversion.

### Convert an Airflow 3 DAG with dynamic mapping / mapped task groups

> "Convert regional_ingest_dag.py to a Databricks Asset Bundle"

Recognizes the `airflow.sdk` and `apache-airflow-providers-standard` imports, maps `.expand()` to a `for_each_task`, and turns a mapped task group (`@task_group.expand()`) into a `for_each_task` → `run_job_task` → child job holding the subgraph. See [`examples/dynamic-mapping/`](examples/dynamic-mapping/) for a complete conversion.

### Convert a recurring ingestion DAG (Lakeflow Connect)

> "Convert orders_replication_dag.py — it replicates a Snowflake table hourly"

Routes recurring source→Delta ingestion to a Lakeflow Connect managed-ingestion pipeline (Snowflake via a UC foreign catalog, `ingest_from_uc_foreign_catalog`) with a `pipeline_task` hop into the downstream transform. See [`examples/lakeflow-connect/`](examples/lakeflow-connect/) for a complete conversion.

### Convert an Airflow DAG that orchestrates Dataproc

> "Convert this Airflow DAG that creates a Dataproc cluster, submits a PySpark job, waits for it, and deletes the cluster"

Routes the nested Dataproc payload rather than the operator name: the PySpark submission becomes a native `spark_python_task`, the cluster lifecycle and wait sensor collapse into Lakeflow Jobs compute/execution semantics, and unsupported Dataproc-only properties are called out explicitly. See [`references/dataproc-migration.md`](references/dataproc-migration.md) and [`examples/dataproc/`](examples/dataproc/).

## flowx Provider Profile

[`providers/flowx-gap-resolver/`](providers/flowx-gap-resolver/) holds a machine-readable provider profile for flowx's fingerprint-bound Airflow gap workflow. In this mode:

- flowx owns DAG parsing, task identity, graph structure, policy, IR, and bundle packaging.
- The provider receives one `GapEnvelope` and returns one constrained `AgenticResolution`, carrying task, graph, provider, and request hashes.
- A resolution can attach one notebook, SQL, or Spark Python leaf payload, request user input with per-argument dispositions, or defer when a faithful migration requires graph or resource changes.
- Static DAG run timeouts and failure notifications are preserved as Job settings; disabled policies are explicit no-ops. Cross-run, retry-email, SLA-callback, auto-pause, dynamic-timeout, and task-environment semantics are blocking gaps.
- The provider never reparses the DAG or generates a competing bundle.

[`provider.json`](providers/flowx-gap-resolver/provider.json) declares contract compatibility, knowledge files, and fixture paths. [`PROFILE.md`](providers/flowx-gap-resolver/PROFILE.md) is the agent entrypoint. The paired JSON fixtures cover notebook, SQL, Spark Python, `needs_input`, and `deferred` outcomes and can be replayed by flowx as interoperability tests.

## Post-Generation Configuration

The generated bundle uses placeholders for environment-specific values. Replace these before deploying, or provide the values in your prompt to skip this step (e.g., "use warehouse ID abc123"). Tasks run on serverless compute unless the workload needs classic compute or you say the workspace has no serverless jobs compute.

| Placeholder | Location | Example value |
|---|---|---|
| `<DEV_WORKSPACE_URL>` | `databricks.yml` → `targets.dev.workspace.host` | `https://my-dev.cloud.databricks.com` |
| `<PROD_WORKSPACE_URL>` | `databricks.yml` → `targets.prod.workspace.host` | `https://my-prod.cloud.databricks.com` |
| `<SERVICE_PRINCIPAL>` | `databricks.yml` → `targets.prod.run_as` | `my-deploy-sp` |
| `spark_version` (classic compute only) | Required bundle variable, no default: `--var spark_version=<value>` or a target's `variables` | A supported runtime from `databricks clusters spark-versions` |
| `node_type_id` (classic compute only) | Required bundle variable, no default | A workspace-supported node type from `databricks clusters list-node-types`; Dataproc machine types are not copied or guessed |
| `warehouse_id` (SQL tasks only) | Required bundle variable, no default | `abc123def456` |
| `<WAREHOUSE_ID>` (factory mode) | `dbt_profiles/profiles.yml` → `http_path` | `/sql/1.0/warehouses/abc123def456` |
| `<DBT_PROFILE_NAME>` (factory mode) | `dbt_profiles/profiles.yml` — must match `profile:` in `dbt_project.yml` | `orders_analytics` |
| `<DEV_CATALOG>` / `<DEV_SCHEMA>` (factory mode) | `dbt_profiles/profiles.yml` → `catalog` / `schema` | `main` / `analytics` |

> **Tip:** If you've already configured auth via `~/.databrickscfg` or `DATABRICKS_HOST`, you can remove `workspace.host` from targets entirely — the CLI picks it up automatically.

## Repository Tests

`make test` runs the skill's own checks, and CI runs the same targets on every push and pull request:

```bash
make test             # contract + dbt glue suites
make test-contracts   # cross-surface rule coverage and structural checks
make test-glue        # regression tests for the generated PyDABs dbt glue
make validate         # schema-validate the checked-in example bundles
make validate-plugin  # validate the Claude Code plugin and marketplace manifests
```

`tests/test_skill_contracts.py` matches each hardening rule through the `<!-- contract: id -->` anchors in `SKILL.md`, so a dropped rule fails the build while rewording does not. Add an anchor when adding a rule.

## Validation

After filling in placeholders, validate the bundle before deploying:

```bash
databricks bundle validate -t dev
```

For factory-mode bundles, install the venv and generate the dbt manifest first — `bundle validate` executes the PyDABs hook, which needs both:

```bash
make setup      # uv sync --dev
make manifest   # dbt deps + dbt parse (no warehouse connection needed)
databricks bundle validate -t dev
```

If Databricks auth is not configured yet, run schema checks offline first:

```bash
databricks bundle schema
```

## Reference Files

| File | Description |
|------|-------------|
| [`references/operator-mapping.md`](references/operator-mapping.md) | Tier 1–4 mapping table with side-by-side Airflow/DABs YAML examples |
| [`references/dab-schema-reference.md`](references/dab-schema-reference.md) | Condensed DABs YAML schema — all task types, triggers, clusters, variables |
| [`references/schedule-trigger-mapping.md`](references/schedule-trigger-mapping.md) | Cron conversion, sensor-to-trigger mapping, Airflow 3 Asset/`AssetOrTimeSchedule` scheduling, `default_args` mapping, Jinja variable conversion |
| [`references/conversion-examples.md`](references/conversion-examples.md) | 7 complete before/after examples (ETL chain, branching, sensor-triggered, multi-system, cosmos dbt factory mode, Airflow 3 dynamic mapping + mapped task group, Dataproc) |
| [`references/airflow3-migration.md`](references/airflow3-migration.md) | Airflow 3 recognition — `airflow.sdk` + `apache-airflow-providers-standard` imports, Assets vs Datasets, asset scheduling, deferrable/native-async/resumable execution model, removed operators, recognize→safe-map→flag checklist |
| [`references/lakeflow-connect.md`](references/lakeflow-connect.md) | Lakeflow Connect ingestion target — when to use it vs a Jobs task, CDC/query-based/foreign-catalog styles (incl. Snowflake→Delta), eligibility, DABs generation contract, continuous-vs-triggered orchestration, MIGRATION_NOTES checklist |
| [`references/dataproc-migration.md`](references/dataproc-migration.md) | Dataproc/Managed Spark operator inventory, native task routing by payload, cluster lifecycle collapse, workflow templates, GCS/auth migration, retained-external fallback, validation checklist |
| [`references/hadoop-migration-guide.md`](references/hadoop-migration-guide.md) | HDFS path conversion, YARN config cleanup, Hive-to-UC mapping, spark-submit detection, Sqoop alternatives, bulk conversion guidance |
| [`assets/templates/`](assets/templates/) | Skeleton `databricks.yml`, job resource, and dbt factory mode templates (PyDABs hook, pyproject, Makefile, profiles) |
| [`.claude-plugin/`](.claude-plugin/) | Claude Code plugin and marketplace manifests |

## Example Output

**Multiple DAGs (default)** — single bundle, multiple jobs:

```
airflow-migration/
  databricks.yml              # Single bundle config, shared variables/targets
  resources/
    etl_pipeline_job.yml       # One job resource per DAG
    reporting_pipeline_job.yml
    data_quality_job.yml
  src/
    etl_pipeline/              # Source files namespaced per DAG
      extract.py
      transform.py
      load.py
    reporting_pipeline/
      generate_report.sql
    data_quality/
      check_nulls.py
  MIGRATION_NOTES.md           # Consolidated notes for all DAGs
```

Cross-DAG dependencies (e.g., `TriggerDagRunOperator`) resolve via `${resources.jobs.<name>.id}` within the same bundle.

**Single DAG** — one standalone bundle:

```
daily-etl-pipeline/
  databricks.yml              # Bundle config with dev/prod targets
  resources/
    daily_etl_pipeline_job.yml # Job with schedule, clusters, 3 tasks
  src/
    extract.py                 # Notebook extracted from PythonOperator
    transform.py
    load.py
  MIGRATION_NOTES.md           # Conversion decisions
```

See [`references/conversion-examples.md`](references/conversion-examples.md) for full before/after walkthroughs.

## License

MIT
