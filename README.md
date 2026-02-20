# airflow-to-dabs

A coding agent skill that converts Apache Airflow DAGs into [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/) (DABs) projects.

Given an Airflow DAG file, the agent produces a complete bundle project — `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files — ready for `databricks bundle deploy`.

## Platform Support

| Platform | Instruction File | Install Path |
|----------|-----------------|--------------|
| **Cursor** | `SKILL.md` | `~/.cursor/skills/airflow-to-dabs/` |
| **Claude Code** | `SKILL.md` | `~/.claude/skills/airflow-to-dabs/` |
| **Codex CLI** | `AGENTS.md` | `~/.codex/AGENTS.md` or `./AGENTS.md` |

## What It Does

- Parses Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Maps **40+ Airflow operator types** (including all [Databricks provider operators](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/index.html)) to DABs task type equivalents using a [tiered mapping system](references/operator-mapping.md)
- Converts Airflow cron expressions and presets to Quartz cron format
- Converts Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (`file_arrival`, `table_update`)
- Extracts inline Python, SQL, and bash into standalone source files
- Converts Jinja template variables (`{{ ds }}`, `{{ params.x }}`) to DABs dynamic value references
- Generates `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- **Hadoop/HDFS migration support**: detects `spark-submit` in BashOperator/SSHOperator, cleans up YARN configs, maps HDFS paths, converts HiveQL, handles Sqoop alternatives
- Covers Airflow edge patterns including dynamic task mapping (`expand`) and timetable/dataset scheduling notes

## Operator Coverage

| Tier | Description | Examples |
|------|-------------|----------|
| **1 — Direct** | 1:1 mapping to a DABs task type | `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `DatabricksSubmitRunOperator`, `DatabricksRunNowOperator`, `DatabricksNotebookOperator`, `DatabricksSqlOperator`, `DatabricksSQLStatementsOperator`, `DatabricksCopyIntoOperator`, `SQLExecuteQueryOperator`, `DbtOperator`, `TriggerDagRunOperator`, `HiveOperator`, `SSHOperator` |
| **2 — Semantic** | Requires reasoning about intent | `KubernetesPodOperator`, `DockerOperator`, `BranchPythonOperator`, `ShortCircuitOperator`, `DatabricksWorkflowTaskGroup`, `DatabricksTaskOperator`, `DatabricksCreateJobsOperator`, `SubDagOperator`, `TaskGroup`, `DummyOperator`, `EmailOperator`, `DatabricksReposCreateOperator`* |
| **3 — Sensor** | Converted to job-level triggers | `S3KeySensor`, `DatabricksSqlSensor`, `DatabricksPartitionSensor`, `DatabricksSQLStatementsSensor`, `HdfsSensor`, `FileSensor`, `ExternalTaskSensor`, `SqlSensor`, `TimeSensor` |
| **4 — Unsupported** | Flagged for manual review | Custom operators, `SqoopOperator`, `PigOperator`, XCom-heavy patterns |

\* `DatabricksReposCreateOperator`, `DatabricksReposUpdateOperator`, and `DatabricksReposDeleteOperator` are infrastructure/repo-management operators with no DABs job task equivalent — they are omitted and noted in `MIGRATION_NOTES.md`.

Full mapping details: [`references/operator-mapping.md`](references/operator-mapping.md)

## Installation

Pick one scope (personal or project) unless you intentionally want both.

<details>
<summary><strong>Cursor</strong></summary>

```bash
mkdir -p ~/.cursor/skills
if [ -d ~/.cursor/skills/airflow-to-dabs/.git ]; then
  git -C ~/.cursor/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.cursor/skills/airflow-to-dabs
fi
```

Cursor auto-discovers skills in `~/.cursor/skills/`. Triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, or DABs generation.

</details>

<details>
<summary><strong>Claude Code</strong></summary>

**Personal** (all projects):

```bash
mkdir -p ~/.claude/skills
if [ -d ~/.claude/skills/airflow-to-dabs/.git ]; then
  git -C ~/.claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.claude/skills/airflow-to-dabs
fi
```

**Project-scoped** (single project):

```bash
mkdir -p .claude/skills
if [ -d .claude/skills/airflow-to-dabs/.git ]; then
  git -C .claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .claude/skills/airflow-to-dabs
fi
```

Claude Code reads `SKILL.md` from `~/.claude/skills/` (personal) or `.claude/skills/` (project).

</details>

<details>
<summary><strong>Codex CLI</strong></summary>

Codex CLI reads `AGENTS.md` from the project root or `~/.codex/`. The install clones the skill repo and appends a marker-delimited block to your `AGENTS.md` — it backs up the file first and skips the append if the block already exists.

**Global** (all projects):

```bash
mkdir -p ~/.codex/skills
if [ -d ~/.codex/skills/airflow-to-dabs/.git ]; then
  git -C ~/.codex/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.codex/skills/airflow-to-dabs
fi
touch ~/.codex/AGENTS.md
cp ~/.codex/AGENTS.md ~/.codex/AGENTS.md.bak.$(date +%Y%m%d%H%M%S)
if ! grep -q "BEGIN airflow-to-dabs" ~/.codex/AGENTS.md; then
  {
    echo
    echo "<!-- BEGIN airflow-to-dabs -->"
    cat ~/.codex/skills/airflow-to-dabs/AGENTS.md
    echo "<!-- END airflow-to-dabs -->"
  } >> ~/.codex/AGENTS.md
fi
```

**Project-scoped** (single project):

```bash
mkdir -p .codex/skills
if [ -d .codex/skills/airflow-to-dabs/.git ]; then
  git -C .codex/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .codex/skills/airflow-to-dabs
fi
touch ./AGENTS.md
cp ./AGENTS.md ./AGENTS.md.bak.$(date +%Y%m%d%H%M%S)
if ! grep -q "BEGIN airflow-to-dabs" ./AGENTS.md; then
  {
    echo
    echo "<!-- BEGIN airflow-to-dabs -->"
    cat .codex/skills/airflow-to-dabs/AGENTS.md
    echo "<!-- END airflow-to-dabs -->"
  } >> ./AGENTS.md
fi
```

</details>

## Usage

The agent follows a 4-phase workflow: **Parse** the DAG, **Map** operators to DABs task types, **Generate** the bundle project, and **Review** for correctness.

### Convert multiple DAGs (default)

> "Convert all DAGs in the dags/ directory to Databricks Asset Bundles"

Produces a single bundle with one `databricks.yml` and a separate job resource per DAG. Cross-DAG dependencies resolve within the same bundle.

### Convert a single DAG

> "Convert my_etl_dag.py to a Databricks Asset Bundle"

Produces a standalone bundle directory for that one DAG.

## Post-Generation Configuration

The generated bundle uses placeholders for environment-specific values. Replace these before deploying, or provide the values in your prompt to skip this step (e.g., "use warehouse ID abc123 and spark version 15.4.x-scala2.12").

| Placeholder | Location | Example value |
|---|---|---|
| `<DEV_WORKSPACE_URL>` | `databricks.yml` → `targets.dev.workspace.host` | `https://my-dev.cloud.databricks.com` |
| `<PROD_WORKSPACE_URL>` | `databricks.yml` → `targets.prod.workspace.host` | `https://my-prod.cloud.databricks.com` |
| `<SERVICE_PRINCIPAL>` | `databricks.yml` → `targets.prod.run_as` | `my-deploy-sp` |
| `<SPARK_VERSION>` | `databricks.yml` → `variables.spark_version` | `15.4.x-scala2.12` |
| `<NODE_TYPE_ID>` | `databricks.yml` → `variables.node_type_id` | `i3.xlarge` (AWS), `Standard_D4s_v3` (Azure) |
| `<WAREHOUSE_ID>` | `databricks.yml` → `variables.warehouse_id` | `abc123def456` |

> **Tip:** If you've already configured auth via `~/.databrickscfg` or `DATABRICKS_HOST`, you can remove `workspace.host` from targets entirely — the CLI picks it up automatically.

## Validation

After filling in placeholders, validate the bundle before deploying:

```bash
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
| [`references/schedule-trigger-mapping.md`](references/schedule-trigger-mapping.md) | Cron conversion, sensor-to-trigger mapping, `default_args` mapping, Jinja variable conversion |
| [`references/conversion-examples.md`](references/conversion-examples.md) | 4 complete before/after examples (ETL chain, branching, sensor-triggered, multi-system) |
| [`references/hadoop-migration-guide.md`](references/hadoop-migration-guide.md) | HDFS path conversion, YARN config cleanup, Hive-to-UC mapping, spark-submit detection, Sqoop alternatives, bulk conversion guidance |
| [`assets/templates/`](assets/templates/) | Skeleton `databricks.yml` and job resource YAML templates |
| [`AGENTS.md`](AGENTS.md) | Codex CLI instruction file (same workflow as SKILL.md) |

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
