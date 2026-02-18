# airflow-to-dabs

A coding agent skill that converts Apache Airflow DAGs into [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/) (DABs) projects.

Given an Airflow DAG file, the agent produces a complete bundle project — `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files — ready for `databricks bundle deploy`.

## Platform Support

| Platform | Instruction File | Install Path |
|----------|-----------------|--------------|
| **Cursor** | `SKILL.md` | `~/.cursor/skills/airflow-to-dabs/` |
| **Claude Code** | `SKILL.md` | `~/.claude/skills/airflow-to-dabs/` or `.claude/skills/airflow-to-dabs/` |
| **Codex CLI** | `AGENTS.md` | Skill repo: `~/.codex/skills/airflow-to-dabs/` or `.codex/skills/airflow-to-dabs/`; active instructions: `~/.codex/AGENTS.md` or `./AGENTS.md` |

## What It Does

- Parses Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Maps **30+ Airflow operator types** to DABs task type equivalents using a [tiered mapping system](references/operator-mapping.md)
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
| **1 — Direct** | 1:1 mapping to a DABs task type | `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `SQLExecuteQueryOperator`, `DbtOperator`, `TriggerDagRunOperator`, `HiveOperator`, `SSHOperator` |
| **2 — Semantic** | Requires reasoning about intent | `BranchPythonOperator`, `ShortCircuitOperator`, `SubDagOperator`, `TaskGroup`, `DummyOperator`, `EmailOperator` |
| **3 — Sensor** | Converted to job-level triggers | `S3KeySensor`, `HdfsSensor`, `FileSensor`, `ExternalTaskSensor`, `SqlSensor`, `TimeSensor` |
| **4 — Unsupported** | Flagged for manual review | Custom operators, `KubernetesPodOperator`, `SqoopOperator`, `PigOperator`, XCom-heavy patterns |

Full mapping details: [`references/operator-mapping.md`](references/operator-mapping.md)

## Installation

This skill works with **Cursor**, **Claude Code**, and **Codex CLI**. Clone the repo into the appropriate directory for your platform:

Install in either personal scope or project scope, not both, unless you intentionally want both available.

### Cursor

```bash
mkdir -p ~/.cursor/skills
if [ -d ~/.cursor/skills/airflow-to-dabs/.git ]; then
  git -C ~/.cursor/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.cursor/skills/airflow-to-dabs
fi
```

Cursor auto-discovers skills in `~/.cursor/skills/`. Triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, or DABs generation from Airflow.

### Claude Code

```bash
# Choose ONE scope.

# Personal (all projects)
mkdir -p ~/.claude/skills
if [ -d ~/.claude/skills/airflow-to-dabs/.git ]; then
  git -C ~/.claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.claude/skills/airflow-to-dabs
fi

# Project-scoped (single project)
mkdir -p .claude/skills
if [ -d .claude/skills/airflow-to-dabs/.git ]; then
  git -C .claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .claude/skills/airflow-to-dabs
fi
```

Claude Code reads the `SKILL.md` frontmatter and instructions from `.claude/skills/` or `~/.claude/skills/`.

### Codex CLI

```bash
# Choose ONE scope.

# Global (all projects)
mkdir -p ~/.codex/skills ~/.codex
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

# Project-scoped (single project)
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
    cat ./.codex/skills/airflow-to-dabs/AGENTS.md
    echo "<!-- END airflow-to-dabs -->"
  } >> ./AGENTS.md
fi
```

Codex CLI reads `AGENTS.md` from the project root or `~/.codex/`. This flow is non-destructive: it creates a timestamped backup and appends a marker-delimited skill block only if it is not already present.

## Usage

The agent follows a 4-phase workflow: **Parse** the DAG, **Map** operators to DABs task types, **Generate** the bundle project, and **Review** for correctness.

### Convert multiple DAGs (default)

> "Convert all DAGs in the dags/ directory to Databricks Asset Bundles"

Produces a single bundle with one `databricks.yml` and a separate job resource per DAG. Cross-DAG dependencies resolve within the same bundle.

### Convert a single DAG

> "Convert my_etl_dag.py to a Databricks Asset Bundle"

Produces a standalone bundle directory for that one DAG.

## Validation and QA

After generation, validate the output bundle before deployment:

```bash
databricks bundle validate -t dev
```

If Databricks auth is not configured yet, run schema checks offline and resolve structural issues first:

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
