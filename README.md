# airflow-to-dabs

A coding agent skill that converts Apache Airflow DAGs into [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/) (DABs) projects.

Given an Airflow DAG file, the agent produces a complete bundle project — `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files — ready for `databricks bundle deploy`.

## What It Does

- Parses Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Maps **30+ Airflow operator types** to DABs task type equivalents using a [tiered mapping system](references/operator-mapping.md)
- Converts Airflow cron expressions and presets to Quartz cron format
- Converts Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers
- Extracts inline Python, SQL, and bash into standalone source files
- Converts Jinja template variables (`{{ ds }}`, `{{ params.x }}`) to DABs dynamic value references
- Generates `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- **Hadoop/HDFS migration support**: detects `spark-submit` in BashOperator/SSHOperator, cleans up YARN configs, maps HDFS paths, converts HiveQL, handles Sqoop alternatives

## Operator Coverage

| Tier | Description | Examples |
|------|-------------|----------|
| **1 — Direct** | 1:1 mapping to a DABs task type | `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `SQLExecuteQueryOperator`, `DbtOperator`, `TriggerDagRunOperator`, `HiveOperator`, `SSHOperator` |
| **2 — Semantic** | Requires reasoning about intent | `BranchPythonOperator`, `ShortCircuitOperator`, `SubDagOperator`, `TaskGroup`, `DummyOperator`, `EmailOperator` |
| **3 — Sensor** | Converted to job-level triggers | `S3KeySensor`, `HdfsSensor`, `FileSensor`, `ExternalTaskSensor`, `SqlSensor`, `TimeSensor` |
| **4 — Unsupported** | Flagged for manual review | Custom operators, `KubernetesPodOperator`, `SqoopOperator`, `PigOperator`, XCom-heavy patterns |

Full mapping details: [`references/operator-mapping.md`](references/operator-mapping.md)

## Installation

Clone this repo into your Cursor skills directory:

```bash
git clone https://github.com/park-peter/airflow-to-dabs.git ~/.cursor/skills/airflow-to-dabs
```

The skill will be automatically discovered by Cursor. It triggers on mentions of Airflow migration, DAG conversion, Airflow to Databricks, or DABs generation from Airflow.

## Usage

In Cursor, provide an Airflow DAG file and ask the agent to convert it:

> "Convert this Airflow DAG to Databricks Asset Bundles"

The agent follows a 4-phase workflow:

1. **Parse** — Read the DAG and extract tasks, dependencies, operators, schedule
2. **Map** — Apply the operator mapping reference to determine DABs task types
3. **Generate** — Produce the full DABs project (YAML configs + source files)
4. **Review** — Validate dependencies, flag manual items, present a summary

## Reference Files

| File | Description |
|------|-------------|
| [`references/operator-mapping.md`](references/operator-mapping.md) | Tier 1–4 mapping table with side-by-side Airflow/DABs YAML examples |
| [`references/dab-schema-reference.md`](references/dab-schema-reference.md) | Condensed DABs YAML schema — all task types, triggers, clusters, variables |
| [`references/schedule-trigger-mapping.md`](references/schedule-trigger-mapping.md) | Cron conversion, sensor-to-trigger mapping, `default_args` mapping, Jinja variable conversion |
| [`references/conversion-examples.md`](references/conversion-examples.md) | 4 complete before/after examples (ETL chain, branching, sensor-triggered, multi-system) |
| [`references/hadoop-migration-guide.md`](references/hadoop-migration-guide.md) | HDFS path conversion, YARN config cleanup, Hive-to-UC mapping, spark-submit detection, Sqoop alternatives, bulk conversion guidance |
| [`assets/templates/`](assets/templates/) | Skeleton `databricks.yml` and job resource YAML templates |

## Example Output

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

See [`references/conversion-examples.md`](references/conversion-examples.md) for full before/after walkthroughs.

## License

MIT
