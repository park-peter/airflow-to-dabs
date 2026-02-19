# Airflow Operator to DABs Task Type Mapping

Authoritative reference for converting Apache Airflow operators to Databricks Asset Bundles (DABs) job task types. All task types confirmed supported in DABs YAML as of Jan 2026.

---

## Tier 1: Direct 1:1 Mappings

These operators have clear, deterministic equivalents in DABs.

---

### PythonOperator / @task (TaskFlow API)

**DABs task type:** `notebook_task`

Extract the `python_callable` function body into a standalone `.py` notebook file in `src/`. Map `op_kwargs` to `base_parameters`, retrieved via `dbutils.widgets.get()` in the notebook.

**Airflow:**

```python
def extract_data(source_table, target_path):
    df = spark.read.table(source_table)
    df.write.format("delta").save(target_path)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    op_kwargs={"source_table": "raw.events", "target_path": "/mnt/silver/events"},
)
```

**DABs YAML:**

```yaml
- task_key: extract_data
  notebook_task:
    notebook_path: ../src/extract_data.py
    base_parameters:
      source_table: "raw.events"
      target_path: "/mnt/silver/events"
```

**Generated `src/extract_data.py`:**

```python
# Databricks notebook source
dbutils.widgets.text("source_table", "")
dbutils.widgets.text("target_path", "")

source_table = dbutils.widgets.get("source_table")
target_path = dbutils.widgets.get("target_path")

df = spark.read.table(source_table)
df.write.format("delta").save(target_path)
```

---

### BashOperator

**DABs task type:** `notebook_task` (general) or `spark_python_task` / `spark_jar_task` (if wrapping `spark-submit`)

For general bash commands, wrap in a notebook using `subprocess.run()`. **If the `bash_command` contains a `spark-submit` invocation**, parse it and convert to a proper `spark_python_task` or `spark_jar_task` instead. See `references/hadoop-migration-guide.md` for spark-submit detection and YARN config cleanup.

**Airflow:**

```python
cleanup = BashOperator(
    task_id="cleanup_staging",
    bash_command="rm -rf /tmp/staging/* && echo 'Staging cleaned'",
)
```

**DABs YAML:**

```yaml
- task_key: cleanup_staging
  notebook_task:
    notebook_path: ../src/cleanup_staging.py
```

**Generated `src/cleanup_staging.py`:**

```python
# Databricks notebook source
import subprocess
result = subprocess.run(
    ["bash", "-c", "rm -rf /tmp/staging/* && echo 'Staging cleaned'"],
    capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    raise RuntimeError(f"Command failed: {result.stderr}")
```

---

### SparkSubmitOperator

**DABs task type:** `spark_python_task` (for .py files) or `spark_jar_task` (for .jar files)

Map `application` to `python_file` or JAR `main_class_name`. Map Spark `conf` to cluster-level `spark_conf`.

**Airflow (Python):**

```python
spark_etl = SparkSubmitOperator(
    task_id="spark_etl",
    application="/opt/spark/jobs/etl_pipeline.py",
    conf={"spark.executor.memory": "4g", "spark.executor.cores": "2"},
    application_args=["--date", "{{ ds }}"],
)
```

**DABs YAML:**

```yaml
- task_key: spark_etl
  new_cluster:
    spark_version: "15.4.x-scala2.12"
    node_type_id: ${var.node_type_id}
    num_workers: 2
    spark_conf:
      spark.executor.memory: "4g"
      spark.executor.cores: "2"
  spark_python_task:
    python_file: ../src/etl_pipeline.py
    parameters:
      - "--date"
      - "{{job.parameters.run_date}}"
```

**Airflow (JAR):**

```python
spark_jar = SparkSubmitOperator(
    task_id="spark_jar_job",
    application="/opt/spark/jars/analytics.jar",
    java_class="com.example.Analytics",
)
```

**DABs YAML:**

```yaml
- task_key: spark_jar_job
  spark_jar_task:
    main_class_name: com.example.Analytics
  libraries:
    - jar: /Volumes/main/default/jars/analytics.jar
```

---

### DatabricksSubmitRunOperator / DatabricksSubmitRunDeferrableOperator

**DABs task type:** native DABs task (extract `json` payload directly)

The operator's `json` parameter already describes a Databricks task. Translate the JSON structure directly into DABs YAML. The deferrable variant (`DatabricksSubmitRunDeferrableOperator`) maps identically -- the deferrable behavior is an Airflow scheduler optimization that has no DABs equivalent.

**Airflow:**

```python
submit_run = DatabricksSubmitRunOperator(
    task_id="run_notebook",
    json={
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        },
        "notebook_task": {
            "notebook_path": "/Workspace/Users/user@example.com/etl",
            "base_parameters": {"env": "prod"},
        },
    },
)
```

**DABs YAML:**

```yaml
- task_key: run_notebook
  new_cluster:
    spark_version: "15.4.x-scala2.12"
    node_type_id: i3.xlarge
    num_workers: 2
  notebook_task:
    notebook_path: ../src/etl.py
    base_parameters:
      env: "prod"
```

---

### DatabricksRunNowOperator / DatabricksRunNowDeferrableOperator

**DABs task type:** `run_job_task`

Map `job_id` directly. Map `notebook_params`, `python_params`, or `jar_params` to `job_parameters`. The deferrable variant maps identically.

**Airflow:**

```python
trigger_job = DatabricksRunNowOperator(
    task_id="trigger_downstream",
    job_id=12345,
    notebook_params={"env": "prod", "date": "{{ ds }}"},
)
```

**DABs YAML:**

```yaml
- task_key: trigger_downstream
  run_job_task:
    job_id: 12345
    job_parameters:
      env: "prod"
      date: "{{job.parameters.run_date}}"
```

---

### DatabricksNotebookOperator

**DABs task type:** `notebook_task`

Direct 1:1 mapping. The operator already runs a Databricks notebook with parameters -- translate to `notebook_task` with `base_parameters`. Map `source` to the notebook path in the bundle (copy notebook into `src/` if the path is workspace-local).

Compute mapping:
- If the Airflow task uses `new_cluster`, emit `new_cluster` on the DABs task.
- If it uses `job_cluster_key` or `existing_cluster_id`, preserve that field in DABs.

**Airflow:**

```python
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator

notebook_run = DatabricksNotebookOperator(
    task_id="run_etl_notebook",
    databricks_conn_id="databricks_default",
    notebook_path="/Workspace/Users/user@example.com/etl_pipeline",
    notebook_params={"env": "prod", "date": "{{ ds }}"},
    source="WORKSPACE",
    new_cluster={
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
    },
)
```

**DABs YAML:**

```yaml
- task_key: run_etl_notebook
  new_cluster:
    spark_version: ${var.spark_version}
    node_type_id: ${var.node_type_id}
    num_workers: 2
  notebook_task:
    notebook_path: ../src/etl_pipeline.py
    source: WORKSPACE
    base_parameters:
      env: "prod"
      date: "{{job.parameters.run_date}}"
```

---

### DatabricksSqlOperator / DatabricksSQLStatementsOperator

**DABs task type:** `sql_task` (warehouse-backed) or `notebook_task`/`spark_python_task` (cluster-backed SQL)

`DatabricksSQLStatementsOperator` uses the Statement Execution API and requires a warehouse context, so it maps directly to `sql_task`.

`DatabricksSqlOperator` supports either a SQL warehouse or a Databricks cluster (`http_path`). Map by backend:
- Warehouse-backed (`sql_endpoint_name` or warehouse `http_path`) -> `sql_task`
- Cluster-backed (`http_path` for interactive cluster) -> `notebook_task`/`spark_python_task` that executes `spark.sql(...)` on cluster compute

Extract inline SQL to a `.sql` file when using `sql_task`. If SQL references an existing query ID, use `sql_task.query.query_id`.

**Airflow:**

```python
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

sql_report = DatabricksSqlOperator(
    task_id="daily_aggregation",
    databricks_conn_id="databricks_default",
    sql="""
        CREATE OR REPLACE TABLE gold.daily_metrics AS
        SELECT date, COUNT(*) as events, SUM(revenue) as total
        FROM silver.transactions
        WHERE date = '{{ ds }}'
        GROUP BY date
    """,
    http_path="/sql/1.0/warehouses/abc123",
)
```

**DABs YAML:**

```yaml
- task_key: daily_aggregation
  sql_task:
    warehouse_id: ${var.warehouse_id}
    file:
      path: ../src/daily_aggregation.sql
      source: WORKSPACE
```

**Generated `src/daily_aggregation.sql`:**

```sql
CREATE OR REPLACE TABLE gold.daily_metrics AS
SELECT date, COUNT(*) as events, SUM(revenue) as total
FROM silver.transactions
WHERE date = '{{job.parameters.run_date}}'
GROUP BY date
```

---

### DatabricksCopyIntoOperator

**DABs task type:** `sql_task` (warehouse-backed) or `notebook_task`/`spark_python_task` (cluster-backed SQL)

The operator runs a `COPY INTO` SQL command to ingest files into a Delta table.

Map by backend:
- Warehouse-backed (`sql_endpoint_name` or warehouse `http_path`) -> `sql_task` with extracted `.sql`
- Cluster-backed (`http_path` for interactive cluster) -> cluster compute task (`notebook_task`/`spark_python_task`) that runs `spark.sql("COPY INTO ...")`

**Airflow:**

```python
from airflow.providers.databricks.operators.databricks_sql import DatabricksCopyIntoOperator

ingest = DatabricksCopyIntoOperator(
    task_id="ingest_csv_data",
    databricks_conn_id="databricks_default",
    table_name="bronze.raw_events",
    file_location="s3://data-landing/events/",
    file_format="CSV",
    format_options={"header": "true", "inferSchema": "true"},
    force_copy=True,
)
```

**DABs YAML:**

```yaml
- task_key: ingest_csv_data
  sql_task:
    warehouse_id: ${var.warehouse_id}
    file:
      path: ../src/ingest_csv_data.sql
      source: WORKSPACE
```

**Generated `src/ingest_csv_data.sql`:**

```sql
COPY INTO bronze.raw_events
FROM 's3://data-landing/events/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('force' = 'true')
```

---

### SQLExecuteQueryOperator / PostgresOperator / MySqlOperator

**DABs task type:** `sql_task`

If SQL is inline, extract it to a `.sql` file and reference via `sql_task.file.path`. If it references an existing Databricks SQL query, use `sql_task.query.query_id`. Requires a `warehouse_id`.

**Airflow:**

```python
run_report = SQLExecuteQueryOperator(
    task_id="generate_report",
    conn_id="databricks_sql",
    sql="""
        CREATE OR REPLACE TABLE gold.daily_report AS
        SELECT date, SUM(revenue) as total_revenue
        FROM silver.transactions
        WHERE date = '{{ ds }}'
        GROUP BY date
    """,
)
```

**DABs YAML:**

```yaml
- task_key: generate_report
  sql_task:
    warehouse_id: ${var.warehouse_id}
    file:
      path: ../src/generate_report.sql
      source: WORKSPACE
```

**Generated `src/generate_report.sql`:**

```sql
CREATE OR REPLACE TABLE gold.daily_report AS
SELECT date, SUM(revenue) as total_revenue
FROM silver.transactions
WHERE date = '{{job.parameters.run_date}}'
GROUP BY date
```

---

### TriggerDagRunOperator

**DABs task type:** `run_job_task`

Map `trigger_dag_id` to the corresponding DABs job using bundle substitutions. Map `conf` to `job_parameters`.

**Airflow:**

```python
trigger_downstream = TriggerDagRunOperator(
    task_id="trigger_reporting_dag",
    trigger_dag_id="reporting_pipeline",
    conf={"source": "etl_pipeline"},
)
```

**DABs YAML:**

```yaml
- task_key: trigger_reporting_dag
  run_job_task:
    job_id: ${resources.jobs.reporting-pipeline-job.id}
    job_parameters:
      source: "etl_pipeline"
```

> NOTE: The target DAG must also be converted to a DABs job for `${resources.jobs...}` substitution to work. Otherwise, use a hardcoded `job_id`.

---

### DbtOperator / DbtRunOperator / DbtCloudRunJobOperator

**DABs task type:** `dbt_task`

Map dbt commands to the `commands` list and map `project_dir` to `project_directory`.

- If using Databricks SQL warehouse execution, set `warehouse_id` and omit `profiles_directory`.
- If using a custom profile-based setup, set `profiles_directory` and omit `warehouse_id`.

**Airflow:**

```python
dbt_run = DbtRunOperator(
    task_id="dbt_transform",
    project_dir="/opt/dbt/my_project",
    profiles_dir="/opt/dbt/profiles",
    select="tag:daily",
)
```

**DABs YAML:**

```yaml
- task_key: dbt_transform
  dbt_task:
    commands:
      - "dbt deps"
      - "dbt run --select tag:daily"
    project_directory: ../dbt/my_project
    warehouse_id: ${var.warehouse_id}
  libraries:
    - pypi:
        package: "dbt-databricks>=1.0.0,<2.0.0"
```

---

### HiveOperator / HivePartitionSensor (Hadoop)

**DABs task type:** `sql_task` or `notebook_task`

HiveQL queries map directly to Spark SQL via `sql_task`. Table references need conversion from `database.table` to `catalog.schema.table` (Unity Catalog). See `references/hadoop-migration-guide.md` for Hive-to-UC table mapping.

**Airflow:**

```python
from airflow.providers.apache.hive.operators.hive import HiveOperator

hive_etl = HiveOperator(
    task_id="hive_aggregate",
    hql="""
        INSERT OVERWRITE TABLE analytics.daily_summary
        SELECT date, COUNT(*) as total, SUM(amount) as revenue
        FROM events.transactions
        WHERE date = '{{ ds }}'
        GROUP BY date
    """,
    hive_cli_conn_id="hive_default",
)
```

**DABs YAML:**

```yaml
- task_key: hive_aggregate
  sql_task:
    warehouse_id: ${var.warehouse_id}
    file:
      path: ../src/hive_aggregate.sql
      source: WORKSPACE
    parameters:
      run_date: "{{job.parameters.run_date}}"
```

**Generated `src/hive_aggregate.sql`:**

```sql
-- Migrated from HiveQL. Table references updated to Unity Catalog.
INSERT INTO catalog.analytics.daily_summary
SELECT date, COUNT(*) as total, SUM(amount) as revenue
FROM catalog.events.transactions
WHERE date = '{{job.parameters.run_date}}'
GROUP BY date
```

> NOTE: `INSERT OVERWRITE TABLE` should be converted to `INSERT INTO` with `CREATE OR REPLACE TABLE` or `MERGE` depending on the use case. Delta tables do not support `INSERT OVERWRITE` in the same way as Hive.

---

### SSHOperator (Hadoop Edge Node)

**DABs task type:** `spark_python_task`, `spark_jar_task`, or `notebook_task`

SSHOperator is commonly used to SSH into a Hadoop edge node and run `spark-submit`. Extract the remote command and convert it to a direct DABs task. The SSH hop is eliminated since Databricks runs Spark natively. See `references/hadoop-migration-guide.md` for spark-submit parsing.

**Airflow:**

```python
from airflow.providers.ssh.operators.ssh import SSHOperator

ssh_spark = SSHOperator(
    task_id="run_spark_on_hadoop",
    ssh_conn_id="hadoop_edge",
    command="spark-submit --master yarn --class com.example.ETL /opt/jars/etl.jar --date {{ ds }}",
)
```

**DABs YAML:**

```yaml
- task_key: run_spark_on_hadoop
  spark_jar_task:
    main_class_name: com.example.ETL
    parameters:
      - "--date"
      - "{{job.parameters.run_date}}"
  libraries:
    - jar: /Volumes/main/default/libs/etl.jar
```

---

## Tier 2: Semantic Mappings (Require Interpretation)

These operators require reasoning about intent to determine the best DABs equivalent.

---

### BranchPythonOperator / ShortCircuitOperator

**DABs task type:** `condition_task` + `depends_on` with `outcome`

For simple comparisons, map directly to `condition_task` fields (`left`, `op`, `right`). For complex logic, split into a `notebook_task` that sets a task value, followed by a `condition_task` that reads that value.

**Airflow:**

```python
def choose_branch(**context):
    if context["params"]["env"] == "prod":
        return "run_full_pipeline"
    return "run_sample_pipeline"

branch = BranchPythonOperator(
    task_id="check_environment",
    python_callable=choose_branch,
)
```

**DABs YAML (simple case):**

```yaml
- task_key: check_environment
  condition_task:
    left: "{{job.parameters.env}}"
    op: EQUAL_TO
    right: "prod"

- task_key: run_full_pipeline
  depends_on:
    - task_key: check_environment
      outcome: "true"
  notebook_task:
    notebook_path: ../src/full_pipeline.py

- task_key: run_sample_pipeline
  depends_on:
    - task_key: check_environment
      outcome: "false"
  notebook_task:
    notebook_path: ../src/sample_pipeline.py
```

**DABs YAML (complex logic -- two-step pattern):**

```yaml
- task_key: evaluate_branch
  notebook_task:
    notebook_path: ../src/evaluate_branch.py

- task_key: check_branch_result
  depends_on:
    - task_key: evaluate_branch
  condition_task:
    left: "{{tasks.evaluate_branch.values.branch_decision}}"
    op: EQUAL_TO
    right: "full"

- task_key: run_full_pipeline
  depends_on:
    - task_key: check_branch_result
      outcome: "true"
  notebook_task:
    notebook_path: ../src/full_pipeline.py
```

---

### PythonVirtualenvOperator / ExternalPythonOperator

**DABs task type:** `python_wheel_task` or `notebook_task`

If the function has custom dependencies, package it as a Python wheel with an `entry_point`. For simpler cases, use a `notebook_task` with `%pip install` commands at the top.

**DABs YAML (wheel approach):**

```yaml
- task_key: custom_transform
  python_wheel_task:
    entry_point: run
    package_name: custom_transform
  libraries:
    - whl: ../dist/custom_transform-*.whl
```

**DABs YAML (notebook approach):**

```yaml
- task_key: custom_transform
  notebook_task:
    notebook_path: ../src/custom_transform.py
```

**Generated `src/custom_transform.py`:**

```python
# Databricks notebook source
# COMMAND ----------
%pip install pandas==2.1.0 scikit-learn==1.3.0
# COMMAND ----------
import pandas as pd
from sklearn.preprocessing import StandardScaler
# ... extracted function body ...
```

---

### SubDagOperator / TaskGroup

**DABs equivalent:** flatten into individual tasks with `depends_on` chains, or extract to a separate job via `run_job_task`.

Flatten the nested tasks into the parent job, preserving dependency order. Prefix task keys with the group name for clarity.

**Airflow:**

```python
with TaskGroup("data_quality") as quality_group:
    check_nulls = PythonOperator(task_id="check_nulls", ...)
    check_schema = PythonOperator(task_id="check_schema", ...)
    check_nulls >> check_schema
```

**DABs YAML:**

```yaml
- task_key: data_quality__check_nulls
  notebook_task:
    notebook_path: ../src/check_nulls.py

- task_key: data_quality__check_schema
  depends_on:
    - task_key: data_quality__check_nulls
  notebook_task:
    notebook_path: ../src/check_schema.py
```

---

### DummyOperator / EmptyOperator

**DABs equivalent:** omit entirely.

Rewire `depends_on` references so that tasks downstream of the DummyOperator depend directly on its upstream tasks instead.

**Airflow:**

```python
start = DummyOperator(task_id="start")
end = DummyOperator(task_id="end")
start >> [task_a, task_b] >> end >> task_c
```

**DABs YAML:**

```yaml
# "start" and "end" are omitted. Dependencies are rewired.
- task_key: task_a
  notebook_task:
    notebook_path: ../src/task_a.py

- task_key: task_b
  notebook_task:
    notebook_path: ../src/task_b.py

- task_key: task_c
  depends_on:
    - task_key: task_a
    - task_key: task_b
  notebook_task:
    notebook_path: ../src/task_c.py
```

---

### EmailOperator

**DABs equivalent:** `email_notifications` at job or task level (not a standalone task type).

**DABs YAML:**

```yaml
# Applied at the job level or individual task level
email_notifications:
  on_success:
    - "team@example.com"
  on_failure:
    - "oncall@example.com"
```

---

### DatabricksWorkflowTaskGroup / DatabricksTaskOperator

**DABs equivalent:** flatten into individual DABs job tasks.

`DatabricksWorkflowTaskGroup` defines a multi-task Databricks workflow within Airflow, with each task defined by `DatabricksTaskOperator`. This is the closest Airflow construct to a DABs job. Each `DatabricksTaskOperator` already specifies a Databricks task type (`notebook_task`, `spark_python_task`, etc.), so the migration is nearly 1:1: extract each child task into a DABs task entry, preserve `depends_on` relationships, and map the group's shared cluster to a `job_cluster_key`.

**Airflow:**

```python
from airflow.providers.databricks.operators.databricks import DatabricksTaskOperator
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup

with DatabricksWorkflowTaskGroup(
    group_id="etl_workflow",
    databricks_conn_id="databricks_default",
    job_clusters=[{
        "job_cluster_key": "etl_cluster",
        "new_cluster": {
            "spark_version": "15.4.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 4,
        },
    }],
) as wf:
    extract = DatabricksTaskOperator(
        task_id="extract",
        notebook_task={"notebook_path": "/Workspace/etl/extract"},
        job_cluster_key="etl_cluster",
    )
    transform = DatabricksTaskOperator(
        task_id="transform",
        notebook_task={"notebook_path": "/Workspace/etl/transform"},
        job_cluster_key="etl_cluster",
    )
    load = DatabricksTaskOperator(
        task_id="load",
        notebook_task={"notebook_path": "/Workspace/etl/load"},
        job_cluster_key="etl_cluster",
    )
    extract >> transform >> load
```

**DABs YAML:**

```yaml
job_clusters:
  - job_cluster_key: etl_cluster
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      node_type_id: ${var.node_type_id}
      num_workers: 4

tasks:
  - task_key: extract
    job_cluster_key: etl_cluster
    notebook_task:
      notebook_path: ../src/extract.py

  - task_key: transform
    depends_on:
      - task_key: extract
    job_cluster_key: etl_cluster
    notebook_task:
      notebook_path: ../src/transform.py

  - task_key: load
    depends_on:
      - task_key: transform
    job_cluster_key: etl_cluster
    notebook_task:
      notebook_path: ../src/load.py
```

---

### DatabricksCreateJobsOperator

**DABs equivalent:** absorbed by `databricks bundle deploy` — omit from job tasks.

This operator programmatically creates Databricks jobs via the Jobs API. In a DABs migration, the job definition itself is the bundle YAML. Remove `DatabricksCreateJobsOperator` tasks from the task graph and instead ensure the job configuration from its `json` parameter is reflected in the generated `resources/<job>_job.yml`. Add a note to `MIGRATION_NOTES.md` explaining that job creation is now handled by `databricks bundle deploy`.

---

### DatabricksReposCreateOperator / DatabricksReposUpdateOperator / DatabricksReposDeleteOperator

**DABs equivalent:** not applicable — infrastructure/repo management, not a job task.

These operators manage Databricks Repos (Git integration). They have no equivalent as DABs job tasks. If a DAG uses these to sync code before running notebooks, note in `MIGRATION_NOTES.md` that DABs handles code deployment natively via `databricks bundle deploy`. Remove these tasks from the job definition.

---

## Tier 3: Sensor to Trigger Mappings

Airflow sensors that wait for external conditions map to DABs job-level triggers.

---

### DatabricksSqlSensor / DatabricksSQLStatementsSensor

**DABs equivalent:** `depends_on`, `trigger.table_update`, or polling task (intent-dependent)

These sensors are blocking/wait semantics in Airflow, so they do not always become triggers.

Use this decision order:
1. If waiting on a statement already submitted by an upstream task (`DatabricksSQLStatementsSensor` with `statement_id`), convert to a normal task dependency (`depends_on`) on that upstream task. Do not convert to a trigger.
2. If the sensor is effectively waiting for external table freshness or table updates, convert to `trigger.table_update`.
3. If it checks an arbitrary SQL condition (for example, a business rule or feature flag), convert to a polling `notebook_task` (or `spark_python_task`) with timeout handling.

Note: `DatabricksSQLStatementsSensor` can either submit a statement (`statement`) or wait on an existing statement (`statement_id`); preserve that intent during conversion.

**Airflow:**

```python
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor

wait_for_data = DatabricksSqlSensor(
    task_id="wait_for_daily_data",
    databricks_conn_id="databricks_default",
    sql="SELECT COUNT(*) FROM silver.transactions WHERE date = '{{ ds }}'",
    success=lambda result: result[0][0] > 0,
    poke_interval=300,
    timeout=3600,
)
```

**DABs YAML (table trigger):**

```yaml
trigger:
  table_update:
    condition: ANY_UPDATED
    table_names:
      - "main.silver.transactions"
    min_time_between_triggers_seconds: 300
```

> NOTE: If using `statement_id`, prefer `depends_on` over triggers. If the sensor checks an arbitrary SQL condition (not table freshness), convert to a polling compute task that raises on timeout. Add this decision to `MIGRATION_NOTES.md`.

---

### DatabricksPartitionSensor

**DABs equivalent:** `trigger.table_update` or polling `notebook_task`

Waits for a specific partition to appear in a Delta table. If the table is managed via Unity Catalog, convert to `trigger.table_update`. For complex partition-level checks, use a polling `notebook_task`.

**Airflow:**

```python
from airflow.providers.databricks.sensors.databricks_partition import DatabricksPartitionSensor

wait_for_partition = DatabricksPartitionSensor(
    task_id="wait_for_partition",
    databricks_conn_id="databricks_default",
    table_name="main.silver.events",
    partitions={"date": "2024-01-15"},
    poke_interval=300,
    timeout=3600,
)
```

**DABs YAML (table trigger):**

```yaml
trigger:
  table_update:
    condition: ANY_UPDATED
    table_names:
      - "main.silver.events"
    min_time_between_triggers_seconds: 300
```

> NOTE: DABs `trigger.table_update` fires on any table update, not partition-specific changes. If partition-level precision is required, use a polling `notebook_task` that checks `DESCRIBE DETAIL` or partition metadata. Add to `MIGRATION_NOTES.md`.

---

### HdfsSensor / WebHdfsSensor (Hadoop)

**DABs equivalent:** job-level `trigger.file_arrival`

HDFS file sensors wait for files to land on HDFS. After migrating to cloud storage, these convert to `trigger.file_arrival` pointing at the equivalent cloud path or UC external location. The HDFS path must be mapped to its cloud storage equivalent first. See `references/hadoop-migration-guide.md`.

**Airflow:**

```python
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor

wait_for_data = HdfsSensor(
    task_id="wait_for_hdfs_file",
    filepath="/data/landing/{{ ds }}/*.parquet",
    hdfs_conn_id="hdfs_default",
    poke_interval=120,
    timeout=3600,
)
```

**DABs YAML (job-level trigger):**

```yaml
trigger:
  file_arrival:
    url: s3://datalake-bucket/data/landing/
    min_time_between_triggers_seconds: 120
    wait_after_last_change_seconds: 60
```

> NOTE: The HDFS path `/data/landing/` must be mapped to its cloud storage equivalent. Add to MIGRATION_NOTES.md.

---

### S3KeySensor / GCSObjectExistenceSensor

**DABs equivalent:** job-level `trigger.file_arrival`

The sensor's bucket/key path maps to a Unity Catalog external location or volume URL.

**Airflow:**

```python
wait_for_file = S3KeySensor(
    task_id="wait_for_upload",
    bucket_name="data-landing",
    bucket_key="incoming/{{ ds }}/*.csv",
    poke_interval=60,
    timeout=3600,
)
```

**DABs YAML (job-level trigger):**

```yaml
resources:
  jobs:
    process_upload_job:
      name: process-upload-job
      trigger:
        file_arrival:
          url: s3://data-landing/incoming/
          min_time_between_triggers_seconds: 60
      tasks:
        - task_key: process_upload
          notebook_task:
            notebook_path: ../src/process_upload.py
```

---

### ExternalTaskSensor

**DABs equivalent:** `depends_on` (same job), `run_job_task` (cross-job), or `trigger.table_update`

**Same job:** use `depends_on` on the task key.
**Cross-job, table-driven:** use `trigger.table_update` to fire when a table is updated by the upstream job.
**Cross-job, explicit:** use `run_job_task` in the upstream job to chain them.

**DABs YAML (table trigger):**

```yaml
resources:
  jobs:
    downstream_job:
      name: downstream-job
      trigger:
        table_update:
          condition: ANY_UPDATED
          table_names:
            - "main.silver.transactions"
          min_time_between_triggers_seconds: 300
      tasks:
        - task_key: process_transactions
          notebook_task:
            notebook_path: ../src/process_transactions.py
```

---

### SqlSensor

**DABs equivalent:** `trigger.table_update` or `notebook_task` with polling logic.

If the SQL checks for table row existence or freshness, convert to a `trigger.table_update`. If the SQL checks an arbitrary condition, wrap it in a `notebook_task`.

---

### FileSensor

**DABs equivalent:** `trigger.file_arrival`

Same pattern as S3KeySensor -- map the file path to a Unity Catalog volume or external location URL.

---

### TimeSensor / TimeDeltaSensor

**DABs equivalent:** absorbed into `schedule.quartz_cron_expression`.

These sensors delay execution until a certain time. In DABs, schedule the job to run at that time directly using a cron expression. If the sensor is mid-pipeline (not at the start), note this in MIGRATION_NOTES.md as requiring manual handling.

---

## Tier 4: Unsupported / Manual Review Required

These operators have no direct DABs equivalent. Flag them in `MIGRATION_NOTES.md`.

| Airflow Operator | Suggested Fallback | Notes |
|---|---|---|
| Custom `BaseOperator` subclass | `notebook_task` | Extract operator logic into a notebook. Review `execute()` method. |
| `KubernetesPodOperator` | `notebook_task` or external API | No container orchestration in Lakeflow Jobs. Consider Databricks container services or calling an external API. |
| `DockerOperator` | `notebook_task` or external API | Same as above. |
| `HttpSensor` / `SimpleHttpOperator` | `notebook_task` wrapping `requests` | Use a notebook with the `requests` library for HTTP calls. |
| `LivyOperator` | `spark_python_task` or `notebook_task` | Livy is unnecessary on Databricks; submit Spark code directly. See `hadoop-migration-guide.md`. |
| `SqoopOperator` (import) | Lakeflow Connect pipeline | Managed RDBMS-to-lakehouse ingestion with CDC. Not a DABs task -- create a pipeline resource. See `hadoop-migration-guide.md`. |
| `SqoopOperator` (export) | `notebook_task` with JDBC write | `df.write.format("jdbc")` in a notebook. See `hadoop-migration-guide.md`. |
| `PigOperator` | `notebook_task` or `sql_task` | Rewrite Pig Latin scripts as Spark SQL or PySpark. No Pig runtime on Databricks. |
| `BashOperator` (wrapping `spark-submit`) | `spark_python_task` or `spark_jar_task` | Parse the spark-submit command and convert. See `hadoop-migration-guide.md`. |
| `SSHOperator` (wrapping `spark-submit`) | `spark_python_task` or `spark_jar_task` | Extract the remote command. SSH hop is eliminated. See `hadoop-migration-guide.md`. |
| Airflow dynamic task mapping (`expand`, mapped TaskFlow tasks) | `for_each_task` | Convert mapped fan-out to `for_each_task` with a deterministic JSON array input, or flag for manual review if mapping logic is dynamic/non-deterministic at parse time. |
| XCom-heavy patterns | `dbutils.jobs.taskValues` | Replace `xcom_push`/`xcom_pull` with `dbutils.jobs.taskValues.set()` and dynamic value references `{{tasks.<key>.values.<name>}}`. |
| Airflow Variables | DABs variables or job parameters | Replace `Variable.get()` with `${var.<name>}` in YAML or `dbutils.widgets.get()` in notebooks. |
| Airflow Connections | Databricks secrets or UC connections | Replace `BaseHook.get_connection()` with `dbutils.secrets.get()` or Unity Catalog connection references. |
