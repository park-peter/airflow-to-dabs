# Airflow DAG to DABs Conversion Examples

Complete before/after examples showing real-world Airflow DAGs converted to Databricks Asset Bundles projects.

---

## Example 1: Simple ETL Pipeline (PythonOperator Chain)

A daily ETL pipeline with three sequential Python tasks.

### Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract(**kwargs):
    source = kwargs["params"]["source_table"]
    df = spark.read.table(source)
    df.write.format("delta").mode("overwrite").saveAsTable("staging.raw_events")

def transform(**kwargs):
    df = spark.read.table("staging.raw_events")
    cleaned = df.filter(df.event_type.isNotNull()).dropDuplicates(["event_id"])
    cleaned.write.format("delta").mode("overwrite").saveAsTable("staging.cleaned_events")

def load(**kwargs):
    df = spark.read.table("staging.cleaned_events")
    df.write.format("delta").mode("append").saveAsTable("gold.events")

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ["data-team@example.com"],
    "email_on_failure": True,
}

with DAG(
    dag_id="daily_etl_pipeline",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "daily"],
) as dag:
    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract,
        params={"source_table": "bronze.raw_events"},
    )
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3
```

### DABs Output

**Directory structure:**

```
daily-etl-pipeline/
  databricks.yml
  resources/
    daily_etl_pipeline_job.yml
  src/
    extract.py
    transform.py
    load.py
```

**`databricks.yml`:**

```yaml
bundle:
  name: daily-etl-pipeline

include:
  - resources/*.yml

variables:
  spark_version:
    description: Spark runtime version
    default: "15.4.x-scala2.12"
  node_type_id:
    description: Cluster node type
    default: "i3.xlarge"

targets:
  dev:
    mode: development
  prod:
    mode: production
    run_as:
      service_principal_name: ${var.service_principal}
```

**`resources/daily_etl_pipeline_job.yml`:**

```yaml
resources:
  jobs:
    daily-etl-pipeline-job:
      name: daily-etl-pipeline
      tags:
        source: airflow-migration
        pipeline: etl
        cadence: daily
      max_concurrent_runs: 1

      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      email_notifications:
        on_failure:
          - "data-team@example.com"

      parameters:
        - name: source_table
          default: "bronze.raw_events"

      job_clusters:
        - job_cluster_key: etl-cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 2

      tasks:
        - task_key: extract
          job_cluster_key: etl-cluster
          max_retries: 2
          min_retry_interval_millis: 300000
          notebook_task:
            notebook_path: ../src/extract.py
            base_parameters:
              source_table: "{{job.parameters.source_table}}"

        - task_key: transform
          depends_on:
            - task_key: extract
          job_cluster_key: etl-cluster
          max_retries: 2
          min_retry_interval_millis: 300000
          notebook_task:
            notebook_path: ../src/transform.py

        - task_key: load
          depends_on:
            - task_key: transform
          job_cluster_key: etl-cluster
          max_retries: 2
          min_retry_interval_millis: 300000
          notebook_task:
            notebook_path: ../src/load.py
```

**`src/extract.py`:**

```python
# Databricks notebook source
dbutils.widgets.text("source_table", "bronze.raw_events")
source_table = dbutils.widgets.get("source_table")

df = spark.read.table(source_table)
df.write.format("delta").mode("overwrite").saveAsTable("staging.raw_events")
```

**`src/transform.py`:**

```python
# Databricks notebook source
df = spark.read.table("staging.raw_events")
cleaned = df.filter(df.event_type.isNotNull()).dropDuplicates(["event_id"])
cleaned.write.format("delta").mode("overwrite").saveAsTable("staging.cleaned_events")
```

**`src/load.py`:**

```python
# Databricks notebook source
df = spark.read.table("staging.cleaned_events")
df.write.format("delta").mode("append").saveAsTable("gold.events")
```

---

## Example 2: Branching DAG (BranchPythonOperator + DummyOperator)

A DAG that branches based on a parameter, with join points using DummyOperator.

### Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def choose_path(**context):
    env = context["params"]["env"]
    if env == "prod":
        return "full_validation"
    return "quick_validation"

def full_validation():
    # Full data quality checks
    pass

def quick_validation():
    # Quick smoke test
    pass

def publish_results():
    pass

with DAG(
    dag_id="branching_validation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"env": "dev"},
) as dag:
    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="choose_validation",
        python_callable=choose_path,
    )

    full = PythonOperator(task_id="full_validation", python_callable=full_validation)
    quick = PythonOperator(task_id="quick_validation", python_callable=quick_validation)

    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    publish = PythonOperator(task_id="publish_results", python_callable=publish_results)

    start >> branch >> [full, quick] >> join >> publish
```

### DABs Output

**`resources/branching_validation_job.yml`:**

```yaml
resources:
  jobs:
    branching-validation-job:
      name: branching-validation
      tags:
        source: airflow-migration

      schedule:
        quartz_cron_expression: "0 0 0 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      parameters:
        - name: env
          default: "dev"

      job_clusters:
        - job_cluster_key: validation-cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 1

      tasks:
        # "start" DummyOperator is omitted -- no upstream dependencies to rewire.
        # BranchPythonOperator becomes a condition_task.
        - task_key: choose_validation
          condition_task:
            left: "{{job.parameters.env}}"
            op: EQUAL_TO
            right: "prod"

        - task_key: full_validation
          depends_on:
            - task_key: choose_validation
              outcome: "true"
          job_cluster_key: validation-cluster
          notebook_task:
            notebook_path: ../src/full_validation.py

        - task_key: quick_validation
          depends_on:
            - task_key: choose_validation
              outcome: "false"
          job_cluster_key: validation-cluster
          notebook_task:
            notebook_path: ../src/quick_validation.py

        # "join" DummyOperator is omitted.
        # publish_results depends directly on both branches with NONE_FAILED.
        - task_key: publish_results
          depends_on:
            - task_key: full_validation
            - task_key: quick_validation
          run_if: NONE_FAILED
          job_cluster_key: validation-cluster
          notebook_task:
            notebook_path: ../src/publish_results.py
```

**Migration notes:**
- `start` (EmptyOperator) removed -- had no upstream tasks.
- `join` (EmptyOperator with `trigger_rule="none_failed_min_one_success"`) removed -- `publish_results` now depends directly on both branches with `run_if: NONE_FAILED`.
- `BranchPythonOperator` replaced with `condition_task`. Simple string equality check is supported natively.

---

## Example 3: Sensor-Triggered DAG (S3KeySensor + SQL)

A pipeline triggered by file arrival that processes data with SQL.

### Airflow DAG

```python
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def ingest_file(**context):
    # Read CSV from S3 and load into staging table
    pass

with DAG(
    dag_id="file_triggered_pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    wait_for_file = S3KeySensor(
        task_id="wait_for_upload",
        bucket_name="data-landing",
        bucket_key="incoming/*.csv",
        poke_interval=60,
        timeout=3600,
    )

    ingest = PythonOperator(
        task_id="ingest_file",
        python_callable=ingest_file,
    )

    validate = SQLExecuteQueryOperator(
        task_id="validate_data",
        conn_id="databricks_sql",
        sql="""
            SELECT COUNT(*) as error_count
            FROM staging.incoming_data
            WHERE customer_id IS NULL OR amount < 0
        """,
    )

    aggregate = SQLExecuteQueryOperator(
        task_id="aggregate_metrics",
        conn_id="databricks_sql",
        sql="""
            CREATE OR REPLACE TABLE gold.daily_metrics AS
            SELECT
                date_trunc('day', event_time) as metric_date,
                COUNT(*) as total_events,
                SUM(amount) as total_amount
            FROM staging.incoming_data
            GROUP BY 1
        """,
    )

    wait_for_file >> ingest >> validate >> aggregate
```

### DABs Output

**`resources/file_triggered_pipeline_job.yml`:**

```yaml
resources:
  jobs:
    file-triggered-pipeline-job:
      name: file-triggered-pipeline
      tags:
        source: airflow-migration

      # S3KeySensor converted to job-level file_arrival trigger.
      # The sensor task is removed; the job starts when files arrive.
      trigger:
        file_arrival:
          url: s3://data-landing/incoming/
          min_time_between_triggers_seconds: 60
          wait_after_last_change_seconds: 30

      job_clusters:
        - job_cluster_key: processing-cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 2

      tasks:
        - task_key: ingest_file
          job_cluster_key: processing-cluster
          notebook_task:
            notebook_path: ../src/ingest_file.py

        - task_key: validate_data
          depends_on:
            - task_key: ingest_file
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../src/validate_data.sql
              source: WORKSPACE

        - task_key: aggregate_metrics
          depends_on:
            - task_key: validate_data
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../src/aggregate_metrics.sql
              source: WORKSPACE
```

**`src/validate_data.sql`:**

```sql
SELECT COUNT(*) as error_count
FROM staging.incoming_data
WHERE customer_id IS NULL OR amount < 0
```

**`src/aggregate_metrics.sql`:**

```sql
CREATE OR REPLACE TABLE gold.daily_metrics AS
SELECT
    date_trunc('day', event_time) as metric_date,
    COUNT(*) as total_events,
    SUM(amount) as total_amount
FROM staging.incoming_data
GROUP BY 1
```

**Migration notes:**
- `wait_for_upload` (S3KeySensor) converted to job-level `trigger.file_arrival`. The sensor task is removed from the task list.
- `schedule_interval=None` is correct -- the job is now trigger-driven, not scheduled.
- The `url` in `file_arrival` must point to a Unity Catalog external location. Ensure the S3 path is registered as an external location.

---

## Example 4: Multi-System DAG (Spark + SQL + dbt)

A complex pipeline combining Spark processing, SQL analytics, and dbt transformations.

### Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "email": ["platform@example.com"],
    "email_on_failure": True,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="multi_system_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["production", "multi-system"],
) as dag:

    # Spark ETL
    spark_ingest = SparkSubmitOperator(
        task_id="spark_ingest",
        application="/opt/spark/jobs/ingest.py",
        conf={"spark.executor.memory": "8g", "spark.executor.cores": "4"},
        application_args=["--date", "{{ ds }}", "--source", "s3://raw-data/"],
    )

    # dbt transformations
    dbt_run = DbtRunOperator(
        task_id="dbt_transform",
        project_dir="/opt/dbt/analytics",
        profiles_dir="/opt/dbt",
        select="tag:daily",
    )

    dbt_test = DbtTestOperator(
        task_id="dbt_test",
        project_dir="/opt/dbt/analytics",
        profiles_dir="/opt/dbt",
    )

    # SQL reporting
    refresh_dashboard = SQLExecuteQueryOperator(
        task_id="refresh_dashboard_data",
        conn_id="databricks_sql",
        sql="CALL main.reporting.refresh_dashboard_materialized_views()",
    )

    # Notification
    notify = PythonOperator(
        task_id="send_completion_notice",
        python_callable=lambda: print("Pipeline complete"),
    )

    end = EmptyOperator(task_id="end")

    spark_ingest >> dbt_run >> dbt_test >> refresh_dashboard >> notify >> end
```

### DABs Output

**`resources/multi_system_pipeline_job.yml`:**

```yaml
resources:
  jobs:
    multi-system-pipeline-job:
      name: multi-system-pipeline
      tags:
        source: airflow-migration
        environment: production
        type: multi-system
      max_concurrent_runs: 1

      schedule:
        quartz_cron_expression: "0 0 6 ? * 2-6"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      email_notifications:
        on_failure:
          - "platform@example.com"

      parameters:
        - name: run_date
          default: "{{job.start_time.iso_date}}"
        - name: source_path
          default: "s3://raw-data/"

      job_clusters:
        - job_cluster_key: spark-cluster
          new_cluster:
            spark_version: ${var.spark_version}
            node_type_id: ${var.node_type_id}
            num_workers: 4
            spark_conf:
              spark.executor.memory: "8g"
              spark.executor.cores: "4"

      tasks:
        # SparkSubmitOperator -> spark_python_task
        - task_key: spark_ingest
          job_cluster_key: spark-cluster
          timeout_seconds: 7200
          max_retries: 3
          min_retry_interval_millis: 600000
          spark_python_task:
            python_file: ../src/ingest.py
            parameters:
              - "--date"
              - "{{job.parameters.run_date}}"
              - "--source"
              - "{{job.parameters.source_path}}"

        # DbtRunOperator -> dbt_task
        - task_key: dbt_transform
          depends_on:
            - task_key: spark_ingest
          job_cluster_key: spark-cluster
          timeout_seconds: 7200
          max_retries: 3
          min_retry_interval_millis: 600000
          dbt_task:
            commands:
              - "dbt deps"
              - "dbt run --select tag:daily"
            project_directory: ../dbt/analytics
            warehouse_id: ${var.warehouse_id}
          libraries:
            - pypi:
                package: "dbt-databricks>=1.0.0,<2.0.0"

        # DbtTestOperator -> dbt_task
        - task_key: dbt_test
          depends_on:
            - task_key: dbt_transform
          job_cluster_key: spark-cluster
          timeout_seconds: 7200
          max_retries: 3
          min_retry_interval_millis: 600000
          dbt_task:
            commands:
              - "dbt test"
            project_directory: ../dbt/analytics
            warehouse_id: ${var.warehouse_id}
          libraries:
            - pypi:
                package: "dbt-databricks>=1.0.0,<2.0.0"

        # SQLExecuteQueryOperator -> sql_task
        - task_key: refresh_dashboard_data
          depends_on:
            - task_key: dbt_test
          timeout_seconds: 7200
          max_retries: 3
          min_retry_interval_millis: 600000
          sql_task:
            warehouse_id: ${var.warehouse_id}
            file:
              path: ../src/refresh_dashboard_data.sql
              source: WORKSPACE

        # PythonOperator (notification) -> notebook_task
        - task_key: send_completion_notice
          depends_on:
            - task_key: refresh_dashboard_data
          job_cluster_key: spark-cluster
          notebook_task:
            notebook_path: ../src/send_completion_notice.py
          # "end" EmptyOperator is omitted -- this is the final task.
```

**`src/refresh_dashboard_data.sql`:**

```sql
CALL main.reporting.refresh_dashboard_materialized_views()
```

**`src/send_completion_notice.py`:**

```python
# Databricks notebook source
print("Pipeline complete")
```

**Migration notes:**
- `SparkSubmitOperator` `conf` values mapped to `job_clusters[].new_cluster.spark_conf`.
- `{{ ds }}` replaced with `{{job.parameters.run_date}}` and defined as a job parameter defaulting to `{{job.start_time.iso_date}}`.
- Airflow `execution_timeout=timedelta(hours=2)` mapped to `timeout_seconds: 7200` on each task.
- `retries=3` mapped to `max_retries: 3`, `retry_delay=timedelta(minutes=10)` mapped to `min_retry_interval_millis: 600000`.
- `end` (EmptyOperator) omitted -- `send_completion_notice` is the terminal task.
- Airflow weekday schedule `0 6 * * 1-5` (Mon-Fri) converted to Quartz `0 0 6 ? * 2-6` (Mon=2 through Fri=6 in Quartz).
- dbt `profiles_dir` is omitted in DABs since `warehouse_id` is used directly for authentication.
