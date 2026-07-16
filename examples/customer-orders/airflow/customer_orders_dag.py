from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def ingest_bronze(source_path: str, bronze_table: str, run_date: str, **_context) -> None:
    print(f"Ingesting orders for {run_date} from {source_path} into {bronze_table}")


def transform_silver(bronze_table: str, silver_table: str, run_date: str, **_context) -> None:
    print(f"Transforming {bronze_table} into {silver_table} for {run_date}")


def choose_validation_path(**context) -> str:
    return "full_validation" if context["params"]["run_full_validation"] else "skip_full_validation"


def full_validation(silver_table: str, run_date: str, **_context) -> None:
    print(f"Running full validation on {silver_table} for {run_date}")


def publish_gold(silver_table: str, gold_table: str, run_date: str, **_context) -> None:
    print(f"Publishing {silver_table} into {gold_table} for {run_date}")


with DAG(
    dag_id="customer_orders_airflow",
    description="Example Airflow DAG for customer order ingestion and publishing.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1, tzinfo=pendulum.timezone("UTC")),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["orders", "airflow-to-dabs-demo"],
    params={
        "catalog": "main",
        "schema": "commerce",
        "run_full_validation": True,
        "min_daily_orders": 100,
    },
) as dag:
    wait_for_orders = S3KeySensor(
        task_id="wait_for_orders",
        bucket_name="company-landing",
        bucket_key="orders/{{ ds }}/*.json",
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    ingest_bronze_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
        op_kwargs={
            "source_path": "s3://company-landing/orders/{{ ds }}/",
            "bronze_table": "{{ params.catalog }}.{{ params.schema }}.bronze_orders_raw",
            "run_date": "{{ ds }}",
        },
    )

    transform_silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
        op_kwargs={
            "bronze_table": "{{ params.catalog }}.{{ params.schema }}.bronze_orders_raw",
            "silver_table": "{{ params.catalog }}.{{ params.schema }}.silver_orders",
            "run_date": "{{ ds }}",
        },
    )

    dq_order_totals = SQLExecuteQueryOperator(
        task_id="dq_order_totals",
        conn_id="databricks_sql",
        sql="""
        SELECT
          CASE
            WHEN COUNT(*) >= {{ params.min_daily_orders }} THEN 1
            ELSE RAISE_ERROR('Daily order volume below threshold')
          END AS passed
        FROM {{ params.catalog }}.{{ params.schema }}.silver_orders
        WHERE order_date = DATE '{{ ds }}'
        """,
    )

    choose_validation = BranchPythonOperator(
        task_id="choose_validation",
        python_callable=choose_validation_path,
    )

    full_validation_task = PythonOperator(
        task_id="full_validation",
        python_callable=full_validation,
        op_kwargs={
            "silver_table": "{{ params.catalog }}.{{ params.schema }}.silver_orders",
            "run_date": "{{ ds }}",
        },
    )

    skip_full_validation = EmptyOperator(task_id="skip_full_validation")

    publish_gold_task = PythonOperator(
        task_id="publish_gold",
        python_callable=publish_gold,
        trigger_rule="none_failed_min_one_success",
        op_kwargs={
            "silver_table": "{{ params.catalog }}.{{ params.schema }}.silver_orders",
            "gold_table": "{{ params.catalog }}.{{ params.schema }}.gold_daily_order_summary",
            "run_date": "{{ ds }}",
        },
    )

    wait_for_orders >> ingest_bronze_task >> transform_silver_task >> dq_order_totals >> choose_validation
    choose_validation >> [full_validation_task, skip_full_validation] >> publish_gold_task
