"""Airflow 3 DAG that replicates a Snowflake table into the lakehouse on a
schedule, then runs a downstream transform.

This is the shape that maps to Lakeflow Connect: a *recurring* ingestion from a
federatable source (Snowflake) into Delta, followed by a transform. Snowflake has
no dedicated managed connector, so it ingests via a Unity Catalog *foreign
catalog* (query-based ingestion), and the transform hangs off the ingestion
pipeline via a pipeline_task hop.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator


@task
def transform_orders(catalog: str, schema: str) -> None:
    print(f"building {catalog}.{schema}.gold_orders from replicated raw_orders")


@dag(
    dag_id="orders_replication",
    schedule="0 * * * *",
    catchup=False,
    tags=["orders", "airflow-to-dabs-demo", "lakeflow-connect"],
)
def orders_replication():
    # Recurring Snowflake -> lakehouse replication. In Airflow this is a query
    # against Snowflake landing into a staging table; the migration replaces it
    # with a Lakeflow Connect foreign-catalog ingestion pipeline (see the bundle).
    replicate = SnowflakeSqlApiOperator(
        task_id="replicate_orders",
        snowflake_conn_id="snowflake_default",
        sql="SELECT * FROM analytics.public.orders",
    )

    replicate >> transform_orders(catalog="main", schema="commerce")


orders_replication()
