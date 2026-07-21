"""Airflow 3 DAG demonstrating TaskFlow dataflow, dynamic task mapping, and a
mapped task group over a collection.

Authored against the Airflow 3 surface on purpose:
- Task SDK imports (`from airflow.sdk import ...`).
- A common operator from the standard provider (`airflow.providers.standard.*`).
- Asset-based scheduling with an explicit Databricks-table binding in `extra`.
"""

from __future__ import annotations

from airflow.sdk import Asset, dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

# The Asset URI is an arbitrary string; the target Unity Catalog table is stated
# unambiguously in extra["databricks_table"] so the converter can bind the
# schedule to a trigger.table_update instead of flagging it.
ORDERS_RAW = Asset(
    "orders-raw",
    extra={"databricks_table": "main.commerce.orders_raw"},
)

REGIONS = ["us", "eu", "apac"]
CHECKSUM_TABLES = ["orders", "customers", "returns"]


@task(multiple_outputs=True)
def plan_run(regions: list[str]) -> dict:
    """Return several named values consumed downstream by name (multiple_outputs)."""
    return {"regions": regions, "batch_size": len(regions)}


@task
def announce(batch_size: int) -> str:
    """Consumes plan_run's `batch_size` output; returns a single value."""
    return f"starting run over {batch_size} regions"


@task
def checksum(table: str, catalog: str) -> None:
    """Dynamically mapped over CHECKSUM_TABLES; `catalog` is a partial (constant)."""
    print(f"checksum {catalog}.{table}")


@task
def ingest(region: str) -> str:
    print(f"ingest {region}")
    return f"main.commerce.silver_orders_{region}"


@task
def validate(silver_table: str) -> str:
    print(f"validate {silver_table}")
    return silver_table


@task
def publish(silver_table: str) -> None:
    print(f"publish gold from {silver_table}")


@task_group
def region_pipeline(region: str):
    """A multi-step subgraph fanned out per region via .expand()."""
    publish(validate(ingest(region)))


@dag(
    dag_id="regional_ingest",
    schedule=[ORDERS_RAW],
    catchup=False,
    tags=["orders", "airflow-to-dabs-demo", "airflow3"],
)
def regional_ingest():
    start = EmptyOperator(task_id="start")

    # TaskFlow dataflow on a NON-mapped chain: plan_run -> announce.
    # multiple_outputs is shown here (not on the mapped checksum task) because a
    # mapped task's per-iteration outputs cannot be consumed downstream.
    plan = plan_run(regions=REGIONS)
    note = announce(batch_size=plan["batch_size"])

    # Dynamic task mapping: one checksum per table, catalog held constant.
    checks = checksum.partial(catalog="main").expand(table=CHECKSUM_TABLES)

    # Mapped task group over a collection: the ingest->validate->publish subgraph
    # runs once per region.
    regions = region_pipeline.expand(region=REGIONS)

    start >> plan
    note >> regions
    checks >> regions


regional_ingest()
