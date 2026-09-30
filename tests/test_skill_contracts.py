"""Structural checks for the skill's behavior-shaping rules.

`SKILL.md` is the single instruction surface; it defers detail to `references/`. These
tests check two things that survive rewording:

* every hardening rule is carried by `SKILL.md`, matched on `<!-- contract: id -->`
  anchors rather than on sentences;
* the parts with machine-readable structure (bundle YAML, Make recipes, code fences,
  operator sections, plugin manifests) assert on that structure.

Run with: make test
"""

from __future__ import annotations

import ast
import json
import re
from datetime import date, timedelta
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
# Every rule an agent must apply. Removing a rule's anchor from SKILL.md fails here.
REQUIRED_CONTRACTS = frozenset(
    {
        "bundles-product-name",
        "no-guessed-executable-default",
        "branch-datetime-dayofweek",
        "constant-sensors",
        "file-arrival-queue",
        "recursive-listing",
        "mixed-schedule-manual",
        "soft-fail-condition-gate",
        "retained-sensor-poke",
        "spark-python-plain-script",
        "lifecycle-retry-disclosure",
        "manifest-recipe-guard",
        "required-var-not-a-fix",
        "dataset-or-time-schedule",
        "dataproc-payload-routing",
        "dbt-intersected-selector",
    }
)

_ANCHOR = re.compile(r"<!--\s*contract:\s*([a-z0-9-]+)\s*-->")
_FENCED_PYTHON = re.compile(r"```python\n(.*?)```", re.DOTALL)
_FENCED_YAML = re.compile(r"```yaml\n(.*?)```", re.DOTALL)


def _text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _anchors(relative_path: str) -> set[str]:
    return set(_ANCHOR.findall(_text(relative_path)))


def test_skill_frontmatter_carries_only_the_discovery_keys():
    # Claude/Cursor skill frontmatter supports `name` and `description`; the version of
    # record is the git tag.
    body = _text("SKILL.md")
    assert body.startswith("---\n")
    frontmatter = yaml.safe_load(body.split("---\n", 2)[1])

    assert set(frontmatter) == {"name", "description"}
    assert "Declarative Automation Bundles" in frontmatter["description"]


def test_plugin_manifests_install_the_root_skill():
    # The repo is its own single-plugin marketplace; the root SKILL.md loads as the plugin's
    # only skill. With no manifest version, installs are keyed to the git commit.
    skill_name = yaml.safe_load(_text("SKILL.md").split("---\n", 2)[1])["name"]
    plugin = json.loads(_text(".claude-plugin/plugin.json"))
    marketplace = json.loads(_text(".claude-plugin/marketplace.json"))
    [entry] = marketplace["plugins"]

    assert plugin["name"] == skill_name
    assert entry["name"] == skill_name
    assert entry["source"] == "./"
    assert "version" not in plugin
    assert "version" not in entry
    assert not (ROOT / "skills").exists(), "a skills/ directory would replace the root SKILL.md"


def test_skill_carries_every_hardening_contract():
    missing = sorted(REQUIRED_CONTRACTS - _anchors("SKILL.md"))
    assert not missing, f"SKILL.md is missing contracts: {missing}"


def test_skill_declares_no_unknown_contract():
    unknown = sorted(_anchors("SKILL.md") - REQUIRED_CONTRACTS)
    assert not unknown, f"SKILL.md declares unknown contracts: {unknown}"


def test_recursive_listing_helper_is_bounded_and_portable():
    for relative_path in ("references/operator-mapping.md",):
        blocks = [b for b in _FENCED_PYTHON.findall(_text(relative_path)) if "list_files_recursive" in b]
        assert blocks, f"{relative_path} has no list_files_recursive definition"
        for block in blocks:
            code = "\n".join(
                line for line in block.splitlines() if not line.lstrip().startswith("#")
            )
            assert "entry.isDir()" not in code, f"{relative_path}: isDir() is absent from the SDK FileInfo"
            assert 'endswith("/")' in code, f"{relative_path}: directory test must use the trailing slash"
            assert "TimeoutError" in code, f"{relative_path}: listing walk needs a timeout"
            assert "MAX_FILES" in code, f"{relative_path}: listing walk needs a size bound"


def test_documented_file_arrival_jobs_enable_queueing():
    # A file_arrival trigger without `queue` drops arrivals detected at the concurrency limit.
    for relative_path in (
        "references/schedule-trigger-mapping.md",
        "references/dab-schema-reference.md",
        "assets/templates/job-resource.yml.tmpl",
    ):
        body = _text(relative_path)
        for block in _FENCED_YAML.findall(body) or [body]:
            if "file_arrival:" in block:
                assert "queue:" in block, f"{relative_path}: file_arrival block without queue"


def test_checked_in_file_arrival_job_enables_queueing():
    resource = yaml.safe_load(
        _text("examples/customer-orders/customer_orders_bundle/resources/customer_orders_job.yml")
    )
    job = resource["resources"]["jobs"]["customer_orders_lakeflow_job"]

    assert "file_arrival" in job["trigger"]
    assert job["queue"] == {"enabled": True}


def test_widened_ingestion_scope_is_disclosed_in_the_example_notes():
    # The source sensor globs one partition (`orders/<ds>/*.json`) while the trigger and
    # Auto Loader read the whole landing prefix, so the notes must name the scope
    # difference and say `_ingest_run_date` labels the run instead of filtering it.
    dag = _text("examples/customer-orders/airflow/customer_orders_dag.py")
    ingestion = _text("examples/customer-orders/customer_orders_bundle/src/ingest_bronze.py")
    notes = _text("examples/customer-orders/customer_orders_bundle/MIGRATION_NOTES.md")

    assert 'bucket_key="orders/{{ ds }}/*.json"' in dag
    assert '.option("pathGlobFilter", "*.json")' in ingestion

    scope_row = next(line for line in notes.splitlines() if line.startswith("| File discovery scope"))
    assert "basename" in scope_row
    assert "not a partition filter" in scope_row
    assert "_ingest_run_date" in scope_row


def test_generated_job_template_enables_queueing_by_default():
    template = yaml.safe_load(_text("assets/templates/job-resource.yml.tmpl"))
    job = next(iter(template["resources"]["jobs"].values()))

    assert job["queue"] == {"enabled": True}


def test_operator_sections_declare_a_databricks_mapping():
    # Family sections cover several operators through a decision tree and route to the
    # per-operator sections instead of naming one task type.
    family_sections = {
        "Snowflake operators (snowflake provider)",
        "dbt CLI Operators (DbtOperator / DbtRunOperator / DbtTestOperator / DbtSeedOperator / DbtSnapshotOperator / DbtBuildOperator)",
        "Cosmos DbtDag / DbtTaskGroup (astronomer-cosmos)",
        "Cloud & messaging operator families",
    }
    body = _text("references/operator-mapping.md")

    for section in re.split(r"^### ", body, flags=re.MULTILINE)[1:]:
        name = section.split("\n", 1)[0].strip()
        if name in family_sections:
            continue
        assert "**DABs task type:**" in section or "**DABs equivalent:**" in section, (
            f"operator section '{name}' declares no DABs mapping"
        )


def test_new_operator_sections_reach_the_readme_coverage_table():
    readme = _text("README.md")
    for operator in (
        "BranchDateTimeOperator",
        "BranchDayOfWeekOperator",
        "BashSensor",
        "PythonSensor",
        "DataprocSubmitJobOperator",
        "DataprocCreateBatchOperator",
        "DataprocJobSensor",
        "DataprocBatchSensor",
    ):
        assert f"`{operator}`" in readme, f"{operator} is absent from the README coverage table"


def test_dataproc_reference_routes_payloads_and_compute_constraints():
    body = _text("references/dataproc-migration.md")

    for operator in (
        "DataprocSubmitJobOperator",
        "ManagedSparkSubmitJobOperator",
        "DataprocCreateBatchOperator",
        "DataprocCreateClusterOperator",
        "DataprocJobSensor",
        "DataprocBatchSensor",
    ):
        assert operator in body

    expected_routes = {
        "pyspark_job": "spark_python_task",
        "pyspark_batch": "spark_python_task",
        "pyspark_notebook_batch": "notebook_task",
        "spark_job": "spark_jar_task",
        "spark_batch": "spark_jar_task",
        "spark_sql_job": "sql_task",
        "spark_r_job": "notebook_task",
    }
    for discriminator, task_type in expected_routes.items():
        row = next(
            line
            for line in body.splitlines()
            if line.startswith("|") and f"`{discriminator}`" in line
        )
        assert f"`{task_type}`" in row

    spark_r_row = next(
        line for line in body.splitlines() if line.startswith("|") and "`spark_r_job`" in line
    )
    assert "classic" in spark_r_row.lower()

    recognition = body.split("## Recognition", 1)[1].split("## Inventory contract", 1)[0].lower()
    assert "preferred" not in recognition
    assert "compatibility" not in recognition


def test_dataproc_example_collapses_lifecycle_to_one_native_task():
    bundle = yaml.safe_load(_text("examples/dataproc/dataproc_events_bundle/databricks.yml"))
    resource = yaml.safe_load(
        _text("examples/dataproc/dataproc_events_bundle/resources/dataproc_events_job.yml")
    )
    notes = _text("examples/dataproc/dataproc_events_bundle/MIGRATION_NOTES.md")
    job = resource["resources"]["jobs"]["dataproc_events_job"]

    assert all("default" not in bundle["variables"][key] for key in ("spark_version", "node_type_id"))
    assert [task["task_key"] for task in job["tasks"]] == ["transform_events"]
    assert "spark_python_task" in job["tasks"][0]
    assert "spark_submit_task" not in json.dumps(resource)
    assert all(prefix not in json.dumps(resource) for prefix in ("yarn:", "hdfs:", "mapred:"))

    parameters = {parameter["name"]: parameter["default"] for parameter in job["parameters"]}
    assert parameters["run_date"] == ""
    assert parameters["trigger_date"] == "{{job.trigger.time.iso_date}}"

    retry_section = notes.split("## Retry and lifecycle differences", 1)[1]
    assert "| Airflow retry boundary | Lakeflow retry boundary | Repeated-side-effect risk |" in retry_section

    logical_date_section = notes.split("## Logical date semantics", 1)[1]
    assert "Airflow 2" in logical_date_section
    assert "previous" in logical_date_section.lower()

    backfill_row = next(
        line for line in logical_date_section.splitlines() if line.startswith("| Native backfill |")
    )
    assert "`run_date` remains empty" in backfill_row
    assert "`trigger_date={{backfill.iso_date}}`" in backfill_row


def test_dataproc_spark_python_task_uses_a_plain_python_file():
    script = _text("examples/dataproc/dataproc_events_bundle/src/transform_events.py")
    first_line = script.splitlines()[0]

    assert "Databricks notebook source" not in first_line
    assert "argparse.ArgumentParser()" in script
    assert "timedelta(days=1)" in script


def test_dataproc_example_preserves_airflow_2_logical_date():
    module = ast.parse(
        _text("examples/dataproc/dataproc_events_bundle/src/transform_events.py")
    )
    resolver = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "resolve_run_date"
    )
    namespace = {"date": date, "timedelta": timedelta}
    exec(compile(ast.Module(body=[resolver], type_ignores=[]), "<resolver>", "exec"), namespace)

    assert namespace["resolve_run_date"]("", "2026-01-02") == "2026-01-01"
    assert namespace["resolve_run_date"]("2025-12-15", "2026-01-02") == "2025-12-15"


def test_dataproc_sensor_mapping_treats_batch_output_as_an_object():
    body = _text("references/operator-mapping.md")
    section = body.split("### DataprocJobSensor / DataprocBatchSensor", 1)[1].split("\n### ", 1)[0]

    assert "`batch_id`" in section
    assert re.search(r"batch (dictionary|dict|object)", section, re.IGNORECASE)


def test_backfill_date_is_not_described_as_airflow_interval_start():
    for relative_path in (
        "references/dab-schema-reference.md",
        "references/schedule-trigger-mapping.md",
    ):
        lines = [line for line in _text(relative_path).splitlines() if "backfill.iso_date" in line]
        assert lines
        assert any("scheduled trigger date" in line.lower() for line in lines)


def test_manifest_recipe_fails_when_dbt_does_not_write_manifest():
    for relative_path in (
        "assets/templates/dbt-Makefile.tmpl",
        "examples/dbt-cosmos/orders_analytics_bundle/Makefile",
    ):
        assert 'test -f "target/$(TARGET)/manifest.json"' in _text(relative_path)


def test_no_superseded_terms_remain():
    # `fail_stop` and `entry.isDir()` are named where the docs record what replaced them,
    # so they are checked in the sections that must not prescribe them, not repo-wide.
    superseded = ("SpecsHandler", "tests_<resource>", "databricks-dbt-factory==0.2.")
    scanned = list(ROOT.glob("*.md")) + list((ROOT / "references").glob("*.md"))

    for path in scanned:
        body = path.read_text(encoding="utf-8")
        for term in superseded:
            assert term not in body, f"{path.relative_to(ROOT)} still references '{term}'"
