from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
INSTRUCTION_FILES = ("SKILL.md", "AGENTS.md", "copilot-instructions.md")


def _text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_skill_frontmatter_uses_supported_keys_and_current_product_name():
    skill = _text("SKILL.md")
    frontmatter = yaml.safe_load(skill.split("---", 2)[1])

    assert set(frontmatter) == {"name", "description"}
    assert "Declarative Automation Bundles" in frontmatter["description"]


def test_file_arrival_contract_prevents_missed_and_ignored_files():
    schedule_reference = _text("references/schedule-trigger-mapping.md")
    operator_reference = _text("references/operator-mapping.md")

    assert "queue:\n    enabled: true" in schedule_reference
    assert "existing files" in schedule_reference
    assert "recursive" in schedule_reference
    assert "Recursive prefix listings must remain recursive" in operator_reference
    assert "list_files_recursive" in operator_reference
    assert "Retained sensors that return file collections" in _text("SKILL.md")


def test_retained_recursive_sensor_generation_fails_closed():
    skill = _text("SKILL.md")
    operator_reference = _text("references/operator-mapping.md")

    assert "single shallow `dbutils.fs.ls(root)`" in skill
    assert "validation error" in skill
    assert "call `list_files_recursive(source_path)` inside every polling attempt" in (
        operator_reference.lower()
    )


def test_checked_in_file_arrival_jobs_enable_queueing():
    resource = yaml.safe_load(
        _text(
            "examples/customer-orders/customer_orders_bundle/resources/"
            "customer_orders_job.yml"
        )
    )
    job = resource["resources"]["jobs"]["customer_orders_lakeflow_job"]

    assert "file_arrival" in job["trigger"]
    assert job["queue"] == {"enabled": True}

    ingestion = _text(
        "examples/customer-orders/customer_orders_bundle/src/ingest_bronze.py"
    )
    notes = _text(
        "examples/customer-orders/customer_orders_bundle/MIGRATION_NOTES.md"
    )
    assert '.option("pathGlobFilter", "*.json")' in ingestion
    assert "initial manual run" in notes
    assert "recursively" in notes


def test_unresolved_identifiers_never_receive_executable_guessed_defaults():
    skill = _text("SKILL.md")
    operator_reference = _text("references/operator-mapping.md")

    assert "Never emit a guessed executable default" in skill
    assert "required bundle variable with no default" in operator_reference


def test_mixed_time_and_asset_schedules_default_to_manual():
    schedule_reference = _text("references/schedule-trigger-mapping.md")

    assert "Do not emit either arm automatically" in schedule_reference
    assert "manual job" in schedule_reference


def test_branch_and_constant_sensor_operators_have_authoritative_mappings():
    operator_reference = _text("references/operator-mapping.md")

    for operator in (
        "BranchDateTimeOperator",
        "BranchDayOfWeekOperator",
        "BashSensor",
        "PythonSensor",
    ):
        assert f"### {operator}" in operator_reference


def test_migration_notes_require_lifecycle_and_retry_granularity_disclosure():
    skill = _text("SKILL.md")

    assert "collapsed retry envelope" in skill
    assert "teardown failure affects the Lakeflow job result" in skill


def test_manifest_recipe_fails_when_dbt_does_not_write_manifest():
    makefile = _text("assets/templates/dbt-Makefile.tmpl")

    assert 'test -f "target/$(TARGET)/manifest.json"' in makefile


def test_all_agent_instruction_surfaces_share_hardening_contracts():
    required_contracts = (
        "Databricks Declarative Automation Bundles",
        "Never emit a guessed executable default",
        "BranchDateTimeOperator",
        "BranchDayOfWeekOperator",
        "BashSensor",
        "PythonSensor",
        "queue.enabled: true",
        "single shallow `dbutils.fs.ls(root)`",
        "manual job with neither arm",
        "collapsed retry envelope",
        "teardown failure affects the Lakeflow job result",
        "legacy two-argument API or current three-argument API",
        "must fail unless `target/<target>/manifest.json` exists",
    )

    for relative_path in INSTRUCTION_FILES:
        instructions = _text(relative_path)
        for contract in required_contracts:
            assert contract in instructions, f"{relative_path} is missing: {contract}"
