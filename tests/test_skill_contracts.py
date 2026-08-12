"""Cross-surface and structural checks for the skill's behavior-shaping rules.

The three instruction surfaces are authored separately: `SKILL.md` (Cursor/Claude) and
`AGENTS.md` (Codex) point at `references/`, while `copilot-instructions.md` restates the
rules because Copilot cannot read sibling files. Their prose differs by design, so these
tests check two things that survive rewording:

* every hardening rule is carried by every surface, matched on `<!-- contract: id -->`
  anchors rather than on sentences;
* the parts with machine-readable structure (bundle YAML, Make recipes, code fences,
  operator sections) assert on that structure.

Run with: make test
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
INSTRUCTION_FILES = ("SKILL.md", "AGENTS.md", "copilot-instructions.md")

# Every rule an agent must apply regardless of harness. A rule added to one surface and
# forgotten on another fails here.
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
        "lifecycle-retry-disclosure",
        "manifest-recipe-guard",
        "required-var-not-a-fix",
        "dataset-or-time-schedule",
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


def test_every_surface_carries_every_hardening_contract():
    for relative_path in INSTRUCTION_FILES:
        present = _anchors(relative_path)
        missing = sorted(REQUIRED_CONTRACTS - present)
        assert not missing, f"{relative_path} is missing contracts: {missing}"


def test_no_surface_declares_an_unknown_contract():
    for relative_path in INSTRUCTION_FILES:
        unknown = sorted(_anchors(relative_path) - REQUIRED_CONTRACTS)
        assert not unknown, f"{relative_path} declares unknown contracts: {unknown}"


def test_copilot_defines_every_helper_it_tells_the_agent_to_call():
    # Copilot cannot read `references/`, so an identifier it names must be defined in it.
    copilot = _text("copilot-instructions.md")
    called = set(re.findall(r"call `([a-z_][a-z0-9_]*)\(", copilot))
    defined = {
        name
        for block in _FENCED_PYTHON.findall(copilot)
        for name in re.findall(r"^def ([a-z_][a-z0-9_]*)\(", block, re.MULTILINE)
    }
    assert called <= defined, f"named but undefined in copilot-instructions.md: {sorted(called - defined)}"


def test_recursive_listing_helper_is_bounded_and_portable():
    for relative_path in ("references/operator-mapping.md", "copilot-instructions.md"):
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
    for operator in ("BranchDateTimeOperator", "BranchDayOfWeekOperator", "BashSensor", "PythonSensor"):
        assert f"`{operator}`" in readme, f"{operator} is absent from the README coverage table"


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
