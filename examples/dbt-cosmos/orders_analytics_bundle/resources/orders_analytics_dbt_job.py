"""PyDABs hook that builds the dbt job for the migrated orders_analytics DAG.

At `databricks bundle deploy` (and `validate`) the Databricks CLI calls
`load_resources`, which reads the dbt manifest and expands it into a Databricks
job with one task per dbt object (model / seed / snapshot / test) using the
databricks-dbt-factory package from PyPI. No per-model YAML is checked in — the
task graph tracks the dbt DAG automatically.

The YAML job in orders_analytics_job.yml triggers this job with a run_job_task
via ${resources.jobs.orders_analytics_dbt_job.id}.
"""

import os
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from pathlib import Path

import yaml
from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job

from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.DbtTask import DbtTaskOptions, TaskType
from databricks_dbt_factory.SpecsHandler import SpecsHandler
from databricks_dbt_factory.TaskFactory import (
    DbtDependencyResolver,
    ModelTaskFactory,
    SeedTaskFactory,
    SnapshotTaskFactory,
    TestTaskFactory,
)

# Resource key of the generated job. The YAML job references it as
# ${resources.jobs.orders_analytics_dbt_job.id}.
JOB_KEY = "orders_analytics_dbt_job"
JOB_NAME = "orders-analytics-dbt"

# dbt manifest read at deploy/validate time. Regenerate with `make manifest`.
MANIFEST_PATH = os.environ.get("DBT_MANIFEST_PATH", "target/manifest.json")

# Serverless environment shared by all generated tasks. The spec file is written
# at deploy time (see _write_serverless_environment_file) and synced with the
# bundle so Databricks pre-builds the environment once instead of installing dbt
# on every task.
ENVIRONMENT_KEY = "Default"
SERVERLESS_ENV_FILE = "dbt_serverless_env.yaml"

# Runner notebook that each generated task executes. It is extracted from the
# installed databricks-dbt-factory package at deploy time so it always matches
# the pinned version. The notebook resolves a relative project_directory against
# its own workspace location, and profiles_directory against the project root.
RUNNER_NOTEBOOK_PATH = "src/run_dbt_command.py"
PROJECT_DIRECTORY = os.path.relpath(".", os.path.dirname(RUNNER_NOTEBOOK_PATH))
PROFILES_DIRECTORY = "dbt_profiles"

# False: one `dbt test` task per test node. True: bundle all single-resource
# tests per resource into one `tests_<resource>` task (fewer task startups) and
# gate downstream resources on it.
BUNDLE_TESTS = False

# Extra options appended to every generated dbt command (e.g. "--vars '{...}'").
EXTRA_DBT_COMMAND_OPTIONS = ""

_BUNDLE_ROOT = Path(__file__).resolve().parent.parent


def _dbt_databricks_dependency() -> str:
    """Pin the serverless runtime to the exact dbt-databricks installed in this
    bundle's venv — the same version used to generate the manifest and to develop
    locally, so the version that runs in Databricks matches the one you tested with.
    """
    try:
        installed = version("dbt-databricks")
    except PackageNotFoundError as exc:
        raise RuntimeError(
            "dbt-databricks is not installed in the bundle venv — run `make setup` first."
        ) from exc

    from packaging.version import InvalidVersion, Version

    try:
        parsed = Version(installed)
    except InvalidVersion:
        parsed = None
    if parsed is None or parsed.local or parsed.is_devrelease:
        raise RuntimeError(
            f"The installed dbt-databricks version ({installed}) is not a plain PyPI "
            "release, so the serverless environment cannot install it. Set a released "
            "version in pyproject.toml and re-run `make setup`."
        )
    return f"dbt-databricks=={installed}"


def _write_idempotent(path: Path, content: str) -> None:
    """Write only when the content differs, so `bundle validate` stays free of
    side effects once the file exists."""
    try:
        if path.read_text(encoding="utf-8") == content:
            return
    except OSError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_serverless_environment_file() -> None:
    spec = {
        "environment_version": "5",
        "dependencies": [_dbt_databricks_dependency()],
    }
    content = yaml.safe_dump(spec, sort_keys=False)
    _write_idempotent(_BUNDLE_ROOT / SERVERLESS_ENV_FILE, content)


def _write_runner_notebook() -> None:
    source = (
        files("databricks_dbt_factory")
        .joinpath("notebook/run_dbt_command.py")
        .read_text(encoding="utf-8")
    )
    _write_idempotent(_BUNDLE_ROOT / RUNNER_NOTEBOOK_PATH, source)


def _build_tasks(target: str) -> list[dict]:
    """Reads the dbt manifest and returns one Databricks task dict per dbt node."""
    resolver = DbtDependencyResolver()
    task_options = DbtTaskOptions(
        environment_key=ENVIRONMENT_KEY,
        task_type=TaskType.NOTEBOOK,
        notebook_path=RUNNER_NOTEBOOK_PATH,
        project_directory=PROJECT_DIRECTORY,
        profiles_directory=PROFILES_DIRECTORY,
    )
    dbt_options = f"--target {target} {EXTRA_DBT_COMMAND_OPTIONS}".strip()

    task_factories = {
        "model": ModelTaskFactory(resolver, task_options, dbt_options),
        "snapshot": SnapshotTaskFactory(resolver, task_options, dbt_options),
        "seed": SeedTaskFactory(resolver, task_options, dbt_options),
        "test": TestTaskFactory(resolver, task_options, dbt_options),
    }

    factory = DbtFactory(SpecsHandler(), task_factories, bundle_tests=BUNDLE_TESTS)
    manifest = SpecsHandler.read_dbt_manifest(str(_BUNDLE_ROOT / MANIFEST_PATH))
    return factory.create_tasks(manifest)


def build_job(target: str) -> Job:
    return Job.from_dict(
        {
            "name": JOB_NAME,
            "queue": {"enabled": True},
            "tags": {"source": "airflow-migration", "original_dag": "orders_analytics"},
            "tasks": _build_tasks(target),
            "environments": [
                {
                    "environment_key": ENVIRONMENT_KEY,
                    "spec": {
                        "base_environment": "${workspace.file_path}/" + SERVERLESS_ENV_FILE,
                    },
                }
            ],
        }
    )


def load_resources(bundle: Bundle) -> Resources:
    """Called by the Databricks CLI during `bundle deploy` and `bundle validate`."""
    _write_serverless_environment_file()
    _write_runner_notebook()
    resources = Resources()
    resources.add_job(JOB_KEY, build_job(bundle.target))
    return resources
