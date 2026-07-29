"""PyDABs hook that builds the dbt job for the migrated orders_analytics DAG.

At `databricks bundle deploy` (and `validate`) the Databricks CLI calls
`load_resources`, which reads the dbt manifest and expands it into a Databricks
job with one task per dbt object using the databricks-dbt-factory package from
PyPI. No per-model YAML is checked in — the task graph tracks the dbt DAG
automatically.

The YAML job triggers this job with a run_job_task via
${resources.jobs.orders_analytics_dbt_job.id}, optionally passing runtime dbt vars
through the `dbt_vars` job parameter.

One module per dbt-bearing DAG; the module name is the dag_id sanitized to a
valid Python identifier.
Verified against databricks-dbt-factory==0.3.1 with databricks-bundles 1.0.0-1.7.0
on Databricks CLI v1.7.0. databricks-dbt-factory selects every node by its full
dot-joined FQN and emits one task per dbt test (including unit tests); this glue
validates and prunes that output rather than rewriting selectors.
"""

import os
import re
import shlex
from collections import Counter
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import yaml

# dbt's own selector matcher, used to check generated selectors against the exact
# semantics dbt will apply at run time. Provided by dbt-core in the bundle venv.
from dbt.graph.selector_methods import is_selected_node
from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job

from databricks_dbt_factory.DbtFactory import DbtFactory
from databricks_dbt_factory.DbtTask import DbtTaskOptions, TaskType
from databricks_dbt_factory.Utils import read_dbt_manifest
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

# Which dbt node types get factories, derived from the dbt commands the original
# Airflow tasks ran (union across tasks):
#   dbt run      -> "model"
#   dbt seed     -> "seed"
#   dbt snapshot -> "snapshot"
#   dbt test     -> "test"
#   dbt build    -> all four
# `deps`/`docs`-only workloads are not factory-eligible (use the single-dbt_task
# fallback). Dependencies pointing at node types without a factory are pruned
# after generation (see _prune_dangling_deps).
FACTORY_TYPES = ["model", "snapshot", "seed", "test"]  # cosmos group ran the whole project

# Serverless environment shared by all generated tasks. The spec file is written
# at deploy time (see _write_serverless_environment_file) and synced with the
# bundle so Databricks pre-builds the environment once instead of installing dbt
# on every task.
ENVIRONMENT_KEY = "Default"
SERVERLESS_ENV_FILE = "dbt_serverless_env.yaml"

# Runner notebook executed by every generated task. It is emitted by the skill
# (based on the pinned databricks-dbt-factory runner, extended with dbt_vars and
# per-target parse-cache support) and checked into the bundle at src/.
RUNNER_NOTEBOOK_PATH = "src/run_dbt_command.py"
PROJECT_DIRECTORY = os.path.relpath(".", os.path.dirname(RUNNER_NOTEBOOK_PATH))
PROFILES_DIRECTORY = "dbt_profiles"

# Static, global options appended to every generated dbt command. Escape hatch
# only. Do NOT put --vars here: vars belong in dbt_vars.json / the dbt_vars job
# parameter (the runner rejects a command-level --vars). And --full-refresh must
# never be applied globally (it is invalid for `dbt test`).
EXTRA_DBT_COMMAND_OPTIONS = ""

# When True, a resource's single-model tests collapse into ONE `<resource>_test`
# task (dbt test --select <resource-fqn> --indirect-selection cautious) instead of one
# task per test node; cross-model and zero-dep tests still get their own tasks.
# Bundling is the main lever for the 1,000-task-per-job Lakeflow limit, but it
# coarsens retry granularity (a model's tests rerun together, not individually).
# The skill flips this to True only when the unbundled task count is over budget
# (see TASK_LIMIT / WARN_THRESHOLD) and the user accepts the tradeoff.
BUNDLE_TESTS = False

# A single Databricks job holds at most TASK_LIMIT tasks. WARN_THRESHOLD leaves
# headroom for the manifest to grow after migration before the hard cap is hit.
TASK_LIMIT = 1000
WARN_THRESHOLD = 900

_BUNDLE_ROOT = Path(__file__).resolve().parent.parent


def _manifest_path(target: str) -> str:
    """Per-target manifest written by `make manifest TARGET=<target>`. Target-specific
    paths prevent artifacts parsed against one profile target (e.g. dev catalog/schema)
    from leaking into another target's deployment."""
    return os.environ.get("DBT_MANIFEST_PATH", f"target/{target}/manifest.json")


def _pinned_release(package: str) -> str:
    """Pin the serverless runtime to the exact version installed in this bundle's
    venv, so the version that runs in Databricks matches the one used to generate
    the manifest and develop locally."""
    try:
        installed = version(package)
    except PackageNotFoundError as exc:
        raise RuntimeError(
            f"{package} is not installed in the bundle venv — run `make setup` first."
        ) from exc

    from packaging.version import InvalidVersion, Version

    try:
        parsed = Version(installed)
    except InvalidVersion:
        parsed = None
    if parsed is None or parsed.local or parsed.is_devrelease:
        raise RuntimeError(
            f"The installed {package} version ({installed}) is not a plain PyPI "
            "release, so the serverless environment cannot install it. Set a released "
            "version in pyproject.toml and re-run `make setup`."
        )
    return f"{package}=={installed}"


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
    # dbt-core is pinned alongside dbt-databricks: dbt-databricks alone allows a
    # dbt-core range, and the selector-exactness check imports the LOCAL dbt-core —
    # runtime must resolve the identical version for the guarantee to hold.
    spec = {
        "environment_version": "5",
        "dependencies": [_pinned_release("dbt-databricks"), _pinned_release("dbt-core")],
    }
    content = yaml.safe_dump(spec, sort_keys=False)
    _write_idempotent(_BUNDLE_ROOT / SERVERLESS_ENV_FILE, content)


def _flat_fqn(fqn: list) -> list[str]:
    """Normalize an FQN the way dbt's selector does: split each component on dots."""
    return [part for comp in fqn for part in str(comp).split(".")]


def _node_selector_matches(selector: str, fqn: list, is_versioned: bool) -> bool:
    """Exactly dbt's QualifiedNameSelectorMethod.node_is_match: full-FQN match via
    dbt's own is_selected_node (leaf shortcuts, versioned models, wildcard slurp,
    prefix semantics), then the package-stripped retry."""
    if is_selected_node(fqn, selector, is_versioned):
        return True
    unscoped = fqn[1:]
    return bool(unscoped) and is_selected_node(unscoped, selector, is_versioned)


def _assert_exact_selectors(manifest: dict, *, bundle_tests: bool = BUNDLE_TESTS) -> None:
    """Defense-in-depth: databricks-dbt-factory 0.3.1 selects every node by its full
    dot-joined FQN, which is unambiguous across packages and subdirectories. This check
    independently confirms each node's FQN still resolves to exactly its own node among
    same-type nodes, catching the residual cases the factory's FQN generation cannot:
    two nodes with an identical FQN, and FQN characters that break dbt's selector grammar
    or the runner's shlex parsing. Matching uses dbt's own is_selected_node (imported
    above) so it cannot drift from runtime selector semantics; requiring the matched set
    to equal {self} also rejects a selector that resolves to a single WRONG node (e.g. a
    test named `[ab]` glob-matching only its sibling `a`).

    Under BUNDLE_TESTS, individual test nodes are not selected on their own — a resource's
    single-model tests run via its `<resource>_test` task (targeting the resource, which
    is still checked here). Scanning test nodes then would raise false positives on
    equal-FQN or shadowed tests that are never selected individually, so test nodes are
    excluded from the scan in bundled mode."""
    scanned_types = set(FACTORY_TYPES)
    if bundle_tests:
        scanned_types.discard("test")
    nodes_by_type: dict[str, list[tuple[str, list, bool]]] = {}
    for key, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") in scanned_types:
            nodes_by_type.setdefault(node["resource_type"], []).append(
                (key, node["fqn"], node.get("version") is not None)
            )
    problems = []
    for rtype, entries in nodes_by_type.items():
        for key, fqn, _ in entries:
            selector = ".".join(_flat_fqn(fqn))
            # The selector is the node's full FQN (package + every directory
            # component + name). dbt's CLI selector grammar (space, ",", "+",
            # "@", ":", "*?[]") and the runner's shlex.split reinterpret or
            # corrupt anything outside dbt identifier characters, so the allowed
            # set is those plus "." (the FQN separator) and "-" (legal in path
            # components).
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", selector):
                problems.append(
                    f"{rtype} '{selector}' contains characters unsafe for dbt "
                    "selector or shell parsing"
                )
                continue
            matched = {
                other_key
                for other_key, other_fqn, other_versioned in entries
                if _node_selector_matches(selector, other_fqn, other_versioned)
            }
            if matched != {key}:
                extras = sorted(matched - {key})
                detail = (
                    f"misses its own node and matches {extras}"
                    if key not in matched
                    else f"also matches {extras}"
                )
                problems.append(f"{rtype} '{selector}' {detail}")
    if problems:
        raise RuntimeError(
            "Generated selectors are not exact: "
            + "; ".join(sorted(problems))
            + ". Rename the affected resources, directories, or tests so each "
            "selector resolves to exactly its own node, or fall back to a single "
            "dbt_task."
        )


def _assert_unique_task_keys(tasks: list[dict]) -> None:
    """Defense-in-depth: databricks-dbt-factory 0.3.1 derives readable task keys
    (`<resource>_<type>`, bundled `<resource>_test`) through build_task_key_maps, which
    already disambiguates collisions and bounds keys to 100 chars — so the factory's own
    output is unique. This independently confirms that invariant on the emitted tasks:
    duplicate keys would make PyDABs serialization keep only one, silently dropping the
    rest, so fail closed if a factory regression ever lets one through."""
    counts = Counter(task["task_key"] for task in tasks)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if duplicates:
        raise RuntimeError(
            "Generated task keys collide: "
            + ", ".join(duplicates)
            + ". This should not happen with databricks-dbt-factory 0.3.1; rename the "
            "affected dbt resources or fall back to a single dbt_task."
        )


def _prune_dangling_deps(tasks: list[dict]) -> list[dict]:
    """When FACTORY_TYPES omits a node type, databricks-dbt-factory still emits depends_on entries
    pointing at the omitted tasks. Retain only references to tasks that exist."""
    emitted = {task["task_key"] for task in tasks}
    for task in tasks:
        deps = task.get("depends_on")
        if deps:
            kept = [dep for dep in deps if dep.get("task_key") in emitted]
            if kept:
                task["depends_on"] = kept
            else:
                task.pop("depends_on", None)
    return tasks


def _assert_within_task_limit(tasks: list[dict]) -> None:
    """A single Databricks job holds at most TASK_LIMIT tasks. Fail closed at deploy
    time with an actionable message rather than let the Jobs API reject the job with a
    cryptic error — the manifest can grow past the limit after migration, and the hook
    cannot prompt. Enabling BUNDLE_TESTS is the first lever; see MIGRATION_NOTES."""
    if len(tasks) > TASK_LIMIT:
        hint = (
            "split the project by dbt tag into multiple factory jobs, or fall back to a "
            "single dbt_task"
            if BUNDLE_TESTS
            else "set BUNDLE_TESTS = True to collapse each resource's tests into one task"
        )
        raise RuntimeError(
            f"The generated dbt job has {len(tasks)} tasks, over the {TASK_LIMIT}-task "
            f"limit for a single Databricks job. To fit within the limit, {hint}."
        )


def count_tasks(target: str, *, bundle_tests: bool) -> int:
    """Number of tasks the factory would emit for `target` in the given test mode,
    computed from the manifest without deploying. Used by `make task-count` so the
    unbundled vs bundled counts can be compared against TASK_LIMIT before choosing
    BUNDLE_TESTS."""
    return len(_build_tasks(target, bundle_tests=bundle_tests, enforce_limit=False))


def _carries_vars_flag(args: list[str]) -> bool:
    """True if a tokenized dbt command carries its own --vars. dbt (see
    dbt/cli/params.py) exposes one spelling with no short alias and no env var,
    accepting both `--vars <yaml>` and `--vars=<yaml>`. The runner applies the
    same predicate to shlex-split commands."""
    return any(a == "--vars" or a.startswith("--vars=") for a in args)


def _build_tasks(
    target: str, *, bundle_tests: bool = BUNDLE_TESTS, enforce_limit: bool = True
) -> list[dict]:
    """Reads the dbt manifest and returns one Databricks task dict per dbt node.
    `bundle_tests` defaults to the module constant; count_tasks overrides it to size
    the other mode. `enforce_limit` is False only for that sizing (the guard applies
    to the mode actually deployed)."""
    resolver = DbtDependencyResolver()
    task_options = DbtTaskOptions(
        environment_key=ENVIRONMENT_KEY,
        task_type=TaskType.NOTEBOOK,
        notebook_path=RUNNER_NOTEBOOK_PATH,
        project_directory=PROJECT_DIRECTORY,
        profiles_directory=PROFILES_DIRECTORY,
    )
    # Vars must go through the canonical dbt_vars channel, not
    # EXTRA_DBT_COMMAND_OPTIONS. Tokenize with shlex.split so the check runs on the
    # same argv the runner sees, matching dbt's parsing rather than raw text.
    if _carries_vars_flag(shlex.split(EXTRA_DBT_COMMAND_OPTIONS)):
        raise RuntimeError(
            "EXTRA_DBT_COMMAND_OPTIONS must not contain --vars; set vars via "
            "dbt_vars.json (static) or the dbt_vars job parameter (runtime)."
        )
    dbt_options = f"--target {target} {EXTRA_DBT_COMMAND_OPTIONS}".strip()

    factory_classes = {
        "model": ModelTaskFactory,
        "snapshot": SnapshotTaskFactory,
        "seed": SeedTaskFactory,
        "test": TestTaskFactory,
    }
    task_factories = {
        node_type: factory_classes[node_type](resolver, task_options, dbt_options)
        for node_type in FACTORY_TYPES
    }

    factory = DbtFactory(task_factories, bundle_tests=bundle_tests)
    manifest = read_dbt_manifest(str(_BUNDLE_ROOT / _manifest_path(target)))
    _assert_exact_selectors(manifest, bundle_tests=bundle_tests)
    tasks = factory.create_tasks(manifest)
    _assert_unique_task_keys(tasks)
    tasks = _prune_dangling_deps(tasks)
    if enforce_limit:
        _assert_within_task_limit(tasks)
    return tasks


def build_job(target: str) -> Job:
    return Job.from_dict(
        {
            "name": JOB_NAME,
            "queue": {"enabled": True},
            "tags": {"source": "airflow-migration", "original_dag": "orders_analytics"},
            # Job parameters flow into every notebook task's widgets. The runner
            # appends ["--vars", dbt_vars] to each dbt command when dbt_vars is a
            # non-empty JSON object, and uses dbt_target to locate the per-target
            # parse cache. The parent job passes dbt_vars via run_job_task
            # job_parameters. Vars that change the dbt graph (enabled nodes,
            # schemas, aliases) are NOT safe here — the task graph was compiled
            # at deploy time.
            "parameters": [
                {"name": "dbt_vars", "default": "{}"},
                {"name": "dbt_target", "default": target},
            ],
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
    resources = Resources()
    resources.add_job(JOB_KEY, build_job(bundle.target))
    return resources
