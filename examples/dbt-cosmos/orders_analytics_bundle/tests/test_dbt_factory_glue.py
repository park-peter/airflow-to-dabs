"""Regression tests for the PyDABs glue in resources/orders_analytics_dbt_job.py.

The glue's guard/rewrite functions are pure (dict/list in, dict/list out), so they
are tested here with synthetic manifests — no dbt install, warehouse, or committed
manifest required, which keeps the suite runnable on a fresh clone. One integration
test drives the whole pipeline against the real manifest and skips when it is absent
(target/dev/manifest.json is git-ignored).

Run with: uv run pytest tests
"""

import importlib.util
import json
import re
from pathlib import Path

import pytest

_BUNDLE_ROOT = Path(__file__).resolve().parent.parent
_GLUE_PATH = _BUNDLE_ROOT / "resources" / "orders_analytics_dbt_job.py"


def _load_glue():
    spec = importlib.util.spec_from_file_location("glue_under_test", _GLUE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


glue = _load_glue()


def _model(name, fqn, version=None):
    node = {"resource_type": "model", "name": name, "fqn": fqn}
    if version is not None:
        node["version"] = version
    return node


def _test(name, fqn):
    return {"resource_type": "test", "name": name, "fqn": fqn}


def _seed(name, fqn):
    return {"resource_type": "seed", "name": name, "fqn": fqn}


def _manifest(nodes, unit_tests=None):
    return {"nodes": nodes, "unit_tests": unit_tests or {}}


# --------------------------------------------------------------------------------------
# _carries_vars_flag — every --vars spelling dbt accepts (see dbt/cli/params.py)
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "argv,expected",
    [
        (["run", "--select", "fqn:p.m", "--vars", "{a: 1}"], True),   # space form
        (["run", "--vars={a:1}"], True),                              # equals + json (one token)
        (["run", "--vars={a:", "1}"], True),                          # equals + yaml (shlex splits)
        (["run", "--select", "fqn:p.m", "--target", "dev"], False),   # no vars
        (["run", "--warn-error-options", "{include: all}"], False),   # different quoted option
        (["build"], False),
    ],
)
def test_carries_vars_flag(argv, expected):
    assert glue._carries_vars_flag(argv) is expected


# --------------------------------------------------------------------------------------
# _assert_unique_task_keys — belt-and-suspenders: the factory guarantees unique keys, so this
# only trips on a factory regression that emits two tasks with the same key.
# --------------------------------------------------------------------------------------

def test_unique_task_keys_pass():
    glue._assert_unique_task_keys([{"task_key": "a"}, {"task_key": "b"}])


def test_unique_task_keys_reject_collision():
    with pytest.raises(RuntimeError, match="collide"):
        glue._assert_unique_task_keys(
            [{"task_key": "orders_model"}, {"task_key": "orders_model"}]
        )


# --------------------------------------------------------------------------------------
# _prune_dangling_deps — deps on tasks that were not emitted are removed
# --------------------------------------------------------------------------------------

def test_prune_removes_dangling_and_keeps_valid():
    tasks = [
        {"task_key": "a"},
        {"task_key": "b", "depends_on": [{"task_key": "a"}, {"task_key": "gone"}]},
        {"task_key": "c", "depends_on": [{"task_key": "gone"}]},
    ]
    pruned = {t["task_key"]: t for t in glue._prune_dangling_deps(tasks)}
    assert pruned["b"]["depends_on"] == [{"task_key": "a"}]   # dangling ref dropped
    assert "depends_on" not in pruned["c"]                     # all deps dangling -> removed


# --------------------------------------------------------------------------------------
# Task-dict helpers, shared by the integration tests below.
# --------------------------------------------------------------------------------------

def _notebook_task(task_key, commands):
    return {
        "task_key": task_key,
        "notebook_task": {"base_parameters": {"dbt_commands": json.dumps(commands)}},
    }


def _commands_of(task):
    return json.loads(task["notebook_task"]["base_parameters"]["dbt_commands"])


# --------------------------------------------------------------------------------------
# _assert_within_task_limit — fail closed above the 1,000-task per-job cap
# --------------------------------------------------------------------------------------

def test_task_limit_passes_at_or_below_limit(monkeypatch):
    monkeypatch.setattr(glue, "TASK_LIMIT", 3)
    glue._assert_within_task_limit([{"task_key": f"t{i}"} for i in range(3)])  # no raise


def test_task_limit_raises_above_limit_suggests_bundling(monkeypatch):
    monkeypatch.setattr(glue, "TASK_LIMIT", 2)
    monkeypatch.setattr(glue, "BUNDLE_TESTS", False)
    with pytest.raises(RuntimeError, match="BUNDLE_TESTS = True"):
        glue._assert_within_task_limit([{"task_key": f"t{i}"} for i in range(3)])


def test_task_limit_raises_above_limit_when_already_bundled(monkeypatch):
    monkeypatch.setattr(glue, "TASK_LIMIT", 2)
    monkeypatch.setattr(glue, "BUNDLE_TESTS", True)
    with pytest.raises(RuntimeError, match="split the project by dbt tag"):
        glue._assert_within_task_limit([{"task_key": f"t{i}"} for i in range(3)])


# --------------------------------------------------------------------------------------
# Integration: drive the whole pipeline against the real manifest (skips if absent).
# --------------------------------------------------------------------------------------

@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_build_tasks_end_to_end():
    tasks = glue._build_tasks("dev")
    # This example project: 1 seed + 3 models + 2 tests.
    assert len(tasks) == 6
    keys = {t["task_key"] for t in tasks}
    for task in tasks:
        for dep in task.get("depends_on", []):
            assert dep["task_key"] in keys       # no dangling deps
        for cmd in _commands_of(task):
            if "--select" in cmd:
                # The factory intersects every independent fact about the node: its fqn plus
                # package, file, and resource_type terms, comma-joined (dbt's AND).
                selector = re.search(r"--select\s+(\S+)", cmd).group(1)
                terms = selector.split(",")
                assert any(t.startswith("fqn:orders_analytics.") for t in terms)
                assert "package:orders_analytics" in terms
                assert any(t.startswith("file:") for t in terms)
                assert any(t.startswith("resource_type:") for t in terms)
                assert "--vars" not in cmd                        # no command-level vars


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_build_tasks_rejects_extra_vars(monkeypatch):
    monkeypatch.setattr(glue, "EXTRA_DBT_COMMAND_OPTIONS", "--vars '{a: 1}'")
    with pytest.raises(RuntimeError, match="--vars"):
        glue._build_tasks("dev")


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_bundled_mode_unions_each_test_selector_with_indirect_selection_empty():
    # Bundling repeats --select once per test node (dbt unions repeated flags)
    # and pins --indirect-selection empty so only the named tests run. Match on the
    # command rather than the task key.
    tasks = glue._build_tasks("dev", bundle_tests=True)
    bundled = [c for t in tasks for c in _commands_of(t) if "dbt test" in c]

    assert bundled, "expected at least one bundled test task in bundled mode"
    for cmd in bundled:
        assert "--indirect-selection empty" in cmd
        assert cmd.count("--select") >= 1


def test_bundled_test_selectors_name_only_test_nodes():
    # Each bundled selector addresses a test node directly, so every --select carries a
    # resource_type:test term. A selector naming the resource instead would rely on
    # indirect selection, which the emitted command disables.
    tasks = glue._build_tasks("dev", bundle_tests=True)
    checked = 0
    for task in tasks:
        for cmd in _commands_of(task):
            if "dbt test" not in cmd:
                continue
            selectors = re.findall(r"--select\s+(\S+)", cmd)
            assert selectors, f"bundled test command has no --select: {cmd!r}"
            for selector in selectors:
                assert "resource_type:test" in selector.split(","), (
                    f"bundled selector {selector!r} does not name a test node"
                )
                checked += 1
    assert checked, "no bundled test selectors were checked"


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_count_tasks_bundled_not_more_than_unbundled():
    unbundled = glue.count_tasks("dev", bundle_tests=False)
    bundled = glue.count_tasks("dev", bundle_tests=True)
    assert bundled <= unbundled          # bundling never increases task count
    assert unbundled >= 1


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_build_tasks_enforces_limit(monkeypatch):
    monkeypatch.setattr(glue, "TASK_LIMIT", 1)
    with pytest.raises(RuntimeError, match="over the 1-task limit"):
        glue._build_tasks("dev")
