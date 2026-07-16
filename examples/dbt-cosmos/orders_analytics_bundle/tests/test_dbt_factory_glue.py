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
# _fail_closed_checks — unit tests dropped by databricks-dbt-factory 0.2.1
# --------------------------------------------------------------------------------------

def test_fail_closed_passes_without_unit_tests():
    m = _manifest({"model.p.a": _model("a", ["p", "a"])})
    glue._fail_closed_checks(m)  # no raise


def test_fail_closed_rejects_unit_tests_when_test_factory_enabled():
    m = _manifest(
        {"model.p.a": _model("a", ["p", "a"])},
        unit_tests={"unit_test.p.a.ut": {}},
    )
    with pytest.raises(RuntimeError, match="unit test"):
        glue._fail_closed_checks(m)


def test_fail_closed_allows_unit_tests_when_test_factory_disabled(monkeypatch):
    monkeypatch.setattr(glue, "FACTORY_TYPES", ["model"])
    m = _manifest(
        {"model.p.a": _model("a", ["p", "a"])},
        unit_tests={"unit_test.p.a.ut": {}},
    )
    glue._fail_closed_checks(m)  # no raise: test factory is off


# --------------------------------------------------------------------------------------
# _assert_exact_selectors — each generated fqn selector must resolve to exactly its node
# under dbt's own matching semantics (imported is_selected_node).
# --------------------------------------------------------------------------------------

def test_exact_selectors_pass_on_distinct_nodes():
    m = _manifest(
        {
            "model.p.staging.stg": _model("stg", ["p", "staging", "stg"]),
            "model.p.marts.fct": _model("fct", ["p", "marts", "fct"]),
        }
    )
    glue._assert_exact_selectors(m)  # no raise


def test_exact_selectors_reject_equal_fqns():
    # Two tests with identical FQN (e.g. same custom test name) — distinct keys, same selector.
    m = _manifest(
        {
            "test.p.dup": _test("dup", ["p", "staging", "dup"]),
            "test.other.dup": _test("dup", ["p", "staging", "dup"]),
        }
    )
    with pytest.raises(RuntimeError, match="not exact"):
        glue._assert_exact_selectors(m)


def test_exact_selectors_reject_prefix_shadow():
    # A model named 'marts' shadows models under the marts/ directory (prefix match).
    m = _manifest(
        {
            "model.p.marts": _model("marts", ["p", "marts"]),
            "model.p.marts.fct": _model("fct", ["p", "marts", "fct"]),
        }
    )
    with pytest.raises(RuntimeError, match="not exact"):
        glue._assert_exact_selectors(m)


def test_exact_selectors_reject_package_stripped_overlap():
    # ['root','a'] vs ['dep','root','a'] — dbt retries selection with the package stripped.
    m = _manifest(
        {
            "model.root.a": _model("a", ["root", "a"]),
            "model.dep.a": _model("a", ["dep", "root", "a"]),
        }
    )
    with pytest.raises(RuntimeError, match="not exact"):
        glue._assert_exact_selectors(m)


def test_exact_selectors_reject_disallowed_characters():
    m = _manifest({"model.p.bad": _model("foo,bar", ["p", "foo,bar"])})
    with pytest.raises(RuntimeError, match="unsafe"):
        glue._assert_exact_selectors(m)


def test_exact_selectors_allow_hyphenated_path():
    m = _manifest({"model.p.stg": _model("stg", ["p", "my-dir", "stg"])})
    glue._assert_exact_selectors(m)  # hyphen is allowed in path components


def test_exact_selectors_allow_versioned_pair():
    m = _manifest(
        {
            "model.p.m.v1": _model("m", ["p", "m", "v1"], version=1),
            "model.p.m.v2": _model("m", ["p", "m", "v2"], version=2),
        }
    )
    glue._assert_exact_selectors(m)  # versioned models are distinguishable


# --------------------------------------------------------------------------------------
# _assert_unique_task_keys — distinct node names can sanitize to one task key
# --------------------------------------------------------------------------------------

def test_unique_task_keys_pass():
    glue._assert_unique_task_keys([{"task_key": "a"}, {"task_key": "b"}])


def test_unique_task_keys_reject_collision():
    with pytest.raises(RuntimeError, match="collide"):
        glue._assert_unique_task_keys(
            [{"task_key": "model_foo_bar_baz"}, {"task_key": "model_foo_bar_baz"}]
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
# _qualify_selectors — bare names rewritten to fqn:, test commands pinned direct-only
# --------------------------------------------------------------------------------------

def _notebook_task(task_key, commands):
    return {
        "task_key": task_key,
        "notebook_task": {"base_parameters": {"dbt_commands": json.dumps(commands)}},
    }


def _commands_of(task):
    return json.loads(task["notebook_task"]["base_parameters"]["dbt_commands"])


def test_qualify_rewrites_model_selector_to_fqn():
    manifest = _manifest({"model.p.staging.stg": _model("stg", ["p", "staging", "stg"])})
    key = glue.generate_task_key("model.p.staging.stg")
    tasks = [_notebook_task(key, ["dbt run --select stg --target dev"])]
    out = _commands_of(glue._qualify_selectors(tasks, manifest)[0])
    assert out == ["dbt run --select fqn:p.staging.stg --target dev"]


def test_qualify_pins_test_to_indirect_selection_empty():
    manifest = _manifest({"test.p.staging.t": _test("t", ["p", "staging", "t"])})
    key = glue.generate_task_key("test.p.staging.t")
    tasks = [_notebook_task(key, ["dbt test --select t --target dev"])]
    (cmd,) = _commands_of(glue._qualify_selectors(tasks, manifest)[0])
    assert "--select fqn:p.staging.t" in cmd
    assert "--indirect-selection empty" in cmd


def test_qualify_replaces_conflicting_indirect_selection():
    manifest = _manifest({"test.p.staging.t": _test("t", ["p", "staging", "t"])})
    key = glue.generate_task_key("test.p.staging.t")
    tasks = [_notebook_task(key, ["dbt test --select t --indirect-selection=cautious"])]
    (cmd,) = _commands_of(glue._qualify_selectors(tasks, manifest)[0])
    assert "--indirect-selection empty" in cmd
    assert "cautious" not in cmd


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
                assert "--select fqn:orders_analytics." in cmd   # every selector qualified
                assert "--vars" not in cmd                        # no command-level vars


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_build_tasks_rejects_extra_vars(monkeypatch):
    monkeypatch.setattr(glue, "EXTRA_DBT_COMMAND_OPTIONS", "--vars '{a: 1}'")
    with pytest.raises(RuntimeError, match="--vars"):
        glue._build_tasks("dev")
