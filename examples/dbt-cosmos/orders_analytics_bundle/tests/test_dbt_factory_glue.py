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


def test_arity_probe_matches_the_installed_dbt_signature():
    from inspect import signature

    expected = len(signature(glue.is_selected_node).parameters) >= 3
    assert glue._SELECTOR_MATCHER_ACCEPTS_VERSIONED is expected
    assert glue._call_is_selected_node(["p", "staging", "stg"], "p.staging.stg", is_versioned=False)


def test_selector_matcher_supports_legacy_two_argument_dbt_api(monkeypatch):
    calls = []

    def legacy_is_selected_node(fqn, selector):
        calls.append((fqn, selector))
        return fqn == ["p", "staging", "stg"] and selector == "p.staging.stg"

    monkeypatch.setattr(glue, "is_selected_node", legacy_is_selected_node)
    monkeypatch.setattr(glue, "_SELECTOR_MATCHER_ACCEPTS_VERSIONED", False)

    assert glue._call_is_selected_node(
        ["p", "staging", "stg"], "p.staging.stg", is_versioned=False
    )
    assert calls == [(["p", "staging", "stg"], "p.staging.stg")]


def test_selector_matcher_passes_version_flag_to_current_dbt_api(monkeypatch):
    calls = []

    def current_is_selected_node(fqn, selector, is_versioned):
        calls.append((fqn, selector, is_versioned))
        return is_versioned

    monkeypatch.setattr(glue, "is_selected_node", current_is_selected_node)
    monkeypatch.setattr(glue, "_SELECTOR_MATCHER_ACCEPTS_VERSIONED", True)

    assert glue._call_is_selected_node(
        ["p", "m", "v2"], "p.m.v2", is_versioned=True
    )
    assert calls == [(["p", "m", "v2"], "p.m.v2", True)]


def test_exact_selectors_bundled_skips_equal_fqn_tests():
    # Equal-FQN tests would fail the per-test exactness scan, but in bundled mode they
    # are not selected individually — they run via their resource's `<resource>_test` task.
    m = _manifest(
        {
            "test.p.dup": _test("dup", ["p", "staging", "dup"]),
            "test.other.dup": _test("dup", ["p", "staging", "dup"]),
        }
    )
    glue._assert_exact_selectors(m, bundle_tests=True)  # no raise: test nodes excluded


def test_exact_selectors_bundled_still_catches_model_collision():
    # Bundling excludes only test nodes; model/seed/snapshot selectors are still checked.
    m = _manifest(
        {
            "model.p.marts": _model("marts", ["p", "marts"]),
            "model.p.marts.fct": _model("fct", ["p", "marts", "fct"]),
        }
    )
    with pytest.raises(RuntimeError, match="not exact"):
        glue._assert_exact_selectors(m, bundle_tests=True)


# --------------------------------------------------------------------------------------
# _assert_unique_task_keys — belt-and-suspenders: 0.3.1 guarantees unique keys, so this
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
                # 0.3.1 selects by full dot-joined FQN (no `fqn:` prefix); the package
                # segment is always present, so a bare-name selector would have no dot.
                assert re.search(r"--select\s+orders_analytics\.\S+", cmd)
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
def test_bundled_mode_collapses_tests_and_preserves_cautious():
    # Bundled test tasks are the `dbt test ... --indirect-selection cautious` commands
    # (0.3.1 keys them `<resource>_test`; match on the command, not the key name).
    tasks = glue._build_tasks("dev", bundle_tests=True)
    bundled = [
        c
        for t in tasks
        for c in _commands_of(t)
        if "dbt test" in c and "--indirect-selection cautious" in c
    ]
    assert bundled, "expected at least one bundled test task in bundled mode"
    for cmd in bundled:
        assert "--indirect-selection empty" not in cmd  # bundle selection not narrowed


@pytest.mark.skipif(
    not (_BUNDLE_ROOT / "target" / "dev" / "manifest.json").exists(),
    reason="target/dev/manifest.json is git-ignored; run `make manifest` first",
)
def test_bundled_test_selectors_resolve_to_the_resource():
    # The invariant that actually matters: each bundled test task's select must resolve
    # to its resource under dbt's own matcher. databricks-dbt-factory 0.3.1 selects by
    # the resource's full dot-joined FQN (with --indirect-selection cautious, which sweeps
    # in that resource's tests); confirm it resolves using the same matcher the glue's
    # exactness check imports. Bundled test tasks are matched by their cautious command,
    # not a task-key prefix (0.3.1 keys them `<resource>_test`).
    manifest = glue.read_dbt_manifest(
        str(_BUNDLE_ROOT / "target" / "dev" / "manifest.json")
    )
    resources = {  # resource full name (dotted fqn) -> is_versioned
        ".".join(n["fqn"]): (n.get("version") is not None)
        for n in manifest["nodes"].values()
        if n["resource_type"] in ("model", "seed", "snapshot")
    }
    tasks = glue._build_tasks("dev", bundle_tests=True)
    checked = 0
    for task in tasks:
        for cmd in _commands_of(task):
            if "dbt test" not in cmd or "--indirect-selection cautious" not in cmd:
                continue
            m = re.search(r"--select\s+(\S+)", cmd)
            assert m, f"bundled test command has no --select: {cmd!r}"
            selector = m.group(1)
            assert any(
                glue._node_selector_matches(selector, fqn.split("."), versioned)
                for fqn, versioned in resources.items()
            ), f"bundled selector {selector!r} matches no resource node"
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
