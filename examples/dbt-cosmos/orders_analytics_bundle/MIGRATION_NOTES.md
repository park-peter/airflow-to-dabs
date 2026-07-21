# Migration Notes: orders_analytics

Converted from Airflow DAG `orders_analytics` (astronomer-cosmos) using dbt factory mode.

## Conversion decisions

| Airflow construct | Conversion | Notes |
|---|---|---|
| `PythonOperator ingest_orders` | `notebook_task` (`src/ingest_orders.py`) | Callable body extracted. |
| cosmos `DbtTaskGroup dbt_transform` | Separate Python-generated job (`resources/orders_analytics_dbt_job.py`) triggered via `run_job_task` | Cosmos renders dbt models as Airflow tasks at runtime from `manifest.json`; databricks-dbt-factory regenerates the same per-model task graph natively from the same manifest. The group is not translated task-by-task — the generator is swapped. |
| `PythonOperator publish_metrics` | `notebook_task` (`src/publish_metrics.py`) | Callable body extracted. |
| `schedule_interval="@daily"` | `schedule.quartz_cron_expression: 0 0 0 * * ?` (UTC) | |
| `default_args.retries: 2` / `retry_delay: 5m` | Task-level `max_retries: 2`, `min_retry_interval_millis: 300000` | Applied to YAML-job notebook tasks. dbt job tasks rely on Lakeflow repair/rerun. |
| `default_args.email` | `email_notifications.on_failure` | |
| cosmos `DatabricksTokenProfileMapping` | `dbt_profiles/profiles.yml` with `DBT_HOST` / `DBT_ACCESS_TOKEN` injected by the runner notebook | No Airflow connection needed; the runner uses the notebook context token. |
| cosmos `RenderConfig(test_behavior=AFTER_EACH)` | Factory default: one test task per test node, downstream models gated on tests | Equivalent behavior. |

## Two-job layout

- `orders_analytics_job` (YAML): ingest → `run_job_task` → publish.
- `orders_analytics_dbt_job` (Python, generated at deploy time): 6 tasks — 1 seed, 3 models, 2 tests — dependencies wired from the dbt DAG. Per-model retry and repair happen inside this job.

## Action items before deploying

1. Replace `<WAREHOUSE_ID>` in `dbt_profiles/profiles.yml` (both targets) with the SQL warehouse that should execute dbt SQL.
2. Replace `<SERVICE_PRINCIPAL>` in `databricks.yml` for the prod target.
3. Confirm catalog/schema (`main.analytics`) matches your environment; adjust `dbt_profiles/profiles.yml`, `models/staging/schema.yml` (source), and the `src/` notebooks together.
4. Run `make setup && make manifest` before the first deploy — the PyDABs hook needs the venv and `target/dev/manifest.json`. For prod: `make deploy TARGET=prod` (parses and deploys against the prod profile target; never reuse a dev-parsed manifest for prod).

## Runtime dbt vars

Static vars live in the committed `dbt_vars.json` at the bundle root (required; `{}` here — this DAG has none). `make manifest` feeds it to `dbt parse --vars`, and the runner falls back to it whenever the `dbt_vars` job parameter is an empty object — parse time and run time always agree. A runtime override that differs from the file also bypasses the parse-cache injection so manifest-resolved settings (hooks, materializations, grants) are re-rendered with the override; each task then pays the dbt parse cost. The parent job exposes a `dbt_vars` parameter (default `{}`) and forwards it through the `run_job_task`; a non-empty runtime value REPLACES the whole static dict, so callers must pass the complete set. **Only safe for vars that do not change the dbt graph** (enabled nodes, dependencies, schemas, aliases) — the task graph and parse cache were compiled at deploy time. If a var changes the graph, fall back to a single `dbt_task`.

## Caveats

- **Selectors:** factory mode explodes the entire manifest. This DAG's cosmos group ran the whole project (no `RenderConfig(select=...)`), so semantics are unchanged. If your DAG subsets the project with selectors, converting to factory mode runs more than Airflow did — confirm intentionally or fall back to a single `dbt_task`.
- **full_refresh:** never applied automatically. A detected `full_refresh=True` in the source DAG is a manual-review decision (it is also an invalid flag for `dbt test`, so it must not be added globally).
- **Selector rewriting + fail-closed checks:** the hook rewrites every generated selector to the node's full FQN (`--select fqn:...`) and pins test commands to `--indirect-selection empty` — dbt bare-name selectors also match FQN/path components, and FQN selectors match by prefix, so unqualified selection over-selects on duplicate or directory-like names. It refuses to generate the job when the project defines dbt unit tests (databricks-dbt-factory 0.2.1 silently drops them), when distinct node names sanitize to the same task key (PyDABs would silently keep only one task), or when a generated selector does not resolve to exactly its own node — checked with dbt's own `is_selected_node` matcher imported at deploy time, so it tracks dbt's real semantics (prefix matching, leaf shortcuts, versioned models, wildcard slurp, package-stripped retry). It also rejects any FQN component (package, directory, or name) outside the `[A-Za-z0-9_.-]` allowlist (dbt's selector grammar or the runner's `shlex.split` would misparse it), and rejects a dbt command that carries its own `--vars` (vars must use the canonical `dbt_vars.json`/`dbt_vars` channel so parse-time and run-time agree). Fall back to a single `dbt_task` if hit.
- **Serverless only:** generated tasks run the runner notebook on serverless with a pre-built base environment (`dbt_serverless_env.yaml`, pinning the venv's exact dbt-databricks AND dbt-core — dbt-core parity is required for the selector-exactness guarantee). For classic compute, set `job_cluster_key` in `DbtTaskOptions` and define the cluster on the job **and install dbt on it, pinning both packages exactly** (cluster library or task `libraries: [{pypi: {package: 'dbt-databricks==<pinned>'}}, {pypi: {package: 'dbt-core==<pinned>'}}]` — the runner imports `dbt.cli.main`; pin dbt-core too so classic runs keep the same matcher/runtime parity as serverless; base environments are serverless-only). The cluster only runs dbt's parse/dispatch; SQL still executes on the warehouse from `profiles.yml`.
- **One dbt project per bundle:** factory mode supports a single dbt project, colocated at the bundle root. Multiple dbt projects → split bundles.
- **Large projects (1,000-task per-job limit):** one task per dbt node, and a Databricks job holds at most 1,000 tasks. Run `make task-count` to see unbundled vs bundled counts (this project: 6 unbundled / 5 bundled). If the unbundled count exceeds ~900, set `BUNDLE_TESTS = True` in `resources/orders_analytics_dbt_job.py` to collapse each resource's single-model tests into one `tests_<resource>` task (coarser retry granularity: a model's tests rerun together). If over 1,000 even bundled, split by dbt tag into multiple factory jobs, await a dbt-factory sub-job API, or use a single `dbt_task`. The glue fails closed above 1,000 tasks at deploy time.
- **Generated files:** `target/<target>/manifest.json` and `dbt_serverless_env.yaml` are git-ignored, so they are absent after a fresh clone. `make validate` and `make deploy` regenerate them — don't edit them by hand. To change dbt versions, edit the `pyproject.toml` pins and re-run `make setup`. Run `make test` to exercise the glue's selector/vars/fail-closed guards (pure-logic tests need no dbt; end-to-end tests run once a manifest exists).
