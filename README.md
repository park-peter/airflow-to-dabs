# airflow-to-dabs

A coding agent skill that converts Apache Airflow DAGs into [Databricks Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/) projects (formerly Databricks Asset Bundles; DABs).

Given an Airflow DAG file, the agent produces a complete bundle project — `databricks.yml`, `resources/*.yml` job definitions, and extracted `src/` source files — ready for `databricks bundle deploy`.

## Platform Support

| Platform | Instruction File | Global (personal) | Project-scoped |
|----------|------------------|--------------------|----------------|
| **Cursor** | `SKILL.md` | `~/.cursor/skills/airflow-to-dabs/` | `.cursor/skills/airflow-to-dabs/` |
| **Claude Code** | `SKILL.md` | `~/.claude/skills/airflow-to-dabs/` | `.claude/skills/airflow-to-dabs/` |
| **Codex CLI** | `AGENTS.md` | `~/.codex/AGENTS.md` | `./AGENTS.md` |
| **VS Code + Copilot** | `copilot-instructions.md` | — | `.github/copilot-instructions.md` |

## What It Does

- Parses Airflow DAG files to extract tasks, dependencies, operators, schedules, and parameters
- Maps **40+ Airflow operator types** (including all [Databricks provider operators](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/index.html)) to DABs task type equivalents using a [tiered mapping system](references/operator-mapping.md)
- **Source-aware routing**: maps by connection, not class alone (`operator → connection → intent → direction → destination → strategy`) — Databricks SQL → `sql_task`, remote federatable DB → Lakehouse Federation, recurring ingestion → Lakeflow Connect; fail-closed on unresolved connections
- **Lakeflow Connect ingestion**: routes recurring source→Delta ingestion (CDC, query-based, and foreign-catalog incl. Snowflake→Delta) to a DABs managed-ingestion pipeline (see [`references/lakeflow-connect.md`](references/lakeflow-connect.md))
- **Snowflake operators**: federation (read), query-based foreign-catalog ingestion (recurring copy), or connector notebook — by intent
- Converts Airflow cron expressions and presets to Quartz cron format
- Converts Airflow sensors (S3, HDFS, file, table, external task) to DABs triggers (`file_arrival`, `table_update`)
- Extracts inline Python, SQL, and bash into standalone source files
- Converts Jinja template variables (`{{ ds }}`, `{{ params.x }}`) to DABs dynamic value references
- **dbt factory mode (default for dbt workloads)**: converts dbt workloads — including [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos) `DbtDag`/`DbtTaskGroup` — into a separate Lakeflow job with one task per dbt model/seed/snapshot/test, generated at deploy time from the dbt manifest via PyDABs and [databricks-dbt-factory](https://github.com/mwojtyczka/databricks-dbt-factory); single `dbt_task` as fallback
- Generates `MIGRATION_NOTES.md` documenting conversion decisions and manual action items
- **Hadoop/HDFS migration support**: detects `spark-submit` in BashOperator/SSHOperator, cleans up YARN configs, maps HDFS paths, converts HiveQL, handles Sqoop alternatives
- Covers Airflow edge patterns including TaskFlow dataflow, dynamic task mapping (`.expand()`) and mapped task groups (`@task_group.expand()` → `for_each_task` + child job), and timetable/dataset scheduling notes
- **Airflow 3 support**: recognizes the `airflow.sdk` Task SDK and `apache-airflow-providers-standard` import paths, and maps `Asset`-based scheduling (see [`references/airflow3-migration.md`](references/airflow3-migration.md))

## Operator Coverage

| Tier | Description | Examples |
|------|-------------|----------|
| **1 — Direct** | 1:1 mapping to a DABs task type | `PythonOperator`, `BashOperator`, `SparkSubmitOperator`, `DatabricksSubmitRunOperator`, `DatabricksRunNowOperator`, `DatabricksNotebookOperator`, `DatabricksSqlOperator`, `DatabricksSQLStatementsOperator`, `DatabricksCopyIntoOperator`, `SQLExecuteQueryOperator`, `DbtOperator`, `TriggerDagRunOperator`, `HiveOperator`, `SSHOperator` |
| **2 — Semantic** | Requires reasoning about intent | cosmos `DbtDag`/`DbtTaskGroup`†, dynamic task mapping (`.expand()`), mapped task groups (`@task_group.expand()`), Snowflake operators (`SnowflakeSqlApiOperator`, `snowpark_task`), SQL data-quality checks (`SQLColumnCheckOperator`/`SQLTableCheckOperator`/…), cloud & messaging families (AWS/GCP/Azure/HTTP/SFTP/Kafka/Trino), `KubernetesPodOperator`, `DockerOperator`, `BranchPythonOperator`, `BranchDateTimeOperator`, `BranchDayOfWeekOperator`, `ShortCircuitOperator`, `DatabricksWorkflowTaskGroup`, `DatabricksTaskOperator`, `DatabricksCreateJobsOperator`, `SubDagOperator`, `TaskGroup`, `DummyOperator`, `EmailOperator`, `DatabricksReposCreateOperator`* |
| **3 — Sensor** | Converted to job-level triggers | `S3KeySensor`, `DatabricksSqlSensor`, `DatabricksPartitionSensor`, `DatabricksSQLStatementsSensor`, `HdfsSensor`, `FileSensor`, `ExternalTaskSensor`, `SqlSensor`, `TimeSensor`, `BashSensor`, `PythonSensor` |
| **4 — Unsupported** | Flagged for manual review | Custom operators, `DbtCloudRunJobOperator`, `SqoopOperator`, `PigOperator`, XCom-heavy patterns |

\* `DatabricksReposCreateOperator`, `DatabricksReposUpdateOperator`, and `DatabricksReposDeleteOperator` are infrastructure/repo-management operators with no DABs job task equivalent — they are omitted and noted in `MIGRATION_NOTES.md`.

† dbt workloads (cosmos, dbt CLI operators, bash `dbt run`) default to **dbt factory mode** — a separate Python-generated job with one task per dbt object — with a single `dbt_task` as the documented fallback. See the dbt conversion decision point in [`references/operator-mapping.md`](references/operator-mapping.md).

Full mapping details: [`references/operator-mapping.md`](references/operator-mapping.md)

## Installation

### Quick install (recommended)

Clone the repo, inspect the script if you like, then run it:

```bash
git clone https://github.com/park-peter/airflow-to-dabs.git
cd airflow-to-dabs
./install.sh
```

The interactive installer prompts you to choose a platform and scope:

```
Select platform:
  1) Cursor
  2) Claude Code
  3) Codex CLI
  4) VS Code + Copilot

Select scope:
  1) Global (all projects)
  2) Project (current directory only)
```

### Non-interactive (flags)

```bash
./install.sh --platform cursor --scope global
```

### Uninstall

```bash
./install.sh --platform cursor --scope global --uninstall
```

### Remote one-liner (optional)

```bash
curl -fsSL https://raw.githubusercontent.com/park-peter/airflow-to-dabs/main/install.sh | sh
```

Pass flags with `sh -s --`:

```bash
curl -fsSL https://raw.githubusercontent.com/park-peter/airflow-to-dabs/main/install.sh | sh -s -- --platform claude --scope project
```

### Manual installation

<details>
<summary><strong>Cursor</strong></summary>

**Global** (all projects):

```bash
mkdir -p ~/.cursor/skills
if [ -d ~/.cursor/skills/airflow-to-dabs/.git ]; then
  git -C ~/.cursor/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.cursor/skills/airflow-to-dabs
fi
```

**Project-scoped** (single project):

```bash
mkdir -p .cursor/skills
if [ -d .cursor/skills/airflow-to-dabs/.git ]; then
  git -C .cursor/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .cursor/skills/airflow-to-dabs
fi
```

</details>

<details>
<summary><strong>Claude Code</strong></summary>

**Personal** (all projects):

```bash
mkdir -p ~/.claude/skills
if [ -d ~/.claude/skills/airflow-to-dabs/.git ]; then
  git -C ~/.claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.claude/skills/airflow-to-dabs
fi
```

**Project-scoped** (single project):

```bash
mkdir -p .claude/skills
if [ -d .claude/skills/airflow-to-dabs/.git ]; then
  git -C .claude/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .claude/skills/airflow-to-dabs
fi
```

</details>

<details>
<summary><strong>Codex CLI</strong></summary>

**Global** (all projects):

```bash
mkdir -p ~/.codex/skills
if [ -d ~/.codex/skills/airflow-to-dabs/.git ]; then
  git -C ~/.codex/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git ~/.codex/skills/airflow-to-dabs
fi
touch ~/.codex/AGENTS.md
cp ~/.codex/AGENTS.md ~/.codex/AGENTS.md.bak.$(date +%Y%m%d%H%M%S)
BEGIN_MARK="<!-- BEGIN airflow-to-dabs -->"
END_MARK="<!-- END airflow-to-dabs -->"
if grep -Fq "$BEGIN_MARK" ~/.codex/AGENTS.md && grep -Fq "$END_MARK" ~/.codex/AGENTS.md; then
  awk -v begin="$BEGIN_MARK" -v end="$END_MARK" '
    $0 == begin {skip=1; next}
    $0 == end {skip=0; next}
    !skip {print}
  ' ~/.codex/AGENTS.md > ~/.codex/AGENTS.md.tmp
  mv ~/.codex/AGENTS.md.tmp ~/.codex/AGENTS.md
fi
{
  [ -s ~/.codex/AGENTS.md ] && echo
  echo "$BEGIN_MARK"
  cat ~/.codex/skills/airflow-to-dabs/AGENTS.md
  echo "$END_MARK"
} >> ~/.codex/AGENTS.md
```

**Project-scoped** (single project):

```bash
mkdir -p .codex/skills
if [ -d .codex/skills/airflow-to-dabs/.git ]; then
  git -C .codex/skills/airflow-to-dabs pull --ff-only
else
  git clone https://github.com/park-peter/airflow-to-dabs.git .codex/skills/airflow-to-dabs
fi
touch ./AGENTS.md
cp ./AGENTS.md ./AGENTS.md.bak.$(date +%Y%m%d%H%M%S)
BEGIN_MARK="<!-- BEGIN airflow-to-dabs -->"
END_MARK="<!-- END airflow-to-dabs -->"
if grep -Fq "$BEGIN_MARK" ./AGENTS.md && grep -Fq "$END_MARK" ./AGENTS.md; then
  awk -v begin="$BEGIN_MARK" -v end="$END_MARK" '
    $0 == begin {skip=1; next}
    $0 == end {skip=0; next}
    !skip {print}
  ' ./AGENTS.md > ./AGENTS.md.tmp
  mv ./AGENTS.md.tmp ./AGENTS.md
fi
{
  [ -s ./AGENTS.md ] && echo
  echo "$BEGIN_MARK"
  cat .codex/skills/airflow-to-dabs/AGENTS.md
  echo "$END_MARK"
} >> ./AGENTS.md
```

</details>

<details>
<summary><strong>VS Code + Copilot</strong></summary>

**Project-scoped** (project-only — no global install):

```bash
SKILL_DIR=$(mktemp -d)
git clone https://github.com/park-peter/airflow-to-dabs.git "$SKILL_DIR"
mkdir -p .github
touch .github/copilot-instructions.md
cp .github/copilot-instructions.md .github/copilot-instructions.md.bak.$(date +%Y%m%d%H%M%S)

BEGIN_MARK="<!-- BEGIN airflow-to-dabs -->"
END_MARK="<!-- END airflow-to-dabs -->"

if grep -Fq "$BEGIN_MARK" .github/copilot-instructions.md && grep -Fq "$END_MARK" .github/copilot-instructions.md; then
  awk -v begin="$BEGIN_MARK" -v end="$END_MARK" '
    $0 == begin {skip=1; next}
    $0 == end {skip=0; next}
    !skip {print}
  ' .github/copilot-instructions.md > .github/copilot-instructions.md.tmp
  mv .github/copilot-instructions.md.tmp .github/copilot-instructions.md
fi

{
  [ -s .github/copilot-instructions.md ] && echo
  echo "$BEGIN_MARK"
  cat "$SKILL_DIR/copilot-instructions.md"
  echo "$END_MARK"
} >> .github/copilot-instructions.md

rm -rf "$SKILL_DIR"
```

</details>

## Usage

The agent follows a 4-phase workflow: **Parse** the DAG, **Map** operators to DABs task types, **Generate** the bundle project, and **Review** for correctness.

### Convert multiple DAGs (default)

> "Convert all DAGs in the dags/ directory to Databricks Asset Bundles"

Produces a single bundle with one `databricks.yml` and a separate job resource per DAG. Cross-DAG dependencies resolve within the same bundle.

### Convert a single DAG

> "Convert my_etl_dag.py to a Databricks Asset Bundle"

Produces a standalone bundle directory for that one DAG.

### Convert a dbt / cosmos DAG (factory mode)

> "Convert orders_analytics_dag.py to a Databricks Asset Bundle — the dbt project is at ./dbt/orders_analytics"

Produces a two-job bundle: a YAML job for the non-dbt tasks with a `run_job_task` hop, plus a Python-generated dbt job (one task per dbt model/seed/snapshot/test) built at deploy time from the dbt manifest via PyDABs. See [`examples/dbt-cosmos/`](examples/dbt-cosmos/) for a complete conversion.

### Convert an Airflow 3 DAG with dynamic mapping / mapped task groups

> "Convert regional_ingest_dag.py to a Databricks Asset Bundle"

Recognizes the `airflow.sdk` and `apache-airflow-providers-standard` imports, maps `.expand()` to a `for_each_task`, and turns a mapped task group (`@task_group.expand()`) into a `for_each_task` → `run_job_task` → child job holding the subgraph. See [`examples/dynamic-mapping/`](examples/dynamic-mapping/) for a complete conversion.

### Convert a recurring ingestion DAG (Lakeflow Connect)

> "Convert orders_replication_dag.py — it replicates a Snowflake table hourly"

Routes recurring source→Delta ingestion to a Lakeflow Connect managed-ingestion pipeline (Snowflake via a UC foreign catalog, `ingest_from_uc_foreign_catalog`) with a `pipeline_task` hop into the downstream transform. See [`examples/lakeflow-connect/`](examples/lakeflow-connect/) for a complete conversion.

## flowx Provider Profile

[`providers/flowx-gap-resolver/`](providers/flowx-gap-resolver/) holds a machine-readable provider profile for flowx's fingerprint-bound Airflow gap workflow. In this mode:

- flowx owns DAG parsing, task identity, graph structure, policy, IR, and bundle packaging.
- The provider receives one `GapEnvelope` and returns one constrained `AgenticResolution`, carrying task, graph, provider, and request hashes.
- A resolution can attach one notebook, SQL, or Spark Python leaf payload, request user input with per-argument dispositions, or defer when a faithful migration requires graph or resource changes.
- Static DAG run timeouts and failure notifications are preserved as Job settings; disabled policies are explicit no-ops. Cross-run, retry-email, SLA-callback, auto-pause, dynamic-timeout, and task-environment semantics are blocking gaps.
- The provider never reparses the DAG or generates a competing bundle.

[`provider.json`](providers/flowx-gap-resolver/provider.json) declares contract compatibility, knowledge files, and fixture paths. [`PROFILE.md`](providers/flowx-gap-resolver/PROFILE.md) is the agent entrypoint. The paired JSON fixtures cover notebook, SQL, Spark Python, `needs_input`, and `deferred` outcomes and can be replayed by flowx as interoperability tests.

## Post-Generation Configuration

The generated bundle uses placeholders for environment-specific values. Replace these before deploying, or provide the values in your prompt to skip this step (e.g., "use warehouse ID abc123 and spark version 15.4.x-scala2.12").

| Placeholder | Location | Example value |
|---|---|---|
| `<DEV_WORKSPACE_URL>` | `databricks.yml` → `targets.dev.workspace.host` | `https://my-dev.cloud.databricks.com` |
| `<PROD_WORKSPACE_URL>` | `databricks.yml` → `targets.prod.workspace.host` | `https://my-prod.cloud.databricks.com` |
| `<SERVICE_PRINCIPAL>` | `databricks.yml` → `targets.prod.run_as` | `my-deploy-sp` |
| `<SPARK_VERSION>` | `databricks.yml` → `variables.spark_version` | `15.4.x-scala2.12` |
| `<NODE_TYPE_ID>` | `databricks.yml` → `variables.node_type_id` | `i3.xlarge` (AWS), `Standard_D4s_v3` (Azure) |
| `<WAREHOUSE_ID>` | `databricks.yml` → `variables.warehouse_id` | `abc123def456` |
| `<WAREHOUSE_ID>` (factory mode) | `dbt_profiles/profiles.yml` → `http_path` | `/sql/1.0/warehouses/abc123def456` |
| `<DBT_PROFILE_NAME>` (factory mode) | `dbt_profiles/profiles.yml` — must match `profile:` in `dbt_project.yml` | `orders_analytics` |
| `<DEV_CATALOG>` / `<DEV_SCHEMA>` (factory mode) | `dbt_profiles/profiles.yml` → `catalog` / `schema` | `main` / `analytics` |

> **Tip:** If you've already configured auth via `~/.databrickscfg` or `DATABRICKS_HOST`, you can remove `workspace.host` from targets entirely — the CLI picks it up automatically.

## Repository Tests

`make test` runs the skill's own checks, and CI runs the same targets on every push and pull request:

```bash
make test             # contract + dbt glue suites
make test-contracts   # cross-surface rule coverage and structural checks
make test-glue        # regression tests for the generated PyDABs dbt glue
make validate         # schema-validate the checked-in example bundles
```

`tests/test_skill_contracts.py` matches each hardening rule through the `<!-- contract: id -->` anchors carried by `SKILL.md`, `AGENTS.md`, and `copilot-instructions.md`, so a rule dropped from one surface fails the build while rewording does not. Add an anchor to all three surfaces when adding a rule.

## Validation

After filling in placeholders, validate the bundle before deploying:

```bash
databricks bundle validate -t dev
```

For factory-mode bundles, install the venv and generate the dbt manifest first — `bundle validate` executes the PyDABs hook, which needs both:

```bash
make setup      # uv sync --dev
make manifest   # dbt deps + dbt parse (no warehouse connection needed)
databricks bundle validate -t dev
```

If Databricks auth is not configured yet, run schema checks offline first:

```bash
databricks bundle schema
```

## Reference Files

| File | Description |
|------|-------------|
| [`references/operator-mapping.md`](references/operator-mapping.md) | Tier 1–4 mapping table with side-by-side Airflow/DABs YAML examples |
| [`references/dab-schema-reference.md`](references/dab-schema-reference.md) | Condensed DABs YAML schema — all task types, triggers, clusters, variables |
| [`references/schedule-trigger-mapping.md`](references/schedule-trigger-mapping.md) | Cron conversion, sensor-to-trigger mapping, Airflow 3 Asset/`AssetOrTimeSchedule` scheduling, `default_args` mapping, Jinja variable conversion |
| [`references/conversion-examples.md`](references/conversion-examples.md) | 6 complete before/after examples (ETL chain, branching, sensor-triggered, multi-system, cosmos dbt factory mode, Airflow 3 dynamic mapping + mapped task group) |
| [`references/airflow3-migration.md`](references/airflow3-migration.md) | Airflow 3 recognition — `airflow.sdk` + `apache-airflow-providers-standard` imports, Assets vs Datasets, asset scheduling, deferrable/native-async/resumable execution model, removed operators, recognize→safe-map→flag checklist |
| [`references/lakeflow-connect.md`](references/lakeflow-connect.md) | Lakeflow Connect ingestion target — when to use it vs a Jobs task, CDC/query-based/foreign-catalog styles (incl. Snowflake→Delta), eligibility, DABs generation contract, continuous-vs-triggered orchestration, MIGRATION_NOTES checklist |
| [`references/hadoop-migration-guide.md`](references/hadoop-migration-guide.md) | HDFS path conversion, YARN config cleanup, Hive-to-UC mapping, spark-submit detection, Sqoop alternatives, bulk conversion guidance |
| [`assets/templates/`](assets/templates/) | Skeleton `databricks.yml`, job resource, and dbt factory mode templates (PyDABs hook, pyproject, Makefile, profiles) |
| [`AGENTS.md`](AGENTS.md) | Codex CLI instruction file (same workflow as SKILL.md) |

## Example Output

**Multiple DAGs (default)** — single bundle, multiple jobs:

```
airflow-migration/
  databricks.yml              # Single bundle config, shared variables/targets
  resources/
    etl_pipeline_job.yml       # One job resource per DAG
    reporting_pipeline_job.yml
    data_quality_job.yml
  src/
    etl_pipeline/              # Source files namespaced per DAG
      extract.py
      transform.py
      load.py
    reporting_pipeline/
      generate_report.sql
    data_quality/
      check_nulls.py
  MIGRATION_NOTES.md           # Consolidated notes for all DAGs
```

Cross-DAG dependencies (e.g., `TriggerDagRunOperator`) resolve via `${resources.jobs.<name>.id}` within the same bundle.

**Single DAG** — one standalone bundle:

```
daily-etl-pipeline/
  databricks.yml              # Bundle config with dev/prod targets
  resources/
    daily_etl_pipeline_job.yml # Job with schedule, clusters, 3 tasks
  src/
    extract.py                 # Notebook extracted from PythonOperator
    transform.py
    load.py
  MIGRATION_NOTES.md           # Conversion decisions
```

See [`references/conversion-examples.md`](references/conversion-examples.md) for full before/after walkthroughs.

## License

MIT
