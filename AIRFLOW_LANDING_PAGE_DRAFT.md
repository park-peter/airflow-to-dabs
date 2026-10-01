# DRAFT — Confluence page: "Airflow to Databricks Migration"

**Not published.** Placement: FE space, sibling of the ADF Migration Play (parent `1094161644`).
Requested shortlink: `go/airflowmigration`. Strip the `<!-- REVIEW -->` comments before publishing.

---

self-link: [go/airflowmigration](https://databricks.atlassian.net/wiki/spaces/FE/pages/PLACEHOLDER)

**Owner:** Peter Park · **Last reviewed:** 2026-08-07 · **Reviewed:** quarterly

## Overview

Apache Airflow is the most common external orchestrator of Databricks jobs — self-hosted or managed
(MWAA, Google Cloud Composer, Astronomer, ADF Managed Airflow), usually submitting work through the
Databricks provider operators.

**What this page is:** the entry point for converting Airflow DAGs to **Lakeflow Jobs** packaged as
Declarative Automation Bundles (DABs). It routes you to the tooling and assets that exist today.

**What's different from ADF:** Airflow isn't deprecated, and it's often a deliberate architectural
choice. Migration is usually **partial, not wholesale** — read [When not to
migrate](#when-not-to-migrate) before pitching. Migrating from ADF instead? →
[ADF Migration Play](https://databricks.atlassian.net/wiki/spaces/FE/pages/5977931847) (`go/adfmigration`).

## Start here

| You want to… | Use |
|---|---|
| Assess an Airflow estate (inventory, complexity, scope) | **`flowx discover`** |
| Migrate an estate — assess, convert at volume, package | **`flowx`** (recommended starting point) |
| Just convert some DAGs, no estate assessment needed | **`airflow-to-dabs` skill** (standalone) |
| Convert DAGs flowx flags as gaps, or dbt / ingestion / Airflow 3-heavy DAGs | **`airflow-to-dabs` skill** |
| Position Lakeflow Jobs vs Airflow | [Positioning](#positioning) |
| Decide whether to migrate at all | [When not to migrate](#when-not-to-migrate) |

## Migration Tooling

Two Field Engineering assets, both emitting DABs. **Start with `flowx`** for anything estate-sized — it
gives you the inventory and complexity report you need to scope the work. The `airflow-to-dabs` skill
also works **standalone** when conversion is all you need (a handful of DAGs, a single team, a
proof-of-concept), and is the tool for DAGs `flowx` routes to a gap.

- **[flowx](https://github.com/databricks-field-eng/flowx)** — deterministic Python library + agent
  skills translating a source orchestrator into Lakeflow Jobs. Supports **ADF and Airflow** on a shared
  Pipeline IR. Run `/flowx:flowx-migrate` end-to-end, or the phases individually (`@` prefix in Genie
  Code); also runs as an MCP server on a Databricks App.
  - `discover` — static `ast` parse of DAG `.py` files (no Airflow install, no DAG execution) →
    independently reconciled `inventory.json` + complexity report `profile_report.csv`. Audited
    constructs become translations, linked failing placeholders, explicit exclusions, or a nonzero
    reconciliation failure. **This is the assessment artifact — use it to scope a pilot.**
  - `convert` — ~35 operator/sensor families deterministic, agentic fallback for the rest. Unmapped
    types become linked placeholders in `gaps.json`; the placeholder notebook raises
    `NotImplementedError` until migrated.
  - `package` — `databricks.yml`, per-job YAML, notebooks, and UC volume/secret/connection setup scripts.
  - Covers Python/Bash/SSH/SparkSubmit (incl. `spark-submit` lift), Databricks provider operators, SQL
    operators, TaskFlow (XCom → `dbutils.jobs.taskValues`), sensors → triggers, dbt CLI + cosmos, cron
    → Quartz, `trigger_rule` → `run_if`.

- **[airflow-to-dabs skill](https://github.com/park-peter/airflow-to-dabs)** — agent skill (Cursor /
  Claude Code / Codex / VS Code Copilot) that converts DAG files into a complete, deployable bundle:
  `databricks.yml`, `resources/*.yml`, extracted `src/`, and `MIGRATION_NOTES.md`. **Usable on its own**
  — point the agent at a DAG and it produces the bundle — and the right tool for the patterns that are
  easiest to get subtly wrong. Install: `git clone … && ./install.sh`.
  - **Source-aware routing** — maps by *connection*, not operator class: Databricks SQL → `sql_task`;
    remote federatable DB read → Lakehouse Federation; recurring source→Delta → **Lakeflow Connect**;
    cloud-storage files → Auto Loader. Fail-closed on unresolved connections; never inlines credentials.
  - **dbt** — cosmos `DbtDag`/`DbtTaskGroup` and dbt CLI → a separate Lakeflow job with **one task per
    dbt node**, generated at deploy time from `manifest.json` via PyDABs.
  - **Airflow 3** — `airflow.sdk` Task SDK and standard-provider import paths, `Asset` scheduling,
    scheduling defaults, native async and resumable operators.
  - **Execution dates + backfill** — classifies `{{ ds }}` as wall-clock vs logical/partition and maps
    it to a job parameter that native [Databricks
    backfill](https://docs.databricks.com/aws/en/jobs/backfill-jobs) can override with
    `{{backfill.iso_date}}`. A common source of silently-wrong conversions.
  - **Edge patterns** — TaskFlow dataflow, `.expand()`, mapped task groups → `for_each_task` + child
    job, sensors → `file_arrival`/`table_update`. Emits `MIGRATION_NOTES.md` with every decision and
    manual action item.

### Which one?

**Recommended: start with `flowx`.** It's the only one that inventories and complexity-scores an estate,
and it converts the bulk deterministically. Then hand what it records in `gaps.json` to the skill.

**The skill standalone is a legitimate path** when you don't need an estate assessment — a handful of
DAGs, one team, a POC, or a customer who just wants to see a faithful conversion of their gnarliest
DAG. It takes DAG files straight to a deployable bundle; no `flowx` run required.

| | flowx | airflow-to-dabs |
|---|---|---|
| Estate inventory + complexity/effort report | ✅ | — |
| Bulk conversion across many DAGs | ✅ | works, but per-DAG |
| Runs standalone | ✅ | ✅ |
| Branching (`BranchPythonOperator`/`ShortCircuitOperator`), `@task_group`, non-literal `.expand()`, mapped task groups | placeholder + gap | mapped |
| S3/cloud sensors, `ExternalTaskSensor`, wider sensor families | partial / placeholder | mapped |
| dbt (cosmos + CLI) | ✅ | ✅ (+ per-node factory job via PyDABs) |
| Lakeflow Connect as an ingestion target, Snowflake routing | — | ✅ |
| Airflow 3 (`airflow.sdk`, Assets, async/resumable) | partial | ✅ |
| Execution-date/backfill semantics | run_date parameter | ✅ (+ wall-clock vs partition classification) |

Coverage boundaries move as both tools develop — flowx's current matrix is in
`skills/flowx-convert/sources/airflow-coverage.md`.

<!-- REVIEW: flowx is v0.1.0 — decide what to promise for customer-facing use, and whether the
MCP/Databricks App path is field-ready, before this page sends people to it at scale. -->

## Positioning

- [**Airflow Battlecard**](https://docs.google.com/presentation/d/1y3xE0247I9umn5ebbGi8O96zCJgm9EujrMdGA7vzzVs) (`go/airflow/battle`): the primary compete asset.
- [**Lakeflow Jobs Orchestration battlecard**](https://docs.google.com/presentation/d/1HaMnowdhEtbKQ5CcFbaaoqjkRDCSmeyDyXeEcybAGEg) (`go/orchestration/battle`)
- [**Blog: How to move from Apache Airflow to Lakeflow Jobs**](https://www.databricks.com/blog/how-move-apache-airflowr-databricks-lakeflow-jobs): customer-shareable.
- Visit [go/lakeflow](https://home.databricks.com/sales/field-performance/products/lakeflow/) for more.

The argument in one line: self-hosted Airflow carries cluster and version-upgrade burden, managed
Airflow adds licence + right-sizing cost, and Lakeflow Jobs is included and fully managed in the same
control plane as lineage, governance, repair/retry, and alerting. Converting classic job compute to
**serverless** during the migration is usually the strongest efficiency argument.

## Concept mapping

Maintained in the `airflow-to-dabs` skill's `references/`:

| Topic | Reference |
|---|---|
| Operator → task type (tiered, 40+ operators) | `operator-mapping.md` |
| Schedules, sensors, triggers, `{{ ds }}`/backfill | `schedule-trigger-mapping.md` |
| DABs job/task schema + dynamic value references | `dab-schema-reference.md` |
| Airflow 3 (`airflow.sdk`, Assets, async, resumable) | `airflow3-migration.md` |
| Lakeflow Connect as an ingestion target | `lakeflow-connect.md` |
| Hadoop/Hive/Sqoop-adjacent DAGs | `hadoop-migration-guide.md` |
| Worked end-to-end conversions | `conversion-examples.md` |

## When not to migrate

Be straight here — the teams running Airflow chose it on purpose, and an all-or-nothing pitch loses
credibility.

- **Heterogeneous orchestration.** If Airflow coordinates many non-Databricks systems, recommend
  **hybrid**: migrate the Databricks-heavy DAGs, keep Airflow for cross-system work.
- **External orchestration is pre-GA.** Lakeflow Jobs orchestrating external systems
  (`python_operator_task`) isn't GA yet — the honest reason some customers stay. Don't position around
  unreleased capability.
- **Deep custom-operator/plugin estates** mean real rewrite cost. Quantify with flowx `discover` before
  committing to scope.
- **Size on the Databricks-orchestrated subset**, not the whole DAG estate. Federated estates (many
  workspaces/teams) migrate **per team**, not as one project.

## Related

- [ADF Migration Play](https://databricks.atlassian.net/wiki/spaces/FE/pages/5977931847) (`go/adfmigration`) — sibling motion
- [Orchestration Layer](https://databricks.atlassian.net/wiki/spaces/FE/pages/4958291579)
- [External Orchestration with Lakeflow Jobs](https://databricks.atlassian.net/wiki/spaces/UN/pages/5143168418) — status for non-Databricks systems
- [Orchestrate Lakeflow Jobs with Apache Airflow](https://docs.databricks.com/aws/en/jobs/how-to/use-airflow-with-jobs) — for customers *keeping* Airflow

## Questions

`flowx` and the `airflow-to-dabs` skill are both actively maintained — file issues/PRs on the repos, or
reach out to Peter Park.

---

<!-- ============ END OF PAGE CONTENT ============ -->

## Pre-publish checklist (not page content)

1. Create the page as an FE-space sibling of the ADF play (parent `1094161644`); request
   `go/airflowmigration`.
2. Fill the `PLACEHOLDER` self-link once the page ID exists.
3. Strip the `<!-- REVIEW -->` comment.
4. Add inbound links — this is what makes Glean rank it: Airflow Battlecard, Orchestration Layer page,
   `go/lakeflow`, ADF play ("migrating from Airflow instead? →"), flowx README, skill README.
5. Labels: `airflow`, `migration`, `lakeflow`, `lakeflow-jobs`, `orchestration`, `dabs`.
6. Announce once in `#field-devx` and the Lakeflow/orchestration channels.
7. Verify every link resolves for a reader who isn't you (especially the two GitHub repos).
