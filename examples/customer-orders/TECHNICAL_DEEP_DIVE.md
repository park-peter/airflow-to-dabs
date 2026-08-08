# airflow-to-dabs Skill Technical Deep Dive

This document explains how the `airflow-to-dabs` skill is built, how an agent consumes it, and how the included customer-orders demo was generated from an Airflow DAG into a Databricks bundle for Lakeflow Jobs.

The target audience is an internal technical team evaluating whether an agent skill can carry enough domain knowledge and workflow discipline to produce useful migration output.

## 1. What This Skill Is

`airflow-to-dabs` is an agent skill for converting Apache Airflow DAGs into Databricks bundle projects. Given one or more Airflow DAG Python files, the agent is instructed to produce:

```text
databricks.yml
resources/*.yml
src/*.py
src/*.sql
MIGRATION_NOTES.md
```

The generated bundle is intended to be deployable with:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

In Databricks terms, the skill targets Lakeflow Jobs packaged with Databricks Asset Bundles, also referred to in newer Databricks documentation as Declarative Automation Bundles.

## 2. Why Use a Skill Instead of a Static Converter

Airflow-to-Databricks migration is not a pure syntax transform. A static converter can handle simple cases, but real DAGs require judgment:

- An Airflow sensor might become a Lakeflow file trigger, table trigger, dependency, or polling task depending on intent.
- `BranchPythonOperator` might become a native `condition_task`, or might need a notebook that computes a task value first.
- `BashOperator` could be harmless shell glue, or it could hide a `spark-submit` command that should become a native Spark task.
- Airflow connections, variables, XComs, custom operators, and timetables require migration notes and sometimes human decisions.

The skill encodes the workflow, mappings, schemas, validation steps, and fallback rules while leaving the agent enough freedom to inspect the DAG and make context-aware choices.

## 3. Physical Repository Layout

The skill is packaged as a folder with one primary instruction file, companion platform instruction files, references, templates, and installation helpers:

```text
airflow-to-dabs/
  SKILL.md
  AGENTS.md
  copilot-instructions.md
  README.md
  install.sh
  references/
    operator-mapping.md
    dab-schema-reference.md
    schedule-trigger-mapping.md
    hadoop-migration-guide.md
    conversion-examples.md
  assets/
    templates/
      databricks.yml.tmpl
      job-resource.yml.tmpl
  examples/
    customer-orders/
      airflow/
        customer_orders_dag.py
      customer_orders_bundle/
        databricks.yml
        resources/customer_orders_job.yml
        src/*.py
        src/*.sql
        MIGRATION_NOTES.md
      CONVERSION_SUMMARY.md
      TECHNICAL_DEEP_DIVE.md
```

The core skill files are `SKILL.md`, `references/`, and `assets/templates/`. The other files make the skill easier to distribute across agents and easier for humans to install, inspect, and demo.

## 4. How Skills Are Structured

An agent skill uses progressive disclosure. The agent does not need to load every reference file immediately. It loads only what it needs as the task becomes clearer.

| Layer | Physical Location | When The Agent Sees It | Purpose |
|---|---|---|---|
| Metadata | `SKILL.md` YAML frontmatter | Always available to the skill runtime or installer | Lets the agent decide whether this skill applies. |
| Core instructions | `SKILL.md` body | Loaded after the skill triggers | Defines workflow, outputs, rules, and validation contract. |
| References | `references/*.md` | Loaded selectively | Holds large mapping tables, schema examples, schedule rules, and edge cases. |
| Assets | `assets/templates/*.tmpl` | Used when generating files | Provides starting skeletons for bundle YAML. |
| Platform adapters | `AGENTS.md`, `copilot-instructions.md` | Loaded by platforms that do not consume `SKILL.md` directly | Carries the same behavior into Codex, Copilot, and similar instruction systems. |

This matters because agent context is limited. The skill keeps the always-loaded surface small and pushes detailed mappings into references that are read only when needed.

## 5. `SKILL.md`: The Primary Agent Contract

`SKILL.md` has two parts: YAML frontmatter and Markdown instructions.

### 5.1 Frontmatter

The frontmatter is the discoverability layer:

```yaml
name: airflow-to-dabs
description: Converts Apache Airflow DAG files into Databricks Asset Bundles...
version: 1.0.0
author: park-peter
repository: https://github.com/park-peter/airflow-to-dabs
keywords:
  - airflow
  - databricks
  - migration
  - lakeflow
  - dabs
```

The most important fields are `name` and `description`. The description is intentionally trigger-rich: it mentions Airflow migration, DAG conversion, Databricks Lakeflow Jobs, DABs, and generated `databricks.yml` output. That gives the agent multiple semantic hooks to decide when the skill applies.

### 5.2 Body

The Markdown body defines the operating procedure:

1. Parse the Airflow DAG.
2. Map operators to Databricks task types.
3. Generate the bundle project.
4. Review and validate the output.

It also tells the agent what files to produce, when to read each reference file, and what checks must pass before the work is considered complete.

The body is deliberately procedural. It does not try to explain all of Airflow or Databricks. It tells the agent exactly what to inspect, what to generate, and how to validate the migration.

## 6. Reference Files

The references are where most of the domain knowledge lives.

| File | What It Encodes | When The Agent Reads It |
|---|---|---|
| `references/operator-mapping.md` | Airflow operator to Lakeflow/DABs task mappings, including direct mappings, semantic mappings, sensors, unsupported operators, and Databricks provider operators. | During operator classification and task generation. |
| `references/dab-schema-reference.md` | Bundle YAML shape, job resources, task types, triggers, clusters, variables, and dynamic value references. | During `databricks.yml` and `resources/*.yml` generation. |
| `references/schedule-trigger-mapping.md` | Airflow cron to Quartz conversion, Airflow presets, sensor-to-trigger mappings, `default_args`, and Jinja conversions. | During schedule, trigger, retry, notification, and parameter conversion. |
| `references/hadoop-migration-guide.md` | HDFS path migration, YARN config cleanup, Hive to Unity Catalog mapping, `spark-submit` detection, and Sqoop alternatives. | When DAGs include Hadoop, Hive, SSH, Bash, or on-prem Spark patterns. |
| `references/conversion-examples.md` | Full before/after examples. | When the agent needs an example shape for similar output. |

The most important design choice is that the mapping table is external to `SKILL.md`. That keeps the skill trigger and workflow lightweight while still allowing detailed coverage when the user provides a complex DAG.

## 7. Assets And Templates

The templates are not instructions; they are output starting points.

```text
assets/templates/databricks.yml.tmpl
assets/templates/job-resource.yml.tmpl
```

They establish the expected bundle shape:

```text
bundle:
  name: ...

include:
  - resources/*.yml

resources:
  jobs:
    ...
```

The agent can adapt them based on DAG content. For example, if the DAG has a file sensor, the generated job should use `trigger.file_arrival`; if it has a cron schedule and no event sensor, the job should use `schedule.quartz_cron_expression`.

## 8. Platform-Specific Instruction Files

Different agent products consume instructions differently, so the repo includes platform adapters.

| Platform | File | Behavior |
|---|---|---|
| Cursor | `SKILL.md` | Installed under `.cursor/skills/...` or `~/.cursor/skills/...`; Cursor can use the skill metadata and body. |
| Claude Code | `SKILL.md` | Installed under `.claude/skills/...` or `~/.claude/skills/...`. |
| Codex CLI | `AGENTS.md` | Appended into project or global `AGENTS.md`; Codex receives it as project instructions. |
| VS Code + Copilot | `copilot-instructions.md` | Appended into `.github/copilot-instructions.md`; uses embedded mappings because Copilot instructions should not rely on local skill reference loading. |

`AGENTS.md` is a condensed version of the skill. It exists because Codex project instructions are read from `AGENTS.md`, not necessarily from Cursor or Claude skill directories.

`copilot-instructions.md` is more self-contained. It embeds a mapping snapshot because Copilot instruction files are not guaranteed to load sibling reference files on demand.

## 9. Installer Design

`install.sh` makes installation repeatable across platforms.

The important implementation behaviors are:

- It clones or fast-forwards the skill repo into the correct platform directory.
- For Codex and Copilot, it writes into an existing instruction file using marker blocks:

```text
<!-- BEGIN airflow-to-dabs -->
...
<!-- END airflow-to-dabs -->
```

- It backs up files before modifying them.
- It replaces a stale marked block on reinstall instead of appending duplicate instructions.
- It validates marker ordering so malformed `BEGIN`/`END` blocks do not accidentally delete user content.

This is separate from the skill itself. The skill is the instruction package; the installer is just distribution and update plumbing.

## 10. How An Agent Interprets The Skill

From the agent's perspective, using the skill looks roughly like this:

```text
User asks to convert Airflow DAGs to Databricks.
  -> Skill metadata matches the request.
  -> Agent opens SKILL.md.
  -> Agent reads only the core workflow first.
  -> Agent inspects the DAG files.
  -> Agent reads operator-mapping.md for the operators it found.
  -> Agent reads schedule-trigger-mapping.md if schedules, sensors, or Jinja appear.
  -> Agent reads dab-schema-reference.md while generating YAML.
  -> Agent reads hadoop-migration-guide.md only if Hadoop/on-prem patterns appear.
  -> Agent generates databricks.yml, resources/*.yml, src/*, and MIGRATION_NOTES.md.
  -> Agent validates dependency graph, task types, parameters, compute, and bundle schema.
```

The skill does not behave like a slash command such as `/airflow-to-dabs`. The normal usage model is intent-based. The user says something like:

```text
Convert this Airflow DAG to a Databricks Asset Bundle.
```

or:

```text
Use the airflow-to-dabs skill on dags/customer_orders.py.
```

The skill then constrains how the agent should work.

## 11. Conversion Workflow In Detail

### 11.1 Phase 1: Parse

The agent extracts:

| Artifact | Examples |
|---|---|
| DAG metadata | `dag_id`, `schedule`, `start_date`, `catchup`, tags, params |
| Task inventory | `task_id`, operator class, callable, SQL, bash command, JSON payload |
| Dependencies | `>>`, `<<`, `set_upstream`, `set_downstream` |
| Runtime flags | XCom usage, Variables, Connections, dynamic task mapping, custom operators |
| Scheduling patterns | Cron, presets, datasets, timetables, sensors |

The skill asks the agent to present an operator mapping table before generation. This is not just user-friendly; it is a QA checkpoint. It forces the agent to make every conversion decision visible.

### 11.2 Phase 2: Map

The mapping system uses four tiers:

| Tier | Meaning | Example |
|---|---|---|
| Tier 1 | Direct mapping | `PythonOperator` -> `notebook_task`; `SQLExecuteQueryOperator` -> `sql_task` |
| Tier 2 | Semantic mapping | `BranchPythonOperator` -> `condition_task` when the branch is simple |
| Tier 3 | Sensor mapping | `S3KeySensor` -> job-level `trigger.file_arrival` |
| Tier 4 | Unsupported/manual | Custom operators, XCom-heavy logic, unknown side effects |

This tiering is important because not every Airflow operator has a literal Lakeflow equivalent. The skill makes the agent label certainty and document manual review items instead of silently inventing behavior.

### 11.3 Phase 3: Generate

The generated Databricks bundle has a stable shape:

```text
<bundle>/
  databricks.yml
  resources/
    <dag_id>_job.yml
  src/
    <task_id>.py
    <task_id>.sql
  MIGRATION_NOTES.md
```

The agent converts Airflow concepts into Lakeflow/DABs concepts:

| Airflow Concept | Databricks Bundle Concept |
|---|---|
| DAG | `resources.jobs.<job_key>` |
| Task dependency | `depends_on` |
| Cron schedule | `schedule.quartz_cron_expression` |
| File sensor | `trigger.file_arrival` |
| Table/SQL sensor | `trigger.table_update` |
| Airflow `params` | Lakeflow job `parameters` |
| `{{ ds }}` | `{{job.parameters.run_date}}` (a logical/partition date; this job is file-arrival triggered so it defaults to `{{job.start_time.iso_date}}` — a cron-scheduled job would default to `{{job.trigger.time.iso_date}}`; backfill overrides it with `{{backfill.iso_date}}`) |
| Python callable | `.py` Databricks notebook or Spark Python source |
| Inline SQL | `.sql` file referenced by `sql_task.file.path` |

### 11.4 Phase 4: Validate

The skill requires validation beyond "files exist":

| Check | Why It Matters |
|---|---|
| Dependency check | Prevents jobs from referencing missing `task_key`s. |
| Orphan check | Catches disconnected tasks after removing sensors or dummy operators. |
| Task type check | DABs tasks must have one task type, not several competing fields. |
| Compute check | Ensures tasks that need compute have valid compute or serverless assumptions. |
| Parameter check | Prevents unresolved `{{job.parameters.*}}` references. |
| Bundle validation | Confirms the generated YAML is accepted by the Databricks CLI. |

## 12. How To Use The Skill

### 12.1 Install

For Cursor:

```bash
git clone https://github.com/park-peter/airflow-to-dabs.git ~/.cursor/skills/airflow-to-dabs
```

For project-scoped Cursor usage:

```bash
git clone https://github.com/park-peter/airflow-to-dabs.git .cursor/skills/airflow-to-dabs
```

The repo also includes:

```bash
./install.sh
```

for guided installation across Cursor, Claude Code, Codex, and VS Code + Copilot.

### 12.2 Invoke

Natural language is enough:

```text
Convert dags/customer_orders_dag.py to a Databricks Asset Bundle using the airflow-to-dabs skill.
```

For better output, provide deployment constraints up front:

```text
Convert dags/customer_orders_dag.py to a DABs project.
Use Unity Catalog catalog main, schema commerce, SQL warehouse variable warehouse_id,
and make the output ready for databricks bundle validate.
```

For multiple DAGs:

```text
Convert all DAGs in ./dags into one Databricks bundle with one Lakeflow Job per DAG.
Keep cross-DAG TriggerDagRunOperator dependencies inside the same bundle where possible.
```

### 12.3 Review The Output

After generation, inspect:

```text
MIGRATION_NOTES.md
resources/*.yml
src/*.py
src/*.sql
```

Then run:

```bash
databricks bundle validate -t dev
```

If workspace auth or required variables are not available locally, run the closest offline checks available and document that limitation.

## 13. Demo Example: Customer Orders

The example in this repo demonstrates the skill end to end.

### 13.1 Source DAG

The source file is:

```text
examples/customer-orders/airflow/customer_orders_dag.py
```

It includes:

- `S3KeySensor`
- `PythonOperator`
- `SQLExecuteQueryOperator`
- `BranchPythonOperator`
- `EmptyOperator`
- Airflow Jinja variables such as `{{ ds }}` and `{{ params.catalog }}`
- Airflow defaults for retries, retry delay, email notifications, timeout, and max active runs

### 13.2 Generated Bundle

The output bundle is:

```text
examples/customer-orders/customer_orders_bundle/
```

Important files:

| File | Purpose |
|---|---|
| `databricks.yml` | Top-level bundle config, variables, dev/prod targets. |
| `resources/customer_orders_job.yml` | Lakeflow Job definition with trigger, parameters, tasks, dependencies, and SQL warehouse reference. |
| `src/ingest_bronze.py` | Extracted ingestion notebook using Auto Loader and file events. |
| `src/transform_silver.py` | Extracted transform notebook. |
| `src/dq_order_totals.sql` | Extracted SQL data quality task. |
| `src/full_validation.py` | Extracted full validation notebook. |
| `src/publish_gold.py` | Extracted publish notebook. |
| `MIGRATION_NOTES.md` | Human-review notes and deployment prerequisites. |

### 13.3 Mapping Table

| Airflow Task ID | Airflow Operator | Lakeflow/DABs Output | Tier |
|---|---|---|---|
| `wait_for_orders` | `S3KeySensor` | `trigger.file_arrival` | 3 |
| `ingest_bronze` | `PythonOperator` | `notebook_task` | 1 |
| `transform_silver` | `PythonOperator` | `notebook_task` | 1 |
| `dq_order_totals` | `SQLExecuteQueryOperator` | `sql_task` | 1 |
| `choose_validation` | `BranchPythonOperator` | `condition_task` | 2 |
| `full_validation` | `PythonOperator` | `notebook_task` | 1 |
| `skip_full_validation` | `EmptyOperator` | Removed and rewired | 2 |
| `publish_gold` | `PythonOperator` | `notebook_task` | 1 |

### 13.4 Generated Job Shape

The generated Lakeflow Job uses:

```yaml
trigger:
  pause_status: UNPAUSED
  file_arrival:
    url: ${var.landing_path}
    min_time_between_triggers_seconds: 300
    wait_after_last_change_seconds: 60
```

The branch becomes:

```yaml
- task_key: choose_validation
  condition_task:
    left: "{{job.parameters.run_full_validation}}"
    op: EQUAL_TO
    right: "true"
```

The false branch is rewired directly to `publish_gold`:

```yaml
- task_key: publish_gold
  depends_on:
    - task_key: full_validation
    - task_key: choose_validation
      outcome: "false"
  run_if: NONE_FAILED
```

`trigger_rule="none_failed_min_one_success"` has no exact Lakeflow equivalent. `NONE_FAILED`
preserves the "no upstream failed" guarantee and drops the "at least one succeeded" clause, so
`publish_gold` also runs when every upstream skipped. `AT_LEAST_ONE_SUCCESS` keeps the opposite half
and would allow the task to run while another upstream had failed. Full rule table:
`references/schedule-trigger-mapping.md`.

### 13.5 Validation Performed

The demo bundle was validated with Databricks CLI:

```bash
databricks bundle validate -t dev --var warehouse_id=0000000000000000
```

Result:

```text
Validation OK!
```

Additional checks performed:

```text
YAML parse: passed
Dependency references: passed
Exactly-one task type per task: passed
Job parameter references: passed
Python syntax compile: passed
```

## 14. Extension Pattern

To extend the skill, add knowledge at the narrowest useful layer:

| Change Needed | Best Place |
|---|---|
| New operator mapping | `references/operator-mapping.md` |
| New DABs schema/task field | `references/dab-schema-reference.md` |
| New schedule or trigger rule | `references/schedule-trigger-mapping.md` |
| New migration category for Hadoop/on-prem | `references/hadoop-migration-guide.md` |
| New output skeleton | `assets/templates/` |
| New top-level workflow rule | `SKILL.md` |
| New platform installation behavior | `install.sh` and `README.md` |

The rule of thumb is: keep `SKILL.md` small and procedural, put detailed domain knowledge in references, and put reusable output shapes in assets.

## 15. Key Takeaways

This skill works because it separates responsibilities:

- `SKILL.md` tells the agent how to think and what workflow to follow.
- `references/` gives the agent detailed domain knowledge on demand.
- `assets/` gives the agent reusable output skeletons.
- `AGENTS.md` and `copilot-instructions.md` adapt the same intent for platforms with different instruction-loading models.
- The generated example proves the workflow can produce a Databricks bundle that passes `databricks bundle validate`.

The result is not a black-box converter. It is an agent-guided migration workflow with explicit mapping decisions, generated code, deployable bundle structure, and migration notes for the places where human judgment still matters.
