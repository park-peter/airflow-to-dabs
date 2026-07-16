# Airflow Schedule and Trigger Mapping Reference

Maps Airflow scheduling mechanisms (cron expressions, presets, sensors) to Databricks Asset Bundles schedule and trigger configurations.

---

## Cron Expression Conversion

Airflow uses **5-field Unix cron** (minute, hour, day-of-month, month, day-of-week).
DABs uses **6-field Quartz cron** (second, minute, hour, day-of-month, month, day-of-week).

### Key Differences

| Feature | Airflow (Unix cron) | DABs (Quartz cron) |
|---|---|---|
| Fields | 5: `MIN HOUR DOM MON DOW` | 6-7: `SEC MIN HOUR DOM MON DOW [YEAR]` |
| Seconds | Not supported | First field, usually `0` |
| Day-of-week | `0-7` (Sun=0 or 7) or `SUN-SAT` | `1-7` (Sun=1) or `SUN-SAT` |
| Mutual exclusion | Both DOM and DOW can be `*` | Use `?` for one when the other is set |
| Timezone | `start_date` timezone or `schedule_interval` | `timezone_id` field (IANA format) |

### Conversion Rule

Prepend `0` for seconds. Replace `*` in day-of-week with `?` when day-of-month is specified (and vice versa).

```
Airflow:  MIN HOUR DOM MON DOW
DABs:     0   MIN  HOUR DOM MON DOW
```

If both DOM and DOW are `*` in Airflow, set DOW to `?` in Quartz:
`* * * * *` -> `0 * * * * ?`

If DOW is numeric, shift by +1 for Quartz and normalize Sunday:
- Airflow `0` or `7` (Sunday) -> Quartz `1`
- Airflow `1-6` (Mon-Sat) -> Quartz `2-7`

Prefer named days (`MON`..`SUN`) to avoid off-by-one conversion bugs.

---

## Airflow Preset to Quartz Cron

| Airflow Preset | Airflow Cron | Quartz Cron | Description |
|---|---|---|---|
| `@once` | N/A | *(no schedule, manual trigger)* | Run once. Remove schedule, trigger manually. |
| `@continuous` | N/A | `continuous.pause_status: UNPAUSED` | Continuous execution mode. |
| `@hourly` | `0 * * * *` | `0 0 * * * ?` | Top of every hour |
| `@daily` / `@midnight` | `0 0 * * *` | `0 0 0 * * ?` | Midnight daily |
| `@weekly` | `0 0 * * 0` | `0 0 0 ? * 1` | Midnight Sunday |
| `@monthly` | `0 0 1 * *` | `0 0 0 1 * ?` | Midnight first day of month |
| `@yearly` / `@annually` | `0 0 1 1 *` | `0 0 0 1 1 ?` | Midnight Jan 1 |
| `None` | N/A | *(no schedule)* | Manual trigger only |

---

## Common Cron Conversions

| Description | Airflow | DABs Quartz |
|---|---|---|
| Every 15 minutes | `*/15 * * * *` | `0 */15 * * * ?` |
| Every 6 hours | `0 */6 * * *` | `0 0 */6 * * ?` |
| 8 AM daily | `0 8 * * *` | `0 0 8 * * ?` |
| 8 AM weekdays | `0 8 * * 1-5` | `0 0 8 ? * 2-6` |
| 6 PM last day of month | `0 18 28-31 * *` | `0 0 18 L * ?` |
| Every Monday 9 AM | `0 9 * * 1` | `0 0 9 ? * 2` |

> **Day-of-week note:** Airflow `1` = Monday, Quartz `2` = Monday. Also map Airflow `7` (Sunday) to Quartz `1`. Using named days avoids numeric ambiguity.

---

## DABs Schedule YAML

```yaml
schedule:
  quartz_cron_expression: "0 0 8 * * ?"
  timezone_id: "America/New_York"
  pause_status: UNPAUSED                   # PAUSED or UNPAUSED
```

### Timezone Mapping

Airflow timezone comes from `default_timezone` in `airflow.cfg` or the DAG's `start_date` timezone. Map to IANA timezone ID for DABs.

| Common Airflow Value | DABs `timezone_id` |
|---|---|
| `UTC` | `UTC` |
| `US/Eastern` | `America/New_York` |
| `US/Pacific` | `America/Los_Angeles` |
| `US/Central` | `America/Chicago` |
| `Europe/London` | `Europe/London` |
| `Asia/Tokyo` | `Asia/Tokyo` |

---

## Sensor to Trigger Mapping

Airflow sensors that block execution until a condition is met map to DABs job-level triggers or are absorbed into task dependencies.

### File-Based Sensors -> `trigger.file_arrival`

| Airflow Sensor | Trigger Config |
|---|---|
| `S3KeySensor` | `trigger.file_arrival` with `url: s3://bucket/prefix/` |
| `GCSObjectExistenceSensor` | `trigger.file_arrival` with `url: gs://bucket/prefix/` |
| `FileSensor` | `trigger.file_arrival` with `url:` pointing to UC volume |

**Airflow:**

```python
wait_for_data = S3KeySensor(
    task_id="wait_for_data",
    bucket_name="landing-zone",
    bucket_key="data/{{ ds }}/*.parquet",
    poke_interval=60,
    timeout=3600,
)
```

**DABs:**

```yaml
trigger:
  file_arrival:
    url: s3://landing-zone/data/
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 60
```

**Key differences:**
- Airflow sensors are task-level (block one task). DABs triggers are job-level (start the whole job).
- Move the sensor to the job trigger. Downstream tasks that depended on the sensor now just run as the first task(s) in the job.

---

### Table-Based Sensors -> `trigger.table_update`

| Airflow Sensor | Trigger Config |
|---|---|
| `ExternalTaskSensor` (if upstream writes to a table) | `trigger.table_update` monitoring the output table |
| `SqlSensor` (if checking table freshness/existence) | `trigger.table_update` with `condition: ANY_UPDATED` |

**Airflow:**

```python
wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_etl",
    external_task_id="write_silver_table",
    timeout=3600,
)
```

**DABs:**

```yaml
trigger:
  table_update:
    condition: ANY_UPDATED
    table_names:
      - "main.silver.transactions"
    min_time_between_triggers_seconds: 300
    wait_after_last_change_seconds: 60
```

---

### Dependency-Based Sensors -> `depends_on` or `run_job_task`

| Airflow Sensor | DABs Equivalent |
|---|---|
| `ExternalTaskSensor` (same job/bundle) | `depends_on` with `task_key` |
| `ExternalTaskSensor` (cross-job) | Upstream job triggers downstream via `run_job_task` |

---

### Time-Based Sensors -> Schedule Adjustment

| Airflow Sensor | DABs Equivalent |
|---|---|
| `TimeSensor` / `TimeDeltaSensor` at DAG start | Adjust `schedule.quartz_cron_expression` to the target time |
| `TimeSensor` / `TimeDeltaSensor` mid-pipeline | Flag in MIGRATION_NOTES.md -- no direct equivalent |
| `DayOfWeekSensor` | Adjust cron to run only on specified days |

---

## Airflow Timetable and Dataset Scheduling

Airflow DAGs can use non-cron schedule APIs that are not 1:1 with a Quartz cron.

| Airflow Scheduling Pattern | DABs Mapping |
|---|---|
| `schedule=[Dataset(...)]` / dataset event scheduling | Prefer `trigger.table_update` on the upstream Unity Catalog table(s). |
| Custom `Timetable` subclass | Flag for manual review and map to `schedule` or `trigger` based on business intent. |
| `@continuous` | Use job-level `continuous` (not periodic trigger). |

If a dataset or timetable schedule cannot be deterministically mapped, add a required action item in `MIGRATION_NOTES.md`.

---

## Airflow `default_args` Mapping

Common `default_args` fields and their DABs equivalents:

| Airflow `default_args` | DABs Equivalent |
|---|---|
| `owner` | *(no direct mapping -- do not auto-map identity; document intended run identity in MIGRATION_NOTES.md)* |
| `retries` | `max_retries` on task |
| `retry_delay` | `min_retry_interval_millis` on task |
| `email` | `email_notifications.on_failure` |
| `email_on_failure` | `email_notifications.on_failure` |
| `email_on_retry` | *(no direct equivalent, note in migration notes)* |
| `depends_on_past` | *(no direct equivalent, note in migration notes)* |
| `start_date` | *(not needed -- DABs jobs start when deployed)* |
| `end_date` | *(no direct equivalent -- pause the schedule manually)* |
| `execution_timeout` | `timeout_seconds` on task |
| `sla` | *(no direct equivalent -- use monitoring/alerts)* |
| `catchup` | *(no direct equivalent, note in migration notes)* |

---

## Jinja Template Variable Conversion

Airflow Jinja variables used in operators/SQL need conversion to DABs dynamic value references.

> Dynamic value references belong in PARAMETER values (`base_parameters`, `sql_task.parameters`, task `parameters` lists) — never inline in SQL files. SQL files use `:name` parameter markers whose values are supplied via `sql_task.parameters` (see `references/dab-schema-reference.md`).

| Airflow Jinja | DABs Equivalent | Notes |
|---|---|---|
| `{{ ds }}` | `{{job.parameters.run_date}}` | Define `run_date` as job parameter with default `{{job.start_time.iso_date}}` |
| `{{ ds_nodash }}` | *(compute in notebook)* | No direct equivalent. Derive from `run_date` in code. |
| `{{ execution_date }}` | `{{job.parameters.run_date}}` | Same as `ds` for most use cases |
| `{{ prev_ds }}` | *(compute in notebook)* | No direct equivalent. Calculate in code. |
| `{{ next_ds }}` | *(compute in notebook)* | No direct equivalent. Calculate in code. |
| `{{ params.x }}` | `{{job.parameters.x}}` | Define as job parameter |
| `{{ var.value.x }}` | `${var.x}` | Define as bundle variable |
| `{{ task_instance.xcom_pull(...) }}` | `{{tasks.<key>.values.<name>}}` | Use `dbutils.jobs.taskValues.set/get` |
| `{{ run_id }}` | `{{job.run_id}}` | Direct mapping |
| `{{ dag.dag_id }}` | `${bundle.name}` or hardcode | Bundle name is typically the DAG equivalent |
| `{{ macros.ds_add(ds, -1) }}` | *(compute in notebook)* | No macro support. Calculate in Python/SQL. |
