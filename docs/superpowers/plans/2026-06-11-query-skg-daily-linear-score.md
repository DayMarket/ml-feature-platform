# Daily Query/SKU-Group linear_score (silver) Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Status:** IMPLEMENTED on branch `new_feature_test`. Full file contents live in
`layers/silver/query_skg_daily_linear_score/v1/`; this plan records the task
breakdown and verification used.

**Goal:** Build silver table `iceberg.silver.feature_platform_query_skg_daily_linear_score`
holding daily average `linear_score` and `normalized_linear_score` per normalized
search query and SKU group.

**Architecture:** New PySpark layer under `layers/silver/query_skg_daily_linear_score/v1`,
mirroring `query_skg_daily_conversions_legacy`. Reads `iceberg.silver.ranking_analytics_events`,
explodes the per-candidate score arrays (positionally aligned with `ranking_candidates`),
filters `model_name LIKE 'search_unified_model_v%'`, normalizes the query, aggregates by
`(date, query, sku_group_id)`. Daily schedule, no sensor, default Spark image + git-sync,
`spark.sql.session.timeZone=UTC`.

**Spec:** `docs/superpowers/specs/2026-06-11-query-skg-daily-linear-score-design.md`

**Source contract (verified on prod 2026-06-11):** `ranking_candidates` are already
`sku_group_id`; `linear_score`/`normalized_linear_score` are arrays inside the
`external_features` JSON, positionally aligned with candidates (cardinalities match
100% under the model filter); `fired_at` is `timestamp with tz`, day boundaries UTC.

## Tasks

- [x] **Task 1 — Scaffold + verbatim copies:** create the layer dirs; copy
  `config/factory.py`, `config/resources.yaml`, `job/__init__.py`,
  `job/arguments.py`, `job/entities.py`, `migrations/__init__.py` from the analog
  unchanged. Verify: files exist.
- [x] **Task 2 — Migration DDL** (`migrations/create_table.sql`): idempotent
  `CREATE TABLE IF NOT EXISTS`, `PARTITIONED BY (date)`, columns
  `date DATE, query STRING, sku_group_id BIGINT, avg_linear_score DOUBLE,
  avg_normalized_linear_score DOUBLE, observations BIGINT`, all commented.
  Verify: contains `IF NOT EXISTS`, no `DROP/DELETE/TRUNCATE`.
- [x] **Task 3 — config.yaml:** key `query_skg_daily_linear_score`, catalog
  `iceberg`, schema `silver`, name `feature_platform_query_skg_daily_linear_score`,
  primary_key `date,query,sku_group_id`, meta.team `team:search`, dag.team `search`,
  alerts team `search`/severity `P3`/webhook `oncall_webhook_search`.
  Verify: `ci_test/test_script.py` lists it as a valid table config.
- [x] **Task 4 — SparkApplication template**
  (`config/fetch_silver_query_skg_daily_linear_score.yaml`): copy of the analog with
  exactly three changes — `metadata.name`, `mainApplicationFile` (new entrypoint),
  and an added `spark.sql.session.timeZone: "UTC"` under `sparkConf`.
  Verify: `diff` vs analog shows only those three lines.
- [x] **Task 5 — Transformation job**
  (`job/getting_query_skg_daily_linear_score.py`): reads
  `iceberg.silver.ranking_analytics_events`; filters `fired_at` in
  `[partition_start, partition_end)` and `model_name` startswith
  `search_unified_model_v`; extracts `$.linear_score` / `$.normalized_linear_score`
  via `get_json_object` + `from_json(array<double>)`; `arrays_zip` with
  `ranking_candidates` + `explode`; normalizes query
  (`lower` -> `ё→е` -> `\s+`-> single space -> `trim`, drop empty); groups by
  `date,query,sku_group_id` into `avg_linear_score`, `avg_normalized_linear_score`,
  `observations=count(linear_score)`; writes via `overwritePartitions()`; creates
  the table from the migration if missing. Verify: `py_compile`; Trino dry-run of
  the equivalent SQL (validated during planning — `normalized` in [0,1],
  `observations > 0`).
- [x] **Task 6 — Entrypoint**
  (`entrypoints/get_query_skg_daily_linear_score.py`): SparkSession with appName
  `getting-query-skg-daily-linear-score`, `parse_arguments`, `run`, `spark.stop()`.
  Verify: `py_compile`.
- [x] **Task 7 — DAG** (`dag.py`): dag_id
  `feature_platform_query_skg_daily_linear_score_silver_dag`, schedule `0 1 * * *`,
  no `ExternalTaskSensor`, task_id `getting_query_skg_daily_linear_score`, settings
  from `config.factory.get_dag_settings()`. Verify: `py_compile`.
- [x] **Task 8 — README** (Russian): purpose, source, UTC boundaries, model filter,
  candidates-are-sku_group_id note, JSON array extraction, normalization,
  aggregation, no-sensor / no-upload notes.
- [x] **Task 9 — Repo CI + commit:**
  `python3 ci_test/test_script.py`, `ci_test/test_sync_dbt_sources.py`,
  `ci_test/test_sync_iceberg_maintenance.py`,
  `scripts/validate_ranking_upload_configs.py`, `git diff --check` — all pass.
  Then commit on `new_feature_test`.

## Self-Review

- Spec coverage: every spec section maps to a task (table/PK -> Task 2/3; source +
  model filter + UTC -> Task 4/5; explode of aligned arrays -> Task 5; normalization
  -> Task 5; three output columns -> Task 2/5; ownership -> Task 3; schedule/no-sensor
  -> Task 7; default image + git-sync -> Task 4).
- No placeholders; type/column names (`avg_linear_score`,
  `avg_normalized_linear_score`, `observations`) consistent across migration, job,
  README, config PK.

## Post-merge follow-ups (after master merge, not now)

- Merge generated dbt-trino DQ PR: https://github.com/DayMarket/dbt-trino/pulls
- Merge generated Iceberg maintenance PR: https://github.com/DayMarket/pyspark-etl/pulls
