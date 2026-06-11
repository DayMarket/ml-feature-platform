# Design: Daily average linear_score per query / SKU group (silver)

Date: 2026-06-11
Status: Approved (brainstorming)

## Goal

Build a feature: average `linear_score` (and `normalized_linear_score`) per
search query and SKU group per day, as a reusable silver pre-aggregate.

## Source contract

- Spark table: `iceberg.silver.ranking_analytics_events`
  (Trino: `"dwh-iceberg".silver.ranking_analytics_events`).
- DE-owned upstream table; its DQ contract lives outside this repo.
- Relevant columns:
  - `search_query` (varchar) — the raw search query.
  - `ranking_candidates` (array(bigint)) — candidate **sku_group_id** values
    (already SKU-group grain; no `silver.sku` mapping needed).
  - `external_features` (varchar JSON) — per-candidate arrays. Keys observed:
    `jamspell_score`, `cpo_adv_percents`, `bid_amounts`, `cpo_percent`,
    `dssm_score`, `linear_score`, `normalized_linear_score`, `prefix_len`.
  - `model_name` (varchar) — ranking model id.
  - `fired_at` (timestamp(6) with time zone) — event time.
- Positional alignment verified on production: for the last day,
  `cardinality(ranking_candidates) == cardinality(linear_score array)` and
  `== cardinality(normalized_linear_score array)` for 100% of rows under the
  target model filter (2.28-2.33M rows checked).
- `linear_score` is only populated for search ranking models. Among models with
  full coverage: `search_unified_model_v1`, `search_unified_model_v6`,
  `search_unified_model_v8`. Filter chosen: `model_name LIKE 'search_unified_model_v%'`.

## Output table

- Table: `iceberg.silver.feature_platform_query_skg_daily_linear_score`.
- Path: `layers/silver/query_skg_daily_linear_score/v1`.
- Primary key: `date, query, sku_group_id`.
- Partition column: `date`.

| column | type | meaning |
|---|---|---|
| `date` | date | partition date = Airflow `{{ ds }}` |
| `query` | string | normalized search query |
| `sku_group_id` | bigint | candidate from `ranking_candidates` |
| `avg_linear_score` | double | average `linear_score` over the day |
| `avg_normalized_linear_score` | double | average `normalized_linear_score` over the day |
| `observations` | bigint | number of candidate positions averaged (DQ/transparency) |

## Transformation logic

1. Filter `fired_at` in `[{{ ds }} 00:00:00 UTC, {{ next_ds }} 00:00:00 UTC)`.
2. Filter `model_name LIKE 'search_unified_model_v%'`.
3. Parse arrays from `external_features` JSON: `linear_score`, `normalized_linear_score`.
4. `arrays_zip(ranking_candidates, linear, normalized)`, then `explode`.
5. Normalize query like the existing query x SKU-group features:
   `lower` -> replace `ё` with `е` -> collapse whitespace -> `trim` -> drop empty.
6. `GROUP BY date, query, sku_group_id`: avg(linear), avg(normalized), count(linear).
7. Spark null/avg semantics: null elements ignored; pairs with no valid scores not emitted.

## Ownership and ops

- `table.meta.team`, `dag.team`, `alerts.team`: `team:search`.
- Alert severity: `P3`. On-call webhook: `oncall_webhook_search`.
- Schedule: `0 1 * * *`. No upstream `ExternalTaskSensor` (rely on schedule).
- DQ: auto-generated from primary key. No downstream ranking upload (silver).
- Deployment: shared default Spark image + `git-sync`. No custom image.
  Day boundaries pinned via `spark.sql.session.timeZone: "UTC"`.

## Out of scope

- Window aggregations (1/3/7/... day) — left for a later gold table if needed.
- Ranking-service upload.
- Other `external_features` keys (dssm_score, cpo_percent, etc.).
