# Repository Context

This file is intended to be read by Codex at the start of each new session in this repository.
Keep it concise, factual, and updated when repository structure or pipeline behavior changes.

## Purpose

`ml-feature-platform` contains Airflow-managed PySpark feature pipelines for search-related SKU group features.
The repository is organized by data layer under `layers/` and currently contains production code for:

- `silver/sku_group_install/v1`: daily pre-aggregated search/category interaction statistics by install, SKU group, and query/category key.
- `silver/sku_group_id_prices/v1`: daily SKU group end-of-day price aggregates.
- `silver/sku_group_orders/v1`: daily SKU group order statistics.
- `silver/sku_group_query_search_orders/v1`: daily search order statistics by query and SKU group.
- `gold/sku_group_query_atc_features/v1`: daily query and SKU group ATC conversion features built from the silver table.
- `gold/feedback_product_id/v1`: daily all-time feedback and rating aggregates by product.
- `gold/feedback_sku_group_id/v1`: daily all-time feedback and rating aggregates by SKU group.

The owning team in configs and DAG metadata is `team:search`.

## Top-Level Structure

- `.drone.yaml`: Drone CI pipelines for tests, dbt source sync, Airflow submodule sync, and Docker image publishing.
- `.github/CODEOWNERS`: repository code owners.
- `ci_config.yaml`: dbt source sync settings.
- `ci_test/test_script.py`: lightweight CI validation for required files, table configs, and migration CREATE TABLE statements.
- `scripts/sync_dbt_sources.py`: CI helper that discovers layer `config.yaml` table definitions and publishes missing dbt source entries to the dbt repository.
- `layers/`: versioned feature pipelines grouped by data layer.
- `docs/`: currently empty.

There are also empty/unimplemented directories such as `layers/gold/sku_group_query_orders/v1`.

## Layer Layout Convention

Each implemented pipeline follows this shape:

- `dag.py`: Airflow DAG definition. Uses `SparkKubernetesOperator` and `config.factory.get_deployment`.
- `config.yaml`: table metadata used by the DAG factory, CI, and dbt source sync.
- `config/resources.yaml`: JSON-formatted resource values for Spark driver/executors and infrastructure placeholders.
- `config/fetch_*.yaml`: SparkApplication template with placeholders filled at DAG parse/runtime.
- `config/factory.py`: fills SparkApplication placeholders using `config.yaml`, `resources.yaml`, random suffixes, Airflow connections, and Airflow date macros.
- `job/arguments.py`: parses `--partition_start`, `--partition_end`, and `--table_name`.
- `job/entities.py`: dataclass for runtime arguments.
- `job/getting_*.py`: main PySpark transformation and write logic.
- `entrypoints/*.py`: executable Spark entrypoint that creates `SparkSession`, parses args, calls `job.run`, and stops Spark.
- `migrations/create_table.sql`: Iceberg table DDL used when the target table does not exist.
- `Dockerfile`: builds the wheel, installs Spark 3.4.1 / Java 11, copies entrypoints, and prepares the Spark-on-Kubernetes image.
- `entrypoint.sh`: Spark container entrypoint script.
- `pyproject.toml`: Poetry package metadata. Python is pinned to `3.9.13`, PySpark to `3.4.1`.

## Silver Pipeline

Path: `layers/silver/sku_group_install/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_install_silver_stats_dag`
- Schedule: `0 1 * * *`
- Start date: `2026-02-01`
- Tags include `spark`, `feature-platform`, `team::search`, `silver`.
- Task id: `getting_sku_group_query_install_stats`
- Runs SparkApplication template `fetch_silver_sku_group_statistics.yaml`.
- ExternalTaskSensor dependencies for clickstream DQ are present but commented out.

Target table config:

- Catalog/schema/table: `iceberg.silver.feature_platform_search_sku_group_id_install_query`
- Config primary key: `sku_group_id,install_id,query`
- Migration columns include `install_id`, `sku_group_id`, `section`, `uniqs`, `sum_atc`, `sum_clicks`, `sum_impressions`, `date`.
- The transformation writes columns `install_id`, `sku_group_id`, `space`, `uniqs`, `sum_atc`, `sum_clicks`, `sum_impressions`, `date`. Note that migration uses `section`, while code writes `space`; check this carefully before changing schema or writes.
- Partition: `date`.

Transformation summary:

- Reads `iceberg.silver_b2c_clickstream.events` for the partition window and `iceberg.silver.sku` for SKU-to-SKU-group fallback.
- Filters clickstream events to `SEARCH_RESULTS`, `PRODUCT_IMPRESSION`, `PRODUCT_VIEW`, and `ADD_TO_CART`.
- Normalizes query quotes and trims query text.
- Builds interaction aggregates for search results and category contexts.
- Counts:
  - impressions from `PRODUCT_IMPRESSION`;
  - clicks from `PRODUCT_VIEW` immediately following a product impression in the same `session_id`/`product_id` window;
  - ATC from `ADD_TO_CART` immediately following a product impression in the same `session_id`/`product_id` window.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-silver-search-sku-group-install-stats-*`
- Published image repo: `cr.yandex/de-common/pyspark-silver-sku-group-query-statistics`
- Current SparkApplication image in config: `cr.yandex/de-common/pyspark-silver-sku-group-query-statistics:spark-silver-search-sku-group-install-stats-v0.3.3`

## Silver SKU Group Prices Pipeline

Path: `layers/silver/sku_group_id_prices/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_id_prices_silver_dag`
- Schedule: `0 1 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `silver`, `prices`.
- Sensor task id: `wait_for_sku_eod`
- Sensor waits for DAG `dbt.models.dwh_trino.sku_eod` with `execution_delta=timedelta(hours=1)` because the dbt DAG runs at `0 0 * * *`.
- Spark task id: `getting_sku_group_id_prices`
- Runs SparkApplication template `fetch_silver_sku_group_id_prices.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.silver.feature_platform_sku_group_id_prices`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads `iceberg.silver.sku_eod` for `dt = {{ ds }}`.
- Joins SKU metadata from `iceberg.silver.sku` by `sku_id`.
- Aggregates by `sku_group_id`.
- Produces average and median end-of-day sell price and full price.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-silver-sku-group-id-prices-*`
- Published image repo: `cr.yandex/de-common/pyspark-silver-sku-group-id-prices`
- Current SparkApplication image in config: `cr.yandex/de-common/pyspark-silver-sku-group-id-prices:spark-silver-sku-group-id-prices-v0.1.0`

## Silver SKU Group Orders Pipeline

Path: `layers/silver/sku_group_orders/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_orders_silver_dag`
- Schedule: `0 1 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `silver`, `orders`.
- Spark task id: `getting_sku_group_orders`
- Runs SparkApplication template `fetch_silver_sku_group_orders.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.silver.feature_platform_sku_group_orders`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads order items from `iceberg.silver.order_items`.
- Joins SKU metadata from `iceberg.silver.sku` by `sku_id`.
- Uses Airflow `{{ ds }} 00:00:00` as `target_date`, `{{ next_ds }} 00:00:00` as `end_date`, and `target_date - INTERVAL 20 DAY` for order lookback.
- Aggregates generated, completed, and returned order metrics by `date` and `sku_group_id`.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-silver-sku-group-orders-*`
- Published image repo: `cr.yandex/de-common/pyspark-silver-sku-group-orders`
- Current SparkApplication image in config: `cr.yandex/de-common/pyspark-silver-sku-group-orders:spark-silver-sku-group-orders-v0.1.0`

## Silver Search Orders Pipeline

Path: `layers/silver/sku_group_query_search_orders/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_query_search_orders_silver_dag`
- Schedule: `0 1 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `silver`, `orders`.
- Spark task id: `getting_sku_group_query_search_orders`
- Runs SparkApplication template `fetch_silver_sku_group_query_search_orders.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.silver.feature_platform_sku_group_query_search_orders`
- Primary key: `date,query,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads search attribution from `iceberg.silver.order_items_attribution`.
- Reads order items from `iceberg.silver.order_items`.
- Joins SKU metadata from `iceberg.silver.sku` by `sku_id`.
- Uses Airflow `{{ ds }} 00:00:00` as `target_date`, `{{ next_ds }} 00:00:00` as `end_date`, and `target_date - INTERVAL 20 DAYS` for attribution/order lookback.
- Filters search attribution to `SHOP_SEARCH_RESULTS`, `COLLECTION_SEARCH_RESULTS`, `SEARCH`, and `SEARCH_RESULTS`.
- Aggregates generated, completed, and returned order metrics by `date`, `query`, and `sku_group_id`.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-silver-sku-group-query-search-orders-*`
- Published image repo: `cr.yandex/de-common/pyspark-silver-sku-group-query-search-orders`
- Current SparkApplication image in config: `cr.yandex/de-common/pyspark-silver-sku-group-query-search-orders:spark-silver-sku-group-query-search-orders-v0.1.0`

Resources note:

- Current implemented Spark layers use the same profile: driver `1 core / 10g`, executors `5 x 8 cores / 16g`.
- Feedback and daily price pipelines use a reduced profile: driver `1 core / 4g`, executors `3 x 4 cores / 8g`.
- The larger driver `1 core / 10g`, executors `5 x 8 cores / 16g` profile is kept for order/search jobs with joins over order facts and lookback windows.
- Resource-only changes in `config/resources.yaml` do not require rebuilding Spark images; code, entrypoint, dependency, or wheel changes do.

## Gold Pipeline

Path: `layers/gold/sku_group_query_atc_features/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_query_atc_features_gold_dag`
- Schedule: `0 2 * * *`
- Start date: `2026-05-18`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`.
- Sensor task id: `wait_for_silver_sku_group_install_stats`
- Sensor waits for silver DAG task `getting_sku_group_query_install_stats` with `execution_delta=timedelta(hours=1)`.
- Spark task id: `getting_sku_group_query_atc_features`
- Runs SparkApplication template `fetch_gold_sku_group_query_atc_features.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_features`
- Primary key: `date,sku_group_id,query_text`
- Partition: `date`.

Transformation summary:

- Reads `iceberg.silver.feature_platform_search_sku_group_id_install_query`.
- Uses only `space = 'SEARCH_RESULTS'`.
- Aggregates by `sku_group_id` and normalized `query_text`.
- Builds ATC and impression windows for 1, 3, 7, 14, 21, 30, 60, and 90 days.
- Produces:
  - `query_skg_conv_imp2atc_*`: ATC/impression conversion features.
  - `share_of_atc_*`: SKU group share of ATC inside each query.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Important implementation note:

- In the current SQL, `query_skg_conv_imp2atc_90` divides `atc_90_day` by `impressions_60_day`. This may be intentional or a bug; verify before touching related logic.

## Feedback Gold Pipelines

Paths:

- `layers/gold/feedback_product_id/v1`
- `layers/gold/feedback_sku_group_id/v1`

Airflow DAGs:

- Product DAG id: `feature_platform_product_feedback_base_stats_gold_dag`
- SKU group DAG id: `feature_platform_sku_group_feedback_base_stats_gold_dag`
- Product schedule: `0 3 * * *`
- SKU group schedule: `10 3 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `feedback`.

Target table configs:

- Product table: `iceberg.gold.feature_platform_product_feedback_base_stats`
- Product primary key: `date,product_id`
- SKU group table: `iceberg.gold.feature_platform_sku_group_feedback_base_stats`
- SKU group primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads published feedback from `iceberg.silver_bxappdb2_foodback.public_feedback`.
- Joins SKU metadata from `iceberg.silver.sku` by `sku_id`.
- Builds daily snapshots for Airflow `{{ ds }}`.
- Uses all feedback history with `date_published < {{ ds }}` so the snapshot reflects the state up to the previous day.
- Uses only `status = 'PUBLISHED'`.
- Product pipeline groups by `f.product_id`; SKU group pipeline groups by `s.sku_group_id`.
- Produces average rating, good/bad review counts, rating bucket counts, text review count, and review ratio features.

Important implementation note:

- Trino source name `"dwh-iceberg".silver_bxappdb2_foodback.public_feedback` maps to Spark source `iceberg.silver_bxappdb2_foodback.public_feedback` in these jobs.

Docker/CI images:

- Product Drone tag trigger: `refs/tags/spark-gold-product-feedback-base-stats-*`
- Product image repo: `cr.yandex/de-common/pyspark-gold-product-feedback-base-stats`
- Current Product SparkApplication image: `cr.yandex/de-common/pyspark-gold-product-feedback-base-stats:spark-gold-product-feedback-base-stats-v0.1.0`
- SKU group Drone tag trigger: `refs/tags/spark-gold-sku-group-feedback-base-stats-*`
- SKU group image repo: `cr.yandex/de-common/pyspark-gold-sku-group-feedback-base-stats`
- Current SKU group SparkApplication image: `cr.yandex/de-common/pyspark-gold-sku-group-feedback-base-stats:spark-gold-sku-group-feedback-base-stats-v0.1.0`

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-search*`
- Published image repo: `cr.yandex/de-common/pyspark-gold-sku-group-query-atc-features`
- Current SparkApplication image in config: `cr.yandex/de-common/pyspark-gold-sku-group-query-atc-features:spark-gold-sku-group-query-atc-features-v0.1.0`

## Airflow and Deployment Details

- DAGs import `send_oncall_notification` from `airflow_commons.helpers.oncall`.
- Default Airflow owner: `team:search`.
- Failure callback sends P3 notifications to `oncall_webhook_search`.
- Spark namespace: `svc-data-spark-jobs`.
- Kubernetes connection id: `spark_k8s`.
- SparkApplication placeholders are filled by `config/factory.py`.
- Airflow connections used by the factory:
  - `spark_ycs_connection`
  - `spark_search_research_connection`
- The factory injects:
  - `{{ ds }} 00:00:00` as `<partition_start>`
  - `{{ next_ds }} 00:00:00` as `<partition_end>`
  - fully qualified table name from `config.yaml` as `<table_name>`
  - S3 credentials, Hive metastore URI, Spark event log bucket, node selector, and Spark resources.

## CI and dbt Source Sync

Drone has three main responsibilities:

- Run `ci_test/test_script.py`.
- Run `scripts/sync_dbt_sources.py` to create/update dbt source entries for tables declared in layer `config.yaml` files.
- Sync the corresponding submodule reference in `DayMarket/airflow-dags`.

`ci_config.yaml` configures dbt source sync:

- dbt repo URL is read from env var `DBT_REPO_URL`.
- dbt models path: `models/ml_feature_platform`.
- base branch: `master`.
- database mapping: `iceberg` -> `dwh-iceberg`.
- schema override: branch `dev` uses schema `staging`.

`scripts/sync_dbt_sources.py`:

- Discovers all `layers/**/config.yaml` files with `table` metadata.
- Requires `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, and `table.meta.team`.
- Adds dbt tests:
  - `dbt_utils.unique_combination_of_columns` over the primary key.
  - `not_null` for each primary key column.
  - If `date` is in the primary key, also adds freshness and row-count tests.
- Skips PR publication on branch `dev`.

## Local Commands

Useful read-only or validation commands:

```bash
python ci_test/test_script.py
```

Per-layer package metadata is under each layer's `pyproject.toml`; there is no root `pyproject.toml` at the moment.

Because dependencies are internal/networked, installing packages or building Docker images may require credentials and network access.

## Development Guidance

- Prefer copying the existing layer pattern when adding a new pipeline.
- Keep table metadata in `config.yaml` synchronized with migration DDL and transformation write columns.
- If changing a target table schema, update:
  - `migrations/create_table.sql`
  - `config.yaml`
  - PySpark write/select columns
  - dbt expectations generated from `scripts/sync_dbt_sources.py`
  - downstream DAG dependencies if applicable.
- Do not assume empty README files contain useful context; inspect code/configs directly.
- Be careful with generated or dirty files. This repository has previously contained Python cache files under layer directories; do not treat them as source.
- Before editing existing files, check `git status --short` and avoid reverting unrelated user changes.

## Current Known State

At the time this context file was created:

- `layers/gold/sku_group_query_atc_features/v1/dag.py` was deleted in the working tree.
- `layers/gold/sku_group_query_atc_features/v1/dag.py` was untracked in the working tree.
- These DAG changes appear user-owned; do not revert them unless explicitly asked.
