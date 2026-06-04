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
- `gold/sku_group_query_atc_order_features/v1`: daily query and SKU group ATC/order conversion features built from silver interaction and search-order tables.
- `gold/sku_group_search_conversion_features/v1`: daily SKU group search imp-to-order conversion compatibility features.
- `gold/sku_group_median_sales_7d/v1`: three-hourly median daily sales count over the last 7 days by SKU group.
- `gold/sku_group_price_features/v1`: daily SKU group price features built from silver price aggregates.
- `gold/sku_group_price_index_status/v1`: temporary SKU group price index status compatibility feature.
- `gold/feedback_product_id/v1`: daily all-time feedback and rating aggregates by product.
- `gold/feedback_sku_group_id/v1`: daily all-time feedback and rating aggregates by SKU group.

The owning team in configs and DAG metadata is `team:search`.

Layer semantics:

- `silver` contains reusable pre-aggregates and intermediate daily/statistical tables.
- `gold` contains final feature tables intended for model consumption.

DQ dependency semantics:

- Each entity automatically gets DQ tests in the `dbt-trino` repository.
- DQ DAG ids follow this pattern: `dbt.source.trino.ml_feature_platform_<layer>.<table_name>.dq`.
- Example: `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_orders.dq`.
- DAGs that depend on `feature_platform` tables must wait for the dependent table's DQ DAG, not for the Spark DAG or Spark task that writes that table.

## Top-Level Structure

- `.drone.yaml`: Drone CI pipelines for tests, dbt source sync, Airflow submodule sync, and Docker image publishing.
- `.github/CODEOWNERS`: repository code owners.
- `ci_config.yaml`: dbt source sync settings.
- `ci_test/test_script.py`: lightweight CI validation for required files, table configs, and migration CREATE TABLE statements.
- `scripts/run_pyspark_migrations.py`: CI helper for executing repository SQL migrations through PySpark after a merge to `master`.
- `scripts/sync_dbt_sources.py`: CI helper that discovers layer `config.yaml` table definitions and publishes missing dbt source entries to the dbt repository.
- `layers/`: versioned feature pipelines grouped by data layer.
- `docs/`: currently empty.

There are also empty/unimplemented directories such as `layers/gold/sku_group_query_orders/v1`.

## Layer Layout Convention

Each implemented pipeline follows this shape:

- `dag.py`: Airflow DAG definition. Uses `SparkKubernetesOperator` and `config.factory.get_deployment`.
- `config.yaml`: table metadata used by the DAG factory, CI, and dbt source sync.
- `config.yaml` may also define `dag.team` and `alerts.*` values used by `dag.py` for Airflow owner, `team::...` tag, and on-call failure callback.
- `config/resources.yaml`: JSON-formatted resource values for Spark driver/executors and infrastructure placeholders.
- `config/fetch_*.yaml`: SparkApplication template with placeholders filled at DAG parse/runtime.
- `config/factory.py`: fills SparkApplication placeholders using `config.yaml`, `resources.yaml`, random suffixes, Airflow connections, and Airflow date macros.
- `job/arguments.py`: parses `--partition_start`, `--partition_end`, and `--table_name`.
- `job/entities.py`: dataclass for runtime arguments.
- `job/getting_*.py`: main PySpark transformation and write logic.
- `entrypoints/*.py`: executable Spark entrypoint that creates `SparkSession`, parses args, calls `job.run`, and stops Spark.
- `migrations/create_table.sql`: Iceberg table DDL. Most existing Spark jobs still run it when the target table does not exist; `gold/sku_group_median_sales_7d/v1` is the pilot where the migration is executed by CI after merge to `master` instead of at job runtime.
- `Dockerfile`: builds the wheel, installs Spark 3.4.1 / Java 11, copies entrypoints, and prepares the Spark-on-Kubernetes image.
- `entrypoint.sh`: Spark container entrypoint script.
- `pyproject.toml`: Poetry package metadata. Python is pinned to `3.9.13`, PySpark to `3.4.1`.

Exceptions:

- `silver/sku_group_query_search_orders/v1`, `gold/sku_group_query_atc_order_features/v1`, `gold/sku_group_search_conversion_features/v1`, and `gold/sku_group_median_sales_7d/v1` use the git-sync deployment approach. They use the default Spark image and run `mainApplicationFile` from `/git/repo/...`, so these entities do not have their own Dockerfile, `entrypoint.sh`, or `pyproject.toml`.

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
- Produces average, median, minimum, and maximum end-of-day sell price and full price.
- Migration `20260602_add_price_min_max_columns.sql` adds `min_sell_price_eod`, `max_sell_price_eod`, `min_full_price_eod`, and `max_full_price_eod` with `ALTER TABLE`.
- The job ensures missing price min/max columns before building the feature dataframe.
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
- `orders_generated` is intentionally counted as distinct `order_item_id` for backward compatibility with legacy `query_skg_uniq_orders_*` gold features.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Deployment:

- Uses default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- SparkApplication runs `mainApplicationFile` from `local:///git/repo/layers/silver/sku_group_query_search_orders/v1/entrypoints/get_sku_group_query_search_orders.py`.
- Driver pod uses a `git-sync` initContainer to clone `https://github.com/DayMarket/ml-feature-platform/` into `/git/repo`.
- Git branch is controlled by Airflow variable `gitsync_branch`.
- This entity has no per-entity Docker image build in Drone; code changes are picked up from git on the next SparkApplication run.

Resources note:

- Current implemented Spark layers use the same profile: driver `1 core / 10g`, executors `5 x 8 cores / 16g`.
- Feedback and price pipelines use a reduced profile: driver `1 core / 4g`, executors `3 x 4 cores / 8g`.
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
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq` with `execution_delta=timedelta(hours=1)`.
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

## Gold SKU Group Query ATC Order Features Pipeline

Path: `layers/gold/sku_group_query_atc_order_features/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_query_atc_order_features_gold_dag`
- Schedule: `0 3 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `orders`, `atc`.
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq` with `execution_delta=timedelta(hours=2)`.
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq` with `execution_delta=timedelta(hours=2)`.
- Spark task id: `getting_sku_group_query_atc_order_features`
- Runs SparkApplication template `fetch_gold_sku_group_query_atc_order_features.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features`
- Primary key: `date,query,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads search interaction stats from `iceberg.silver.feature_platform_search_sku_group_id_install_query`.
- Reads search order stats from `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Uses only `space = 'SEARCH_RESULTS'`.
- Normalizes the original query text by lower-casing, replacing `ё` with `е`, collapsing whitespace, trimming, and filtering empty values.
- Does not transform queries into `base_query`: stopwords, repeated tokens, punctuation, and word order are otherwise preserved.
- Builds 1, 3, 7, 14, 21, 30, 60, and 90 day windows ending at Airflow `{{ ds }}`.
- Uses interaction pairs as the output key base and left-joins order aggregates.
- Keeps only pairs where `query_skg_uniq_impressions_14 >= 2`, matching the legacy pairwise filter.
- Excludes pairs with no ATC and no orders in the 90-day window because all final features for those pairs are zero.
- Produces order counts, `impression -> atc` conversions, `impression -> order` conversions, and cross-window conversion ratios.
- Uses Spark division semantics for conversions and ratios instead of replacing missing or zero denominators with `0.0`; this keeps legacy-like `NULL` behavior for unstable ratio features.
- Builds each daily snapshot from silver sources and does not carry rows forward from previous target-table partitions.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Deployment:

- Uses default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- SparkApplication runs `mainApplicationFile` from `local:///git/repo/layers/gold/sku_group_query_atc_order_features/v1/entrypoints/get_sku_group_query_atc_order_features.py`.
- Driver pod uses a `git-sync` initContainer to clone `https://github.com/DayMarket/ml-feature-platform/` into `/git/repo`.
- Git branch is controlled by Airflow variable `gitsync_branch`.
- This entity has no per-entity Docker image build in Drone; code changes are picked up from git on the next SparkApplication run.

## Gold SKU Group Search Conversion Features Pipeline

Path: `layers/gold/sku_group_search_conversion_features/v1`

Purpose:

- Compatibility feature set for the previous model's SKU group search conversion features.
- Produces only `smooth_conv_imp2order_3`, `smooth_conv_imp2order_7`, `smooth_conv_imp2order_14`, `imp2order_3_to_1`, `imp2order_21_to_14`, and `imp2order_30_to_21`.

Airflow DAG:

- DAG id: `feature_platform_sku_group_search_conversion_features_gold_dag`
- Schedule: `0 3 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `orders`, `conversion`.
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq` with `execution_delta=timedelta(hours=2)`.
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq` with `execution_delta=timedelta(hours=2)`.
- Spark task id: `getting_sku_group_search_conversion_features`
- Runs SparkApplication template `fetch_gold_sku_group_search_conversion_features.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_sku_group_search_conversion_features`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads search impressions from `iceberg.silver.feature_platform_search_sku_group_id_install_query` with `space = 'SEARCH_RESULTS'`.
- Reads search generated orders from `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Aggregates both sources to `date,sku_group_id`, then builds windows 1, 3, 7, 14, 21, and 30 days.
- SKU group windows exclude the Airflow `{{ ds }}` day and end at `{{ ds }} - 1`, matching the previous model's behavior.
- Smooth order conversion uses `(0.003384 + orders) / (0.003384 + 1.402240 + impressions)`.
- Raw conversion ratios use Spark division semantics and keep `NULL` for missing or zero denominators, matching the legacy feature-store behavior more closely.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Deployment:

- Uses default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- SparkApplication runs `mainApplicationFile` from `local:///git/repo/layers/gold/sku_group_search_conversion_features/v1/entrypoints/get_sku_group_search_conversion_features.py`.
- Driver pod uses a `git-sync` initContainer to clone `https://github.com/DayMarket/ml-feature-platform/` into `/git/repo`.
- Git branch is controlled by Airflow variable `gitsync_branch`.
- This entity has no per-entity Docker image build in Drone; code changes are picked up from git on the next SparkApplication run.

## Gold SKU Group Median Sales 7D Pipeline

Path: `layers/gold/sku_group_median_sales_7d/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_median_sales_7d_gold_dag`
- Schedule: `0 */3 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `orders`.
- Spark task id: `getting_sku_group_median_sales_7d`
- Runs SparkApplication template `fetch_gold_sku_group_median_sales_7d.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_sku_group_median_sales_7d`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads order items from `iceberg.silver.order_items`.
- Joins SKU metadata from `iceberg.silver.sku` by `sku_id`.
- Uses `data_interval_end` as the rolling-window cutoff because the DAG runs every 3 hours.
- Takes completed sales from the last 7 суток before `data_interval_end`, excluding items returned before the interval end.
- Splits the rolling window into seven 24-hour buckets, fills missing buckets with zero sales for SKU groups active in the window, then computes `median_sales_count_7d`.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Important implementation note:

- This layer's `config/factory.py` intentionally injects `data_interval_start` and `data_interval_end` instead of `{{ ds }}` / `{{ next_ds }}` so three-hourly runs use the actual interval boundaries.

Deployment:

- Uses default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- SparkApplication runs `mainApplicationFile` from `local:///git/repo/layers/gold/sku_group_median_sales_7d/v1/entrypoints/get_sku_group_median_sales_7d.py`.
- Driver pod uses a `git-sync` initContainer to clone `https://github.com/DayMarket/ml-feature-platform/` into `/git/repo`.
- Git branch is controlled by Airflow variable `gitsync_branch`.
- This entity has no per-entity Docker image build in Drone; code changes are picked up from git on the next SparkApplication run.

## Gold SKU Group Price Features Pipeline

Path: `layers/gold/sku_group_price_features/v1`

Airflow DAG:

- DAG id: `feature_platform_sku_group_price_features_gold_dag`
- Schedule: `0 2 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `prices`.
- Sensor waits for DQ DAG `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_id_prices.dq` with `execution_delta=timedelta(hours=1)`.
- Spark task id: `getting_sku_group_price_features`
- Runs SparkApplication template `fetch_gold_sku_group_price_features.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_sku_group_price_features`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Reads daily price aggregates from `iceberg.silver.feature_platform_sku_group_id_prices`.
- Joins SKU metadata from `iceberg.silver.sku` to get `category_id`.
- Computes category average sell price, `log1p(avg_sell_price_eod)` as `sell_price_eod`, absolute discount, and `fraq_discount`.
- Computes ratios of yesterday's `min_full_price_eod` to average `min_full_price_eod` over the previous 14 and 30 days.
- Uses nullable division semantics matching SQL `NULLIF`: zero or missing denominators produce `NULL`.
- Writes with `features.writeTo(target_table).overwritePartitions()` after creating the Iceberg table if needed.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-gold-sku-group-price-features-*`
- Published image repo: `cr.yandex/de-common/pyspark-gold-sku-group-price-features`
- Current SparkApplication image: `cr.yandex/de-common/pyspark-gold-sku-group-price-features:spark-gold-sku-group-price-features-v0.1.0`

## Gold SKU Group Price Index Status Pipeline

Path: `layers/gold/sku_group_price_index_status/v1`

Purpose:

- Temporary compatibility table for an old model.
- Produces only `date`, `sku_group_id`, and numeric `price_index_status`.

Airflow DAG:

- DAG id: `feature_platform_sku_group_price_index_status_gold_dag`
- Schedule: `0 3 * * *`
- Start date: `2026-06-01`
- Tags include `spark`, `feature-platform`, `team::search`, `gold`, `price-index`.
- Spark task id: `getting_sku_group_price_index_status`
- Runs SparkApplication template `fetch_gold_sku_group_price_index_status.yaml`.

Target table config:

- Catalog/schema/table: `iceberg.gold.feature_platform_sku_group_price_index_status`
- Primary key: `date,sku_group_id`
- Partition: `date`.

Transformation summary:

- Checks that `s3a://um-prod-airflow-fs/price_index_dag/dag_runs/{{ ds }}/price_index_features.parquet` exists before reading.
- Fails with a clear `FileNotFoundError` if the parquet path is missing.
- Reads the parquet file, filters out `price_index_status = 'NO_BOOST'`, maps supported statuses to integers, and writes the three output columns.

Docker/CI image:

- Drone tag trigger: `refs/tags/spark-gold-sku-group-price-index-status-*`
- Published image repo: `cr.yandex/de-common/pyspark-gold-sku-group-price-index-status`
- Current SparkApplication image: `cr.yandex/de-common/pyspark-gold-sku-group-price-index-status:spark-gold-sku-group-price-index-status-v0.1.0`

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
- Airflow owner and the `team::...` DAG tag should be derived from `config.yaml` via `config.factory.get_dag_settings()`, using `dag.team` with fallback to `table.meta.team`.
- Failure callback alert settings should be derived from `config.yaml` via `config.factory.get_dag_settings()`, using `alerts.team`, `alerts.severity`, and `alerts.oncall_webhook_conn_id`.
- Current default/fallback values are owner `team:search`, team tag `team::search`, severity `P3`, and webhook `oncall_webhook_search`.
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
- Run all repository SQL migrations through PySpark on `master` push after merge.
- Run `scripts/sync_dbt_sources.py` to create/update dbt source entries for tables declared in layer `config.yaml` files.
- Sync the corresponding submodule reference in `DayMarket/airflow-dags`.

PySpark migration CI step:

- Runs only for `branch: master` and `event: push`, so migrations are not applied from PR checks.
- Uses default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- Runs `scripts/run_pyspark_migrations.py --repo-root .` through `spark-submit` and discovers every `layers/**/config.yaml` that has SQL files under `migrations/`.
- Reads Spark/Iceberg settings from `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `HIVE_METASTORE_URIS`, and optionally `ICEBERG_WAREHOUSE`.
- Substitutes `{target_table}` with the Spark table name from `config.yaml`, for example `iceberg.gold.feature_platform_sku_group_median_sales_7d`.
- Runs `create_table.sql` first for each entity, then the remaining migrations in filename order.
- Validates idempotency before execution: `CREATE TABLE` must use `IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN` must use `IF NOT EXISTS`, and destructive `DROP`/`DELETE`/`TRUNCATE` statements are rejected.

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
