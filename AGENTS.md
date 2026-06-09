# Agent Handbook

This file is the single source of repository instructions for any coding agent or ML engineer assistant working in this repo. Read it before making changes. Keep it factual and update it whenever repository structure, feature semantics, CI, deployment, or downstream contracts change.

## Repository Purpose

`ml-feature-platform` contains Airflow-managed PySpark feature pipelines for search-related marketplace features. Pipelines are organized by data layer under `layers/`.

Layer semantics:

- `silver` contains reusable pre-aggregates and intermediate tables.
- `gold` contains final feature tables intended for model consumption and ranking-service upload.

Owner metadata:

- Current owning team in configs and DAG metadata is `team:search`.
- DAG tags should include `team::search` unless the owning team is intentionally changed in `config.yaml`.

## First Rules For Agents

- Read this file first, then inspect the code/configs that are relevant to the requested table or feature.
- Do not rely on memory alone. Confirm feature names, source tables, keys, schedules, sensors, and formulas in `layers/**/config.yaml`, migrations, README files, and PySpark jobs.
- If the request is ambiguous or the implementation contract is not clear, do not invent missing details. Ask concise clarifying questions before generating code, configs, migrations, or upload contracts.
- For every new layer table, upload process, DAG, or other new ownership context, always ask the user to explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before creating or editing files. Do this even when nearby tables appear to use `team:search`; do not silently default or infer these fields for new entities.
- When there is a meaningful design choice, propose options before implementation. Common examples: add a column to an existing table vs create a new table; publish to ranking upload now vs leave as an internal gold table; use generated orders vs completed sales; include `{{ ds }}` vs use `[ds - N, ds - 1]`.
- If uncertain after repository inspection, ask. It is better to pause for clarification than to create a plausible but wrong feature contract.
- If repository files do not contain enough information about an upstream table, table schema, values, or data semantics, say that the repository does not answer the question and ask before querying the table through available MCP tools such as Trino or ClickHouse. Do not silently inspect production data sources.
- Treat source enum/filter semantics as data semantics, not as obvious constants. For fields such as `widget_space_name`, `widget_section_name`, attribution space, source status values, recommendation placement names, or other business-coded values, do not assume meaning from the literal name alone. If the repository does not document the exact value contract, ask the user to confirm it or ask for permission to inspect the source through MCP tools before implementation.
- If a user asks to create a table from data that is not already produced by `ml-feature-platform`, do not invent the source or silently treat this repo as the owner of upstream ingestion. First explain that the data is outside feature-platform, identify what is missing from the repository, and ask the user to confirm the upstream table/path, owning team, source freshness/DQ contract, join keys, and whether feature-platform should only transform that existing source or whether a separate upstream ingestion task is needed.
- Do not immediately start implementation after interpreting a request. First give the user a short summary of what you understood, mention the intended approach or options, and wait for clarification or agreement when the task changes code, configs, migrations, CI, deployment, or downstream contracts.
- Before creating a new feature, search the repository for the requested feature name and close variants. If the feature or an equivalent feature already exists, stop and tell the user where it is produced, what table stores it, and how it is uploaded if applicable.
- If a user asks for lineage of a feature from `feature_marketplace` or ranking/model context, answer from final feature back to all source tables, including formulas, windows, grain, date semantics, DAG/DQ dependencies, and upload feature group order.
- If new code imports a library that is not available in the default Spark image, warn the user before finalizing. Add or update the custom image build path and Drone tag trigger only when `git-sync` cannot deliver the dependency.
- New layer jobs should use the shared Spark image plus `git-sync`. Do not add per-entity images for ordinary PySpark code, SQL, config, README, migration, or resource changes.
- Migrations are executed by CI after merge to `master`; jobs may keep defensive table/column checks, but schema evolution belongs in migrations.
- DAGs that depend on feature-platform tables must wait for the dependent table's dbt DQ DAG, not the Spark DAG that writes that table.
- Be careful with dirty worktrees and generated files. Do not revert unrelated user changes.

## Duplicate Feature Check

Before adding or renaming any feature:

- Search feature names and close variants with `rg` across `layers/`, `upload/`, `scripts/`, and README files.
- Inspect target-table migrations under `layers/**/migrations/*.sql`; CI validates upload features against migration columns.
- Inspect `upload/ranking_features/v1/config.yaml` and `upload/ranking_features/v1/ranking_service_input.yaml` for downstream usage and required order.
- Inspect the PySpark transformation that writes the candidate source table. Similar column names can have different date windows, grains, or null semantics.
- If a requested feature already exists with the same grain and semantics, do not create a duplicate. Report the existing path, table, feature column, schedule, and any downstream feature group.
- If a requested feature is similar but differs in grain/window/formula/null handling, call out the difference and ask whether a new feature is still required.

## Lineage Answer Format

When asked for lineage, include:

- Final feature column and final table.
- Entity grain and primary key.
- Source layers/tables and join keys.
- Formula, including smoothing constants, `log1p`, window boundaries, filters, null/zero denominator behavior, and query normalization.
- Airflow DAG id, schedule, and DQ sensors for feature-platform dependencies.
- Migration/code/README paths.
- Ranking upload feature group name and order if the feature is published to the ranking service.
- Known caveats from the current implementation.

## Repository Map

- `.drone.yaml`: Drone CI for tests, migrations, dbt source sync, Iceberg maintenance sync, Airflow submodule sync, and custom image publishing.
- `.github/CODEOWNERS`: repository code owners.
- `ci_config.yaml`: dbt source sync settings.
- `ci_test/test_script.py`: validates required files, table configs, and migration idempotency.
- `ci_test/test_sync_dbt_sources.py`: regression tests for schema-aware dbt source sync.
- `ci_test/test_sync_iceberg_maintenance.py`: regression tests for Iceberg maintenance sync.
- `scripts/run_pyspark_migrations.py`: executes repository SQL migrations through PySpark on `master` push.
- `scripts/sync_dbt_sources.py`: creates/repairs/removes dbt source entries for repository-managed layer tables.
- `scripts/sync_iceberg_maintenance.py`: creates/updates a PR in `DayMarket/pyspark-etl` for Iceberg maintenance of repository-managed tables.
- `scripts/validate_ranking_upload_configs.py`: validates ranking upload feature groups against layer configs and migrations.
- `layers/`: versioned feature pipelines grouped by layer.
- `upload/`: downstream upload processes, currently ranking-service Kafka upload.
- `docs/`: optional project documentation.

## Layer Layout

Implemented layer pipelines follow this shape:

- `dag.py`: Airflow DAG definition using `SparkKubernetesOperator` and `config.factory.get_deployment`.
- `config.yaml`: table metadata used by DAG factories, CI, dbt source sync, maintenance sync, and upload validation.
- `config.yaml` may define `dag.team` and `alerts.*` for Airflow owner, `team::...` tag, and on-call callback.
- `config/resources.yaml`: JSON-formatted Spark driver/executor resources and infrastructure placeholders.
- `config/fetch_*.yaml`: SparkApplication template populated by `config/factory.py`.
- `config/factory.py`: fills placeholders using `config.yaml`, `resources.yaml`, random suffixes, Airflow connections, and Airflow date macros.
- `job/arguments.py`: parses `--partition_start`, `--partition_end`, and `--table_name`.
- `job/entities.py`: dataclass for runtime arguments.
- `job/getting_*.py`: main PySpark transformation and write logic.
- `entrypoints/*.py`: executable Spark entrypoint that creates `SparkSession`, parses args, calls `job.run`, and stops Spark.
- `migrations/create_table.sql`: Iceberg DDL. Add extra migration files for schema changes.
- `README.md`: Russian-language human summary of purpose, sources, grain, key formulas, and operational notes.

Configuration constraints:

- Existing CI parsers read `config.yaml` with a simple nested key parser. Keep layer configs simple: nested mappings are fine, but avoid YAML anchors, complex lists, and non-obvious syntax unless the CI parser is updated.
- Required table fields are `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, and `table.meta.team`.
- Primary keys should include `date` for daily/hourly feature tables unless there is a deliberate exception documented in README and code.

## Deployment Standard

Layer table jobs:

- Use shared default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- Deliver job code through a `git-sync` initContainer.
- Set `mainApplicationFile` to `local:///git/repo/layers/.../entrypoints/*.py`.
- Use Airflow variable `gitsync_branch` to choose the branch cloned into Spark pods.
- Do not build a new image for code/config/README/DAG/migration/resource-only changes.

Inactive runtime files:

- Some layer directories still contain `Dockerfile`, `entrypoint.sh`, or `pyproject.toml` from earlier deployment patterns. Treat them as inactive unless the active SparkApplication template references them.
- New layer tables should not add those files unless a custom runtime image is truly needed.

Custom image decision:

- Use a custom Spark image only when the job needs Python libraries, truststores, binaries, or runtime files that are not present in the default image and cannot be delivered by `git-sync`.
- Current example: `upload/ranking_features/v1` uses a custom image because it needs `ranking-python-client` and a Kafka truststore.
- If a new custom image is needed, add/update a Dockerfile, add/reuse a Drone Docker build pipeline with a unique tag trigger, document the tag naming, and update the SparkApplication image.
- Internal packages must be installed from Nexus using Drone-provided `NEXUS_USERNAME` and `NEXUS_PASSWORD` build args. Never commit credentials.
- Example tag pattern: `spark-feature-platform-ranking-upload-v0.1.0`; Drone publishes the configured image for tags matching the trigger.
- After the image is built once, later Python job/config changes should still flow through `git-sync`; rebuild only for dependency, base-image, truststore, or Dockerfile-managed runtime changes.

## DQ And Scheduling

- Every repository-managed entity gets DQ tests in `dbt-trino`.
- DQ DAG id pattern: `dbt.source.trino.ml_feature_platform_<schema>.<table_name>.dq`.
- Example: `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_orders.dq`.
- Platform dbt source/DQ DAGs start at `01:00 UTC` by default. Use this as the baseline when calculating `ExternalTaskSensor.execution_delta` unless a specific upstream DAG documents a different schedule.
- Use the table's actual schema in the DQ source name. Do not assume every source is `silver`.
- Downstream DAGs must wait for DQ DAGs of feature-platform dependency tables.
- For upstream DE-owned tables, use the source DAG/DQ contract that the producing team owns. Example: `silver/sku_group_id_prices` waits for `dbt.models.dwh_trino.sku_eod`.

Automatically generated DQ tests:

- `dbt_utils.unique_combination_of_columns` over all columns from `table.primary_key`.
- `not_null` for every primary key column.
- If `date` is part of the primary key, `loaded_at_field` is generated as `CAST(date AS timestamp) + INTERVAL '1' DAY`.
- If `date` is part of the primary key, freshness is generated with `error_after: count: 2, period: day`.
- If `date` is part of the primary key, `row_count_greater_than_for_date` is generated for the previous day with `min_rows: 0`.
- If `date` is part of the primary key, `row_count_growth_within_limit` is generated for the previous day with `max_growth_ratio: 0.2`.

Additional DQ tests an agent may propose or add when they match the feature contract:

- `not_null` for required non-key columns, for example mandatory entity ids or status columns.
- Accepted values for enum-like fields, for example numeric status mappings.
- Range checks for ratios, probabilities, ratings, prices, counts, and non-negative metrics.
- Cross-column consistency checks, for example bucket counts summing to total count or `min <= median <= max`.
- Partition completeness checks with a table-specific minimum row threshold when `min_rows: 0` is too weak.
- Growth/drop thresholds different from the default 20% when the table is expected to be sparse or bursty.
- Relationship checks to dimension/source tables when the join key contract is stable and the test is not too expensive.
- Custom SQL tests for business rules that are part of the model contract.

Do not add expensive high-cardinality or source-wide relationship tests blindly. If a DQ test can be costly or noisy, explain the tradeoff and confirm the intended contract with the user.

## CI Contracts

Drone currently does the following:

- Runs `scripts/validate_ranking_upload_configs.py`.
- Runs `scripts/run_pyspark_migrations.py --validation-mode` on pushes to `dev`/`master` and on pull requests targeting `dev`/`master` against a disposable local Spark/Iceberg warehouse.
- Runs all repository SQL migrations through PySpark on `master` push after merge.
- Runs `scripts/sync_dbt_sources.py` on `master` push to create/update dbt source entries for tables declared in layer configs.
- Runs `scripts/sync_iceberg_maintenance.py` on `master` push to create/update a PR in `DayMarket/pyspark-etl`.
- Syncs the corresponding submodule reference in `DayMarket/airflow-dags` on `master` push.
- Builds the ranking upload custom image only on tags matching `spark-feature-platform-ranking-upload-*`.

Drone trigger policy:

- The main Drone pipeline is triggered on pushes to `dev`/`master` and pull requests targeting `dev`/`master`. For pull requests, Drone evaluates the target branch in the `branch` trigger.
- Feature branches are not expected to run standalone Drone builds unless they are opened as pull requests targeting `dev` or `master`; test feature-branch changes locally before opening/merging when needed.
- Pull requests targeting `dev` or `master` should run validation-only Drone checks: ranking upload config validation and local PySpark migration validation. They must not apply real PySpark migrations to production Hive/S3, sync dbt source PRs, create or update Iceberg maintenance registration, or push Airflow submodule references.
- A push/merge to `dev` should run validation-only Drone checks: ranking upload config validation and local PySpark migration validation. It must not apply real PySpark migrations to production Hive/S3, sync dbt source PRs, create or update Iceberg maintenance registration, or push Airflow submodule references.
- A push/merge to `master` may run both validation and real side-effecting sync/apply steps.

Migration CI:

- Real migration execution runs only for `branch: master` and `event: push`.
- Validation runs on pushes to `dev`/`master` and pull requests targeting `dev`/`master` against a disposable local Spark/Iceberg warehouse, before real master-only migration execution.
- Validation uses the default Spark image and `spark-submit scripts/run_pyspark_migrations.py --repo-root . --validation-mode`; it does not require production Hive Metastore or S3 credentials.
- Real migration execution uses the default Spark image and `spark-submit scripts/run_pyspark_migrations.py --repo-root .`.
- Discovers every `layers/**/config.yaml` with SQL files under `migrations/`.
- Real migration execution reads Spark/Iceberg settings from `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `HIVE_METASTORE_URIS`, optional `ICEBERG_WAREHOUSE`, and S3/AWS region settings.
- Substitutes `{target_table}` with the Spark table name from `config.yaml`.
- Runs `create_table.sql` first, then remaining migrations in filename order.
- Validates idempotency before execution: `CREATE TABLE` must use `IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN` must use `IF NOT EXISTS`, and destructive `DROP`/`DELETE`/`TRUNCATE` statements are rejected.

dbt source sync:

- `ci_config.yaml` maps `iceberg` to `dwh-iceberg`.
- `scripts/sync_dbt_sources.py` discovers all layer configs with table metadata.
- It writes repository-managed source blocks to `models/ml_feature_platform/sources.yaml`.
- It creates one source block per effective schema, named `ml_feature_platform_<schema>`.
- It does not create `sources_gold.yaml`.
- It adds uniqueness/not-null tests from primary keys and adds freshness/row-count tests when `date` is part of the primary key.
- It removes stale table blocks from managed `ml_feature_platform_*` source blocks when a table is no longer declared under `layers/**/config.yaml`, so deleted feature-platform tables do not keep obsolete DQ tests in dbt-trino.
- The side-effecting dbt source sync Drone step should run only on `master` push. Run `ci_test/test_sync_dbt_sources.py` locally when changing source sync behavior.

Iceberg maintenance sync:

- Maintenance is configured in `DayMarket/pyspark-etl`, path `dags_v3/maintenance_generator`.
- `ml-feature-platform` owns maintenance registration only for Iceberg tables it creates from `layers/**/config.yaml`.
- Do not use dbt source files or dbt manifests as the source of truth for maintenance.
- Include both repository-created `silver` and `gold` tables.
- Do not add upstream DE-owned dependency tables such as `iceberg.silver.order_items` or `iceberg.silver.sku`.
- The generated maintenance DAG is separate: `create_dag(config_name="feature_platform", dag_suffix="_fp")`, producing DAG id `spark.iceberg_maintenance_fp`.
- Maintenance sync should run only on `master` push. A `dev` pipeline must not create or update maintenance because the maintenance config is maintained from the master state.
- CI should create/update a PR in `DayMarket/pyspark-etl` only from master-side maintenance sync; when the current repo has an open dev-to-master PR, CI may comment there with the maintenance PR link.
- Do not remove maintenance entries automatically when a table disappears locally; removals need manual review.

## Adding Or Changing A Feature

Use this workflow:

- Classify the requested output: reusable pre-aggregate goes to `silver`; final model feature goes to `gold`; downstream publication goes to `upload`.
- Run the duplicate feature check before scaffolding anything.
- If the requested source data is not in feature-platform, stop before scaffolding and propose the integration contract: external source table/path, source owner/team, upstream DAG or DQ sensor to wait for, schema/partition fields, freshness expectations, and whether a new silver adapter table should be created in feature-platform. Query Trino/ClickHouse/MCP for schema or sample values only after telling the user the repository lacks this information and getting confirmation.
- Before scaffolding, present the viable implementation options when more than one is reasonable, especially whether to add a feature to an existing table or create a new table. Ask the user to choose unless the request already makes the choice explicit.
- Ask clarifying questions for any unspecified or ambiguous contract: entity grain, source table, attribution space, date window boundaries, whether `{{ ds }}` is included, generated vs completed orders, null/zero denominator behavior, publication to ranking upload, schedule, owner team, and on-call settings.
- If a feature depends on source values whose semantics are not documented in the repository, explicitly ask whether to use MCP tools such as Trino or ClickHouse to inspect schema/sample values, or whether the user will provide the contract. Do not implement from a literal value name alone.
- For new entities, ownership and alerting are never considered safely inferred: explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before scaffolding configs or DAGs.
- After duplicate checks and clarification, summarize the selected contract back to the user before editing files: target table or existing table, grain, sources, window/date semantics, schedule, DQ dependencies, ownership/on-call, and whether ranking upload is included.
- Choose the entity grain and primary key. Include `date` for scheduled snapshots.
- Add or update migrations first. Use idempotent DDL and include comments for all columns.
- Implement PySpark using existing local patterns. Prefer Spark functions/DataFrame API where it improves maintainability; Spark SQL is acceptable when it mirrors a validated analytical SQL clearly.
- Keep source table names inside transformation code or config as existing jobs do; do not introduce hidden constants that make lineage harder to read.
- Update `config.yaml`, `dag.py`, resources, SparkApplication template, factory, entrypoint, job code, migrations, and README together.
- Add DQ sensor dependencies on DQ DAGs for feature-platform source tables.
- Decide whether the default generated DQ is enough; if not, propose table-specific DQ tests from the DQ section above.
- Add ranking upload config only if the model/service needs the feature now.
- Run the local validation commands listed at the end of this file.
- After creating or changing a DAG that fills a table, include a concise table summary in the final response.
- Update this handbook if the new feature changes structure, contracts, deployment, CI, or feature inventory.

Post-master merge follow-up for new tables:

- After the feature-platform PR that creates a new table is merged to `master`, remind the user to check and merge the generated dbt-trino PR for DQ tests: https://github.com/DayMarket/dbt-trino/pulls.
- After the feature-platform PR that creates a new table is merged to `master`, remind the user to check and merge the generated `DayMarket/pyspark-etl` PR for Iceberg maintenance automation: https://github.com/DayMarket/pyspark-etl/pulls.
- When reporting generated post-master PRs to the user, include both links: the dbt-trino DQ PR and the `DayMarket/pyspark-etl` Iceberg maintenance PR. Do not send only the dbt-trino PR when a maintenance PR is also created.
- These follow-up PRs are created by master-side CI after the merge to `master`; the user should do this after the master merge, not during the feature-branch stage.

Schema-change checklist:

- Update `migrations/create_table.sql` for new environments.
- Add an `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration for existing environments.
- Update PySpark select/write columns.
- Update README feature descriptions.
- Update ranking upload config if the feature is published.
- Ensure migration comments are present.

Final table summary checklist:

- Target table: fully qualified name, layer, and repository path where it is created.
- DAG: DAG id, schedule, task name, and important sensors/DQ dependencies.
- Grain and primary key: list all primary key columns and partition column.
- Sources: all source tables or external paths, including join keys and important filters.
- Collection semantics: date/window boundaries, whether `{{ ds }}` is included, lookbacks, all-time snapshots, query normalization, denominator/null behavior, and other non-obvious logic.
- Output columns/features: short description of the produced fields or feature groups.
- DQ: generated DQ tests and any table-specific DQ tests added or intentionally not added.
- Deployment/runtime: default Spark image with `git-sync` or custom image reason if one is used.
- Downstream usage: ranking upload feature group, maintenance/dbt source sync, or note that there is no downstream upload yet.

## Current Feature Inventory

### Silver: SKU Group Install/Search Interactions

Path: `layers/silver/sku_group_install/v1`

- Table: `iceberg.silver.feature_platform_search_sku_group_id_install_query`.
- Primary key: `sku_group_id,install_id,query`.
- DAG id: `feature_platform_sku_group_install_silver_stats_dag`.
- Schedule: `0 1 * * *`.
- Source tables: `iceberg.silver_b2c_clickstream.events`, `iceberg.silver.sku`.
- Grain: `install_id`, `sku_group_id`, and search query/category key.
- Metrics: `sum_impressions`, `sum_clicks`, `sum_atc`.
- Filters/logic: uses `SEARCH_RESULTS`, `PRODUCT_IMPRESSION`, `PRODUCT_VIEW`, `ADD_TO_CART`; click/ATC follow product impression in the same `session_id`/`product_id` window.
- Note: migration and code historically used `section`/`space` naming differently. Verify schema and writes before changing this table.

### Silver: SKU Group Prices

Path: `layers/silver/sku_group_id_prices/v1`

- Table: `iceberg.silver.feature_platform_sku_group_id_prices`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_id_prices_silver_dag`.
- Schedule: `0 1 * * *`.
- Sensor: waits for `dbt.models.dwh_trino.sku_eod` with `execution_delta=timedelta(hours=1)`.
- Source tables: `iceberg.silver.sku_eod`, `iceberg.silver.sku`.
- Logic: filters `sku_eod.dt = {{ ds }}`, joins SKU metadata, aggregates by `sku_group_id`.
- Features: average, median, minimum, and maximum EOD sell/full prices.

### Silver: SKU Group Orders

Path: `layers/silver/sku_group_orders/v1`

- Table: `iceberg.silver.feature_platform_sku_group_orders`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_orders_silver_dag`.
- Schedule: `0 1 * * *`.
- Source tables: `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Logic: uses `{{ ds }} 00:00:00` to `{{ next_ds }} 00:00:00` and a 20-day generated-at lookback.
- Metrics: generated, completed, and returned item/order/GMV metrics by `sku_group_id`.

### Silver: Search Orders By Query And SKU Group

Path: `layers/silver/sku_group_query_search_orders/v1`

- Table: `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Primary key: `date,query,sku_group_id`.
- DAG id: `feature_platform_sku_group_query_search_orders_silver_dag`.
- Schedule: `0 1 * * *`.
- Source tables: `iceberg.silver.order_items_attribution`, `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Search attribution spaces: `SHOP_SEARCH_RESULTS`, `COLLECTION_SEARCH_RESULTS`, `SEARCH`, `SEARCH_RESULTS`.
- Logic: uses `{{ ds }} 00:00:00` to `{{ next_ds }} 00:00:00` and a 20-day attribution/order lookback.
- Metrics: generated, completed, and returned item/order/GMV metrics by `query` and `sku_group_id`.
- Important: `orders_generated` is counted as distinct `order_item_id`. Current gold query-order features depend on that exact meaning.

### Silver: Legacy Query/SKU Group Daily Conversions

Path: `layers/silver/query_skg_daily_conversions_legacy/v1`

- Table: `iceberg.silver.feature_platform_query_skg_daily_conversions_legacy`.
- Primary key: `date,query,platform,sku_group_id`.
- DAG id: `feature_platform_query_skg_daily_conversions_legacy_silver_dag`.
- Schedule: `0 1 * * *`.
- Source tables: `iceberg.silver_b2c_clickstream.events`, `iceberg.silver.order_items_attribution`, `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Grain: daily `query`, `platform`, and `sku_group_id`.
- Logic: follows the old feature-store daily conversions approach using `lower(query)` for event metrics, `trim(query)` after joining orders, `widget_section_name = 'SEARCH_RESULTS'`, and `last_atc_platform` for order platform.
- Metrics: `uniq_impressions`, `uniq_clicks`, `uniq_atcs`, `uniq_orders`.
- Downstream: source for legacy query/SKU group aggregated conversions.

### Gold: Query ATC Features

Path: `layers/gold/sku_group_query_atc_features/v1`

- Table: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_features`.
- Primary key: `date,sku_group_id,query_text`.
- DAG id: `feature_platform_sku_group_query_atc_features_gold_dag`.
- Schedule: `0 2 * * *`.
- Sensor: waits for DQ of `iceberg.silver.feature_platform_search_sku_group_id_install_query`.
- Source table: `iceberg.silver.feature_platform_search_sku_group_id_install_query`.
- Logic: uses `space = 'SEARCH_RESULTS'`, normalizes query text with lower/trim, builds 1/3/7/14/21/30/60/90-day windows.
- Features: `query_skg_conv_imp2atc_*` and `share_of_atc_*`.
- Caveat: current implementation should be inspected before changing `query_skg_conv_imp2atc_90`; verify the denominator before editing related formulas.

### Gold: Query ATC And Order Features

Path: `layers/gold/sku_group_query_atc_order_features/v1`

- Table: `iceberg.gold.feature_platform_search_sku_group_id_query_atc_order_features`.
- Primary key: `date,query,sku_group_id`.
- DAG id: `feature_platform_sku_group_query_atc_order_features_gold_dag`.
- Schedule: `0 3 * * *`.
- Sensors: wait for DQ of `iceberg.silver.feature_platform_search_sku_group_id_install_query` and `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Source tables: search interaction silver table and search-order silver table.
- Query normalization: `lower(query)`, replace `ё` with `е`, collapse whitespace, `trim`, filter non-empty query.
- Do not transform query into a tokenized `base_query`; do not remove stopwords, deduplicate tokens, or sort tokens.
- Windows: 1/3/7/14/21/30/60/90 days ending at Airflow `{{ ds }}`.
- Output key base: interaction pairs; order aggregates are left-joined.
- Filters: keep pairs with `query_skg_uniq_impressions_14 >= 2`; exclude pairs with no ATC and no orders over the 90-day feature horizon.
- Division: Spark division semantics are kept for conversions and ratios; zero or missing denominators become `NULL`, not forced to `0.0`.
- Features include `query_skg_uniq_orders_*`, `query_skg_conv_imp2atc_*`, `query_skg_conv_imp2order_*`, and cross-window ratio features such as `query_skg_imp2atc_7_to_3`.
- This table builds each daily partition from silver sources and does not carry rows forward from previous gold partitions.

### Gold: Legacy Query/SKU Group Aggregated And Pairwise Features

Paths:

- `layers/gold/query_skg_aggregated_conversions_legacy/v1`.
- `layers/gold/query_skg_pairwise_features_legacy/v1`.

Tables:

- `iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy`, primary key `date,query,sku_group_id`.
- `iceberg.gold.feature_platform_query_skg_pairwise_features_legacy`, primary key `date,query,sku_group_id`.

DAGs:

- `feature_platform_query_skg_aggregated_conversions_legacy_gold_dag`, schedule `0 3 * * *`, waits for DQ of `iceberg.silver.feature_platform_query_skg_daily_conversions_legacy`.
- `feature_platform_query_skg_pairwise_features_legacy_gold_dag`, schedule `30 3 * * *`, waits for DQ of `iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy`.

Logic:

- Aggregated table reads daily conversions for `[{{ ds }} - 90 days, {{ ds }}]` inclusive.
- Aggregated table collapses `platform`, builds 1/3/7/14/21/30/60/90-day `query_skg_uniq_*` windows, conversion features, and cross-window ratios.
- Legacy filter: keep only rows with `query_skg_uniq_impressions_14 >= 2`.
- Division keeps Spark semantics; zero denominators produce `NULL`, not forced to `0.0`.
- Pairwise table reads aggregated rows for `[{{ ds }} - 30 days, {{ ds }}]`, picks the latest row by `date desc` for each `query,sku_group_id`, and writes it to partition `{{ ds }}`.
- Ranking upload group `fs_search_query_skg_v3` reads the pairwise table and publishes the ordered 29-feature subset.

### Gold: SKU Group Search Conversion Features

Path: `layers/gold/sku_group_search_conversion_features/v1`

- Table: `iceberg.gold.feature_platform_sku_group_search_conversion_features`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_search_conversion_features_gold_dag`.
- Schedule: `0 3 * * *`.
- Sensors: wait for DQ of search interaction and search-order silver tables.
- Source tables: `iceberg.silver.feature_platform_search_sku_group_id_install_query`, `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Grain: `sku_group_id`.
- Windows exclude Airflow `{{ ds }}` and end at `{{ ds }} - 1`.
- Smooth formula: `(0.003384 + orders) / (0.003384 + 1.402240 + impressions)`.
- Raw ratios use Spark division semantics and keep `NULL` for missing or zero denominators.
- Features: `smooth_conv_imp2order_3`, `smooth_conv_imp2order_7`, `smooth_conv_imp2order_14`, `imp2order_3_to_1`, `imp2order_21_to_14`, `imp2order_30_to_21`.

### Gold: SKU Group Search Sales 7D

Path: `layers/gold/sku_group_search_sales_7d/v1`

- Table: `iceberg.gold.feature_platform_sku_group_search_sales_7d`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_search_sales_7d_gold_dag`.
- Schedule: `0 3 * * *`.
- Sensor: waits for DQ of `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Source table: `iceberg.silver.feature_platform_sku_group_query_search_orders`.
- Logic: sums `items_completed` by `sku_group_id` over `[{{ ds }} - 7, {{ ds }} - 1]`; Airflow `{{ ds }}` is excluded.
- Feature: `search_sales_count_7d`.
- Downstream usage: not published to ranking upload unless explicitly added to `upload/ranking_features/v1/config.yaml`.

### Gold: SKU Group Cart Sales Features

Path: `layers/gold/sku_group_cart_sales_features/v1`

- Table: `iceberg.gold.feature_platform_sku_group_cart_sales_features`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_cart_sales_features_gold_dag`.
- Schedule: `0 3 * * *`.
- Source tables: `iceberg.silver.order_items_attribution`, `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Logic: filters attribution rows with `widget_space_name = 'CART'`, joins by `order_item_id`, keeps completed orders with `issued_at` inside the 28-day horizon, excludes items returned before `{{ next_ds }}`, and counts `COUNT(DISTINCT order_id)` by `sku_group_id`.
- Windows include Airflow `{{ ds }}`: `7d = [{{ ds }} - 6, {{ ds }}]`, `14d = [{{ ds }} - 13, {{ ds }}]`, `28d = [{{ ds }} - 27, {{ ds }}]`.
- Features: `cart_sales_count_7d`, `cart_sales_count_14d`, `cart_sales_count_28d`.
- Downstream usage: not published to ranking upload unless explicitly added to `upload/ranking_features/v1/config.yaml`.

### Gold: SKU Group Home Recommendations Average Sales 7D

Path: `layers/gold/sku_group_home_reco_avg_sales_7d/v1`

- Table: `iceberg.gold.feature_platform_sku_group_home_reco_avg_sales_7d`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_home_reco_avg_sales_7d_gold_dag`.
- Schedule: `0 3 * * *`.
- Source tables: `iceberg.silver.order_items_attribution`, `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Logic: identifies homepage recommendation attribution by `widget_space_name`/`widget_section_name`, uses a 20-day generated-at lookback for attribution/order rows, keeps completed items with `issued_at` in `[{{ ds }} - 7, {{ ds }} - 1]`, excludes items returned before the window end, fills missing daily buckets with zero, and averages daily sales over 7 days by `sku_group_id`.
- Feature: `home_reco_avg_sales_count_7d`.
- Known caveat: homepage recommendation placement names are inferred from current attribution naming patterns; confirm exact `widget_space_name`/`widget_section_name` values before using this feature in serving.
- Downstream usage: not published to ranking upload unless explicitly added to `upload/ranking_features/v1/config.yaml`.

### Gold: SKU Group Median Sales 7D

Path: `layers/gold/sku_group_median_sales_7d/v1`

- Table: `iceberg.gold.feature_platform_sku_group_median_sales_7d`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_median_sales_7d_gold_dag`.
- Schedule: `0 */3 * * *`.
- Source tables: `iceberg.silver.order_items`, `iceberg.silver.sku`.
- Runtime boundary: uses Airflow `data_interval_end`, not plain `{{ ds }}`.
- Logic: completed sales over the last 7 days before `data_interval_end`, excluding items returned before the cutoff; split into seven 24-hour buckets, fill missing buckets with zero, compute `median_sales_count_7d`.

### Gold: SKU Group Price Features

Path: `layers/gold/sku_group_price_features/v1`

- Table: `iceberg.gold.feature_platform_sku_group_price_features`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_price_features_gold_dag`.
- Schedule: `0 2 * * *`.
- Sensor: waits for DQ of `iceberg.silver.feature_platform_sku_group_id_prices`.
- Source tables: `iceberg.silver.feature_platform_sku_group_id_prices`, `iceberg.silver.sku`.
- Logic: joins SKU category metadata, computes category mean sell price, discounts, and historical min-full-price ratios.
- Important formula: `sell_price_eod = log1p(avg_sell_price_eod)`.
- Discount columns: `abs_discount = median_full_price_eod - median_sell_price_eod`; `fraq_discount = median_sell_price_eod / median_full_price_eod`.
- Ratio columns: yesterday's `min_full_price_eod` divided by average `min_full_price_eod` over previous 14 and 30 days.
- Division: zero or missing denominators produce `NULL`.

### Gold: SKU Group Price Index Status

Path: `layers/gold/sku_group_price_index_status/v1`

- Table: `iceberg.gold.feature_platform_sku_group_price_index_status`.
- Primary key: `date,sku_group_id`.
- DAG id: `feature_platform_sku_group_price_index_status_gold_dag`.
- Schedule: `0 3 * * *`.
- Source path: `s3a://um-prod-airflow-fs/price_index_dag/dag_runs/{{ ds }}/price_index_features.parquet`.
- Logic: fail clearly if the parquet path is missing; filter out `NO_BOOST`; map supported statuses to numeric classes.
- Columns: `date`, `sku_group_id`, `price_index_status`.
- This is a temporary model-contract table; do not extend it without confirming the serving contract.

### Gold: Feedback Features

Paths:

- `layers/gold/feedback_product_id/v1`.
- `layers/gold/feedback_sku_group_id/v1`.

Tables:

- `iceberg.gold.feature_platform_product_feedback_base_stats`, primary key `date,product_id`.
- `iceberg.gold.feature_platform_sku_group_feedback_base_stats`, primary key `date,sku_group_id`.

DAGs:

- `feature_platform_product_feedback_base_stats_gold_dag`, schedule `0 3 * * *`.
- `feature_platform_sku_group_feedback_base_stats_gold_dag`, schedule `10 3 * * *`.

Logic:

- Source feedback table in Spark: `iceberg.silver_bxappdb2_foodback.public_feedback`.
- Trino source name equivalent: `"dwh-iceberg".silver_bxappdb2_foodback.public_feedback`.
- Join `iceberg.silver.sku` by `sku_id`.
- Use only `status = 'PUBLISHED'`.
- Use all published feedback history with `date_published < {{ ds }}` so the snapshot reflects state up to the previous day.
- Product pipeline groups by `product_id`; SKU group pipeline groups by `sku_group_id`.
- Features: average rating, good/bad counts, counts for ratings 1-5, text-review count, and ratio features.

## Ranking Features Upload

Path: `upload/ranking_features/v1`

Purpose:

- Publishes final gold feature groups to the ranking service through Kafka topic `ranking.features.updates`.
- Uses one DAG and one `config.yaml` for multiple feature groups.
- Reads only repository-managed gold source tables.

Configuration rules:

- Each feature group reads exactly one source table. Do not join multiple source tables inside upload.
- Each group declares `source.schema`, `source.table`, ranking feature group `name`, and ordered `features`.
- `features` are source-table column names. The ranking service receives ordered values, not individual feature names.
- Feature order and feature group order are part of the serving contract.
- Do not reuse one feature group `name` for partial vectors from multiple sources.
- Catalog, date column, and entity keys are derived from the source layer `config.yaml`; entity keys are the primary key without `date`.
- Supported entity keys are `sku_group_id`, `query`, `account_id`, `query,sku_group_id`, `category_id,sku_group_id`, and `account_id,category_id`.
- `source.dq_execution_delta_minutes` sets the sensor delta from upload logical date to the source DQ logical date.
- `source.limit` is only for temporary smoke tests. Production configs must not contain it.
- `log1p_features` is optional and only for features that must be transformed at upload time.
- Update `ranking_service_input.yaml` whenever feature group order, names, schemas, or sizes change.

Current upload groups:

- `fs_search_skg_rating_v1`: `product_rating` from `feature_platform_sku_group_feedback_base_stats`.
- `fs_search_skg_price_ratios_v1`: 14/30-day min-full-price ratios from `feature_platform_sku_group_price_features`.
- `fs_search_skg_conversion_features_v1`: six SKU-group search conversion features from `feature_platform_sku_group_search_conversion_features`.
- `fs_search_skg_price_index_status_v1`: `price_index_status` from `feature_platform_sku_group_price_index_status`.
- `fs_search_skg_price_features_v1`: `sell_price_eod`, `abs_discount`, `fraq_discount`, `category_mean_sell_price` from `feature_platform_sku_group_price_features`.
- `fs_search_query_skg_v3`: 29 query/SKU-group ATC/order features from `feature_platform_query_skg_pairwise_features_legacy`.

Runtime:

- DAG id: `feature_platform_ranking_features_upload_dag`.
- Schedule: `0 4 * * *`.
- Waits for DQ DAGs of all configured source tables.
- Reads each source partition for Airflow `{{ ds }}`.
- Serializes `ranking_python_client.FeaturesUpdate` messages.
- Kafka writer uses `acks=all`.
- Kafka keys are `feature_group_name|entity_keys...` so different feature groups for the same entity do not collide.
- Job logs source row counts, Kafka record counts, and sample key/payload information.
- Driver and executors use `git-sync`; executors receive `/git/repo/upload/ranking_features/v1` through `spark.executorEnv.PYTHONPATH`.
- Custom image `cr.yandex/de-common/pyspark-feature-platform-ranking-upload` contains `ranking-python-client` and Kafka truststore.

## Agent Workflow For Ranking Feature Additions

An ML engineer may provide only a source table and ordered feature names. The agent should:

- Find the source under `layers/**/config.yaml`.
- Confirm the source is a repository-managed `gold` table.
- Confirm all requested columns exist in migrations.
- Confirm the primary key maps to a supported entity schema.
- Add one feature group per source table.
- Preserve the requested feature order exactly.
- Set `source.dq_execution_delta_minutes` from upload schedule and source DQ schedule.
- Update `ranking_service_input.yaml`.
- Run `python3 scripts/validate_ranking_upload_configs.py`.
- If `source.limit` was added for a smoke test, remove it before production.

## Airflow Details

- DAGs import `send_oncall_notification` from `airflow_commons.helpers.oncall`.
- Airflow owner and `team::...` tag should be derived from `config.yaml` through `config.factory.get_dag_settings()`.
- Alert settings should be derived from `alerts.team`, `alerts.severity`, and `alerts.oncall_webhook_conn_id`.
- Defaults are owner `team:search`, severity `P3`, and webhook `oncall_webhook_search`.
- Spark namespace: `svc-data-spark-jobs`.
- Kubernetes connection id: `spark_k8s`.
- Common Airflow connections used by factories: `spark_ycs_connection`, `spark_search_research_connection`.
- Common factory placeholders include partition start/end, target table name, S3 credentials, Hive metastore URI, Spark event log bucket, node selector, and Spark resources.
- Three-hourly jobs can use `data_interval_start`/`data_interval_end` instead of `{{ ds }}`/`{{ next_ds }}` when rolling windows require true interval boundaries.

## Common Corner Cases

- `{{ ds }}` usually means the partition date being written. Some business logic intentionally uses data strictly before `ds` to produce features “as of yesterday”; document this in README and lineage answers.
- A DAG scheduled later than its source should use `execution_delta` equal to upload/source schedule difference in the correct direction. Platform dbt DQ DAGs normally start at `01:00 UTC`; for example, an upload DAG at `04:00 UTC` should use `execution_delta=timedelta(hours=3)`.
- Trino table names such as `"dwh-iceberg".silver.table` map to Spark names such as `iceberg.silver.table`.
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` support can differ by engine. Repository migrations are run through PySpark, so validate syntax against Spark/Iceberg, not only Trino.
- Spark worker imports must be available on executors too. If a UDF imports project code, configure executor `PYTHONPATH` and `git-sync` for executors, as ranking upload does.
- Do not silently coalesce null/zero denominators to `0.0` unless the feature contract says so. Several conversion features intentionally keep Spark `NULL` division semantics.
- Query normalization is feature-specific. For current query ATC/order gold features, only lower/`ё` replacement/space collapse/trim/non-empty filtering is used.
- Production ranking upload must not keep `source.limit`; limits are for explicit smoke checks only.
- Feature names are not sent to ranking service, only ordered values. Reordering is a model-serving contract change.
- Maintenance sync should add only tables created by this repo, not external dependency tables.

## Local Validation Commands

Run the relevant checks before finishing changes:

```bash
python3 ci_test/test_script.py
python3 ci_test/test_sync_dbt_sources.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 scripts/validate_ranking_upload_configs.py
git diff --check
```

For migration behavior, CI runs Spark against real infrastructure after merge. Local execution may require S3, Hive metastore, and network credentials.

## Updating This Handbook

Update this file when:

- A new layer entity or upload process is added.
- A feature formula, window, grain, null handling, or query normalization changes.
- DAG schedules, DQ dependencies, CI behavior, or deployment behavior changes.
- A custom image or non-standard runtime dependency is introduced.
- A downstream ranking-service feature group is added or reordered.

For agent-specific files such as `CLAUDE.md`, do not duplicate repository knowledge. Point them to this `AGENTS.md` so the instructions remain consistent across tools.
