# Agent Handbook

This file is the repository handbook for coding agents and ML engineer assistants working in `ml-feature-platform`. Keep it focused on workflow, contracts, and where to find facts. Do not turn it into a feature catalog or DAG inventory; per-feature details belong in `layers/**/README.md`, migrations, configs, DAG files, and PySpark jobs.

## Repository Purpose

`ml-feature-platform` contains Airflow-managed PySpark feature pipelines for search-related marketplace features. Pipelines are organized by data layer under `layers/`.

Layer semantics:

- `silver` contains reusable pre-aggregates and intermediate tables.
- `gold` contains final feature tables intended for model consumption and optional ranking-service upload.
- `upload` contains downstream publication processes, currently ranking-service Kafka upload.

Owner metadata:

- Existing configs generally use `team:search`.
- For new layer tables, upload processes, DAGs, or other ownership contexts, always ask the user to explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before creating or editing files. Do this even when nearby tables use `team:search`.

## First Rules

- Read this file first, then inspect the relevant repository files. Do not rely on memory or on this handbook as a feature inventory.
- Before creating or renaming a feature, run the duplicate feature check below.
- If the request changes code, configs, migrations, CI, deployment, downstream contracts, or ownership, first summarize what you understood and the intended approach or options. Wait for agreement when the contract is not already explicit.
- If the implementation contract is unclear, ask concise clarifying questions. It is better to pause than to create a plausible but wrong feature.
- Do not silently inspect production data sources. If repository files do not answer a source schema, value contract, or business meaning question, say so and ask whether to use MCP tools such as Trino or ClickHouse.
- Treat source enum/filter semantics as data semantics, not obvious constants. For fields such as `widget_space_name`, `widget_section_name`, attribution spaces, recommendation placement names, status values, and similar business-coded values, ask the user to confirm the contract or permit MCP inspection when the repository does not document it.
- If a requested source is not produced by this repo, do not invent ownership or ingestion. Ask for upstream table/path, owning team, source freshness/DQ contract, join keys, and whether feature-platform should transform an existing source or whether separate upstream ingestion is needed.
- DAGs that depend on feature-platform tables must wait for the dependent table's dbt DQ DAG, not the Spark DAG that writes that table.
- New layer jobs should use the shared Spark image plus `git-sync`. Add a custom image only when runtime dependencies cannot be delivered by `git-sync`.
- Migrations are executed by CI after merge to `master`; jobs may keep defensive table/column checks, but schema evolution belongs in migrations.
- Be careful with dirty worktrees and generated files. Do not revert unrelated user changes.

## Where To Find Facts

Use repository files as the source of truth. Do not duplicate their contents in this handbook.

- Table name, schema, primary key, and ownership: `layers/**/config.yaml`.
- Output columns and comments: `layers/**/migrations/*.sql`.
- Sources, joins, filters, formulas, windows, null handling, and write behavior: `layers/**/job/*.py`.
- Human-readable feature contract and caveats: `layers/**/README.md`.
- Orchestration, schedule, sensors, task names, and Airflow details: `layers/**/dag.py`, `layers/**/config/fetch_*.yaml`, and `layers/**/config/factory.py`.
- Spark resources and runtime image: `layers/**/config/resources.yaml` and `config/fetch_*.yaml`.
- Ranking-service publication: `upload/ranking_features/v1/config.yaml` and `upload/ranking_features/v1/ranking_service_input.yaml`.
- CI and generated downstream sync behavior: `.drone.yaml`, `scripts/`, and `ci_test/`.

Useful discovery commands:

```bash
find layers -path '*/config.yaml' -print | sort
rg -n "<feature_or_column_or_table_name>" layers upload scripts docs
rg -n "<source_table_or_filter_value>" layers/**/job layers/**/README.md
rg -n "<column_name>" layers/**/migrations upload/ranking_features/v1
```

When a table is declared under `layers/**/config.yaml`, treat `ml-feature-platform` as the owner of that layer table. When a table is only read by jobs and is not declared under `layers/**/config.yaml`, treat it as an upstream external source and confirm its contract before depending on undocumented semantics.

## Duplicate Feature Check

Before adding or renaming any feature:

- Search feature names and close variants with `rg` across `layers/`, `upload/`, `scripts/`, `docs/`, and README files.
- Inspect candidate target-table migrations under `layers/**/migrations/*.sql`; upload validation checks ranking features against migration columns.
- Inspect `upload/ranking_features/v1/config.yaml` and `upload/ranking_features/v1/ranking_service_input.yaml` for downstream usage and required feature order.
- Inspect the PySpark transformation that writes the candidate source table. Similar column names can have different windows, grains, formulas, filters, and null semantics.
- If the requested feature already exists with the same grain and semantics, do not create a duplicate. Report where it is produced, what table stores it, and whether/how it is uploaded.
- If a similar feature exists but differs in grain, window, formula, filter, or null handling, call out the difference and ask whether a new feature is still required.

## Source And MCP Decision

Use this decision flow before implementation:

- If the requested feature can be built from repository-managed feature-platform tables, inspect their configs, migrations, jobs, and README files first. Do not query production just to rediscover facts already encoded in the repo.
- If the requested feature needs upstream external tables, source enum values, schemas, sample values, or business semantics that are not documented in the repo, pause and ask the user to provide the contract or allow MCP inspection.
- If MCP inspection is approved, query only the minimum needed schema, sample, or distinct-value information. Summarize what came from repository files and what came from MCP.
- Never silently treat a literal filter value, table name, or column name as proof of business meaning.
- Never silently add a feature-platform table that owns upstream ingestion for data this repo does not already produce.

## Adding Or Changing A Feature

Use this workflow:

- Classify the requested output: reusable pre-aggregate goes to `silver`; final model feature goes to `gold`; downstream publication goes to `upload`.
- Run the duplicate feature check before scaffolding anything.
- Present meaningful implementation options when more than one is reasonable, especially add-column vs new-table, internal table vs ranking upload, generated vs completed orders, and whether `{{ ds }}` is included.
- Clarify entity grain, source tables, join keys, attribution/filter spaces, date boundaries, lookbacks, generated/completed/returned semantics, null/zero denominator behavior, ranking publication, ownership, alerts, and on-call settings.
- For new entities, ownership and alerting are never safely inferred. Explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before scaffolding configs or DAGs.
- If a feature depends on undocumented source values, explicitly ask whether to use MCP tools such as Trino or ClickHouse or whether the user will provide the contract.
- After duplicate checks and clarification, summarize the selected contract back to the user before editing files.
- Choose the entity grain and primary key. Include `date` for scheduled snapshots unless there is a deliberate exception documented in README and code.
- Add or update migrations first. Use idempotent DDL and include comments for all output columns.
- Implement PySpark using existing local patterns. Prefer Spark functions/DataFrame API where it improves maintainability; Spark SQL is acceptable when it mirrors a validated analytical SQL clearly.
- Keep source table names visible in transformation code or config; do not hide lineage behind opaque constants.
- Update the full entity surface together: `config.yaml`, `dag.py`, resources, SparkApplication template, factory, entrypoint, job code, migrations, and README.
- Add DQ sensor dependencies on DQ DAGs for feature-platform source tables. For external upstream tables, use the producing team's documented DAG/DQ contract.
- Decide whether generated DQ is enough. Propose table-specific DQ tests only when they are part of the feature contract and are not likely to be noisy or expensive.
- Add ranking upload config only if the model/service needs the feature now.
- Run local validation commands before finishing.
- Do not update this handbook just because a feature was added. Update it only when workflow, repository structure, CI, deployment, MCP policy, or downstream contract rules change.

Schema-change checklist:

- Update `migrations/create_table.sql` for new environments.
- Add an idempotent migration for existing environments, for example `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- Update PySpark select/write columns.
- Update README feature descriptions.
- Update ranking upload config if the feature is published.
- Ensure migration comments are present.

Final response checklist for new/changed tables:

- Target table, layer, and repository path.
- Grain and primary key.
- Source tables or paths, join keys, important filters, and source-contract caveats.
- Collection semantics: windows, date boundaries, whether `{{ ds }}` is included, lookbacks, query normalization, denominator/null behavior, and non-obvious logic.
- Output columns/features.
- DQ behavior and whether table-specific DQ was added or intentionally left out.
- Runtime/deployment: default Spark image with `git-sync` or custom image reason.
- Downstream usage: ranking upload group or note that there is no upload.
- Post-master follow-up for new tables: check generated dbt-trino DQ PRs and `DayMarket/pyspark-etl` Iceberg maintenance PRs.

## Lineage Answers

When asked for lineage, answer from final feature back to all sources. Include:

- Final feature column and final table.
- Entity grain and primary key.
- Source layers/tables/paths and join keys.
- Formula, including smoothing constants, `log1p`, windows, filters, null/zero denominator behavior, and normalization.
- DQ dependencies and orchestration facts only after reading the relevant `dag.py`/config files; do not answer those from memory or from this handbook.
- Migration, code, and README paths.
- Ranking upload feature group and feature order if published.
- Known caveats from the implementation.

## Layer Layout

Implemented layer pipelines generally follow this shape:

- `dag.py`: Airflow DAG definition using `SparkKubernetesOperator` and `config.factory.get_deployment`.
- `config.yaml`: table metadata used by DAG factories, CI, dbt source sync, maintenance sync, and upload validation.
- `config/resources.yaml`: JSON-formatted Spark driver/executor resources and infrastructure placeholders.
- `config/fetch_*.yaml`: SparkApplication template populated by `config/factory.py`.
- `config/factory.py`: fills placeholders using `config.yaml`, `resources.yaml`, Airflow connections, random suffixes, and Airflow date macros.
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

- Layer table jobs use shared default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- Job code is delivered through a `git-sync` initContainer.
- `mainApplicationFile` should point to `local:///git/repo/layers/.../entrypoints/*.py`.
- Airflow variable `gitsync_branch` chooses the branch cloned into Spark pods.
- Do not build a new image for ordinary PySpark code, SQL, config, README, migration, or resource changes.
- Some older layer directories may contain inactive `Dockerfile`, `entrypoint.sh`, or `pyproject.toml`. Treat them as inactive unless the active SparkApplication template references them.
- Use a custom Spark image only for Python libraries, truststores, binaries, or runtime files not available in the default image and not deliverable by `git-sync`.
- If a custom image is required, add/update Dockerfile, Drone tag trigger, documentation, and SparkApplication image. Internal packages must be installed from Nexus using Drone-provided build args; never commit credentials.

## DQ And Source Sync

- Every repository-managed entity gets DQ tests in `dbt-trino`.
- DQ DAG id pattern: `dbt.source.trino.ml_feature_platform_<schema>.<table_name>.dq`.
- Use the table's actual schema from `config.yaml`. Do not assume every source is `silver`.
- Downstream DAGs must wait for DQ DAGs of feature-platform dependency tables.
- For upstream DE-owned tables, use the source DAG/DQ contract owned by the producing team.

Automatically generated DQ tests:

- `dbt_utils.unique_combination_of_columns` over all columns from `table.primary_key`.
- `not_null` for every primary key column.
- If `date` is part of the primary key, generated freshness uses `CAST(date AS timestamp) + INTERVAL '1' DAY`, `error_after: count: 2, period: day`, row-count for previous day with `min_rows: 0`, and growth limit with `max_growth_ratio: 0.2`.

Additional DQ tests may be proposed when they match the feature contract:

- `not_null` for required non-key columns.
- Accepted values for enum-like fields.
- Range checks for ratios, probabilities, ratings, prices, counts, and non-negative metrics.
- Cross-column consistency checks such as bucket counts summing to total count or `min <= median <= max`.
- Partition completeness checks when `min_rows: 0` is too weak.
- Relationship checks only when the join key contract is stable and the test is not too expensive.
- Custom SQL tests for business rules that are part of the model contract.

Do not add expensive high-cardinality or source-wide relationship tests blindly. Explain the tradeoff and confirm the intended contract first.

## CI Contracts

Drone currently:

- Runs `scripts/validate_ranking_upload_configs.py`.
- Runs `scripts/run_pyspark_migrations.py --validation-mode` on pushes to `dev`/`master` and on pull requests targeting `dev`/`master` against a disposable local Spark/Iceberg warehouse.
- Runs real SQL migrations only on `master` push.
- Runs dbt source sync, Iceberg maintenance sync, and Airflow submodule sync only on `master` push.
- Builds the ranking upload custom image only on tags matching `spark-feature-platform-ranking-upload-*`.

Trigger policy:

- The main Drone pipeline is triggered on pushes to `dev`/`master` and pull requests targeting `dev`/`master`.
- Pull requests run validation-only checks and must not apply real migrations, sync dbt source PRs, create/update Iceberg maintenance registration, or push Airflow submodule references.
- Push/merge to `dev` runs validation-only checks.
- Push/merge to `master` may run validation plus real side-effecting sync/apply steps.

Migration CI:

- Real migration execution reads Spark/Iceberg settings from `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `HIVE_METASTORE_URIS`, optional `ICEBERG_WAREHOUSE`, and S3/AWS region settings.
- Validation uses a disposable local Spark/Iceberg warehouse and does not require production Hive Metastore or S3 credentials.
- Migration discovery walks `layers/**/config.yaml` with SQL files under `migrations/`.
- `{target_table}` is substituted with the Spark table name from `config.yaml`.
- `create_table.sql` runs first, then remaining migrations in filename order.
- Idempotency validation requires `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`; destructive `DROP`/`DELETE`/`TRUNCATE` statements are rejected.

dbt source sync:

- `ci_config.yaml` maps `iceberg` to `dwh-iceberg`.
- `scripts/sync_dbt_sources.py` discovers repository-managed layer configs and writes source blocks to `models/ml_feature_platform/sources.yaml`.
- It creates one source block per effective schema, named `ml_feature_platform_<schema>`, and removes stale table blocks from managed `ml_feature_platform_*` source blocks.
- Side-effecting dbt source sync runs only on `master` push.

Iceberg maintenance sync:

- `ml-feature-platform` owns maintenance registration only for Iceberg tables it creates from `layers/**/config.yaml`.
- Include repository-created `silver` and `gold` tables; do not add upstream external dependency tables.
- Maintenance removals need manual review; do not remove entries automatically just because a table disappeared locally.
- Side-effecting maintenance sync runs only on `master` push.

## Ranking Feature Upload

Ranking upload lives in `upload/ranking_features/v1`.

Configuration rules:

- Each feature group reads exactly one repository-managed `gold` source table.
- Each group declares `source.schema`, `source.table`, ranking feature group `name`, and ordered `features`.
- Source columns must exist in source-table migrations.
- Feature names are not sent to the ranking service; only ordered values are sent. Reordering is a model-serving contract change.
- Do not reuse one feature group `name` for partial vectors from multiple source tables.
- Catalog, date column, and entity keys are derived from the source layer `config.yaml`; entity keys are the primary key without `date`.
- Supported entity keys are `sku_group_id`, `query`, `account_id`, `query,sku_group_id`, `category_id,sku_group_id`, and `account_id,category_id`.
- `source.dq_execution_delta_minutes` sets the sensor delta from upload logical date to source DQ logical date.
- `source.limit` is only for temporary smoke tests. Production configs must not contain it.
- `log1p_features` is optional and only for features that must be transformed at upload time.
- Update `ranking_service_input.yaml` whenever feature group order, names, schemas, or sizes change.
- Run `python3 scripts/validate_ranking_upload_configs.py`.

## Common Corner Cases

- `{{ ds }}` usually means the partition date being written. Some business logic intentionally uses data strictly before `ds`; inspect the job and README before assuming inclusion/exclusion.
- Trino table names such as `"dwh-iceberg".silver.table` map to Spark names such as `iceberg.silver.table`.
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` support can differ by engine. Repository migrations run through PySpark, so validate syntax against Spark/Iceberg.
- Spark worker imports must be available on executors. If a UDF imports project code, configure executor `PYTHONPATH` and `git-sync` for executors.
- Do not coalesce null/zero denominators to `0.0` unless the feature contract says so.
- Query normalization is feature-specific. Inspect the current job before changing lower/trim/space-collapse/tokenization behavior.
- Production ranking upload must not keep `source.limit`.
- Maintenance sync should add only tables created by this repo, not external dependency tables.

## Local Validation Commands

Run relevant checks before finishing changes:

```bash
python3 ci_test/test_script.py
python3 ci_test/test_sync_dbt_sources.py
python3 ci_test/test_sync_iceberg_maintenance.py
python3 scripts/validate_ranking_upload_configs.py
git diff --check
```

For migration behavior, CI runs Spark against the correct runtime. Local execution may require PySpark, S3, Hive metastore, and network credentials.

## Updating This Handbook

Update this file only when repository-wide instructions change:

- Workflow for discovering, validating, creating, or publishing features.
- Repository structure or required files.
- MCP/source-contract policy.
- Deployment, CI, DQ, maintenance, or ranking-upload rules.
- Custom image policy or non-standard runtime dependency rules.

Do not add a section for each new table, feature, DAG, or upload group. Put feature-specific facts in the layer README, migration, config, DAG, and job files. For agent-specific files such as `CLAUDE.md`, do not duplicate repository knowledge; point them to this `AGENTS.md`.
