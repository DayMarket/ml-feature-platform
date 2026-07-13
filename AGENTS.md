# Agent Handbook

This file is the repository handbook for coding agents and ML engineer assistants working in `ml-feature-platform`. Keep it focused on workflow, contracts, and where to find facts. Do not turn it into a feature catalog or DAG inventory; per-feature details belong in `layers/**/README.md` or `datasets/**/README.md`, migrations, configs, DAG files, and PySpark jobs.

## Repository Purpose

`ml-feature-platform` contains Airflow-managed feature pipelines for search-related marketplace features and training dataset pipelines for model development. Feature pipelines are organized by data layer under `layers/`; training datasets are organized under `datasets/`. Most existing layer jobs are PySpark/SparkApplication jobs, but new feature and dataset pipelines may also read from Trino or ClickHouse when the source is not available through Spark/Iceberg.

All repository-managed feature, aggregate, and training dataset outputs must be stored as Iceberg tables. Trino and ClickHouse are allowed as source engines, not as final storage for `silver`, `gold`, or `datasets` outputs.

Top-level areas:

- `silver` contains reusable pre-aggregates and intermediate tables.
- `gold` contains final feature tables intended for model consumption and optional ranking-service upload.
- `datasets` contains training sample tables for offline model training and evaluation. Dataset tables must not be uploaded to ranking-service, inference services, or any online serving path from this repository.
- `upload` contains downstream publication processes, currently ranking-service Kafka upload.

Owner metadata:

- Existing configs generally use `team:search`.
- For new layer tables, dataset tables, upload processes, DAGs, or other ownership contexts, always ask the user to explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before creating or editing files. Do this even when nearby tables use `team:search`.

## First Rules

- Read this file first, then inspect the relevant repository files. Do not rely on memory or on this handbook as a feature inventory.
- The canonical Airflow namespace is `feature-platform`; `ml-feature-platform` is only the Git repository name. Do not mix these values in DAG ids or tags.
- Layer DAG ids must encode the repository path using Airflow-safe dots: `feature-platform.layers.<layer>.<primary_key_group>.<entity>`, without the version suffix. For example, `layers/gold/h3_index/location_h3_forecast_features/v1/dag.py` must use `feature-platform.layers.gold.h3_index.location_h3_forecast_features`. Airflow keys allow only alphanumeric characters, dashes, dots, and underscores, so never use path slashes in a DAG id. Do not invent shorter aliases such as `location_forecast_features_dag`.
- Dataset DAG ids must encode the repository path using Airflow-safe dots: `feature-platform.datasets.<team>.<domain>.<version>`. For example, `datasets/search/query_relevance/v1/dag.py` must use `feature-platform.datasets.search.query_relevance.v1`. Do not omit the dataset version from the DAG id because dataset versions are part of the repository path and training contract.
- Every related DAG group must have one repository-unique, human-readable Airflow group tag, stored as `dag.group_tag` in each member entity's `config.yaml` and added to each member DAG from config. All DAGs in the group must use the exact same tag, unrelated DAGs must not reuse it, and each entity README must state it. For example, the location forecast chain uses `location-h3-forecast`.
- Every layer entity README must state the fully qualified output table (`<catalog>.<schema>.<name>`) and the exact DAG id. Keep both values consistent with `config.yaml` and `dag.py`.
- Every dataset README must state the fully qualified output table (`<catalog>.<schema>.<name>`), the exact DAG id, the dataset path (`datasets/<team>/<domain>/<version>`), and the training/evaluation purpose. Keep these values consistent with `config.yaml` and `dag.py`.
- `layers/` is an entity tree grouped by primary key, not a shared-library root. Keep an entity's runtime, query, write, and orchestration code inside `layers/<layer>/<primary_key_group>/<entity>/vN/`. Derive `primary_key_group` by removing `date` from `table.primary_key`, preserving the remaining column order, and joining the names with underscores. The `sku_group_install` entity is the sole semantic exception and uses `sku_group_id_query_category`. Do not create `layers/_common`, `layers/_commons`, or another cross-entity utility package there. If reuse is genuinely needed, first identify an existing approved top-level shared location or ask the user to approve a repository-wide abstraction; do not introduce one implicitly.
- `datasets/` is an entity tree for repository-managed training sample tables, not a shared-library root. Keep a dataset's runtime, query, write, and orchestration code inside `datasets/<team>/<domain>/<version>/`. Before creating anything under `datasets/`, explicitly ask the user to confirm `team`, `domain`, and `version`; do not infer them from nearby paths, table names, or ownership metadata.
- One repository-managed layer table means one self-contained entity directory and orchestration contract. Do not declare several independent silver tables in separate configs while materializing them from a gold entity's DAG or code. Each silver entity owns its own DAG, source query/job, runtime configuration, migration, and README; the gold entity only reads completed silver outputs.
- One repository-managed dataset table means one self-contained dataset directory and orchestration contract. Do not materialize several independent dataset tables from one dataset DAG or code path unless the user explicitly approves that contract and the CI/config tooling supports it.
- Before creating or renaming a feature or dataset, run the duplicate feature or dataset check below.
- If the request changes code, configs, migrations, CI, deployment, downstream contracts, or ownership, first summarize what you understood and the intended approach or options. Wait for agreement when the contract is not already explicit.
- If the implementation contract is unclear, ask concise clarifying questions. It is better to pause than to create a plausible but wrong feature.
- Do not silently inspect production data sources. If the user has not explicitly provided the source contract and repository files do not fully answer a source schema, value contract, sample-row need, partition/freshness contract, enum/filter value, or business meaning question, say so and ask whether to use MCP tools such as Trino or ClickHouse.
- Treat source enum/filter semantics as data semantics, not obvious constants. Finding a table, column, or literal filter value in the repository does not by itself confirm that it is the right source contract for a new feature or dataset. For fields such as `widget_space_name`, `widget_section_name`, attribution spaces, recommendation placement names, status values, and similar business-coded values, ask the user to confirm the contract or permit MCP inspection unless the current request or an existing README/job contract explicitly removes the ambiguity.
- If a requested source is not produced by this repo, do not invent ownership or ingestion. Ask for upstream table/path, owning team, source freshness/DQ contract, join keys, and whether feature-platform should transform an existing source or whether separate upstream ingestion is needed.
- DAGs that depend on feature-platform tables must wait for the dependent table's dbt DQ DAG, not the Spark DAG that writes that table.
- A gold DAG that reads repository-managed silver tables must declare a sensor dependency for every such silver table's dbt DQ DAG. A Python import, task ordering inside one combined DAG, matching schedules, or the fact that the silver table already exists is not an acceptable substitute.
- A dataset DAG that reads repository-managed layer or dataset tables must declare a sensor dependency for every source table's dbt DQ DAG. A training dataset must not bypass DQ sensors just because it is offline-only.
- When adding or changing DAG schedules, `start_date`, backfill ranges, or partition intervals, ask the user to confirm the launch time in UTC and propose UTC times by default. Keep Airflow `start_date`/`end_date` timezone-aware UTC, and generate job partition boundaries from Airflow `data_interval_start`/`data_interval_end` converted to UTC.
- When adding or changing Spark layer or dataset DAGs, ask the user which Spark resource profile to use or what driver/executor resources are expected. Use `config/spark/resources.yaml` profiles and reference them from the entity `config.yaml`; add a new named profile only after the resource contract is confirmed.
- New Spark layer and dataset jobs should use the shared Spark image plus `git-sync`. Add a custom Spark image only when runtime dependencies cannot be delivered by `git-sync`.
- Trino-source DAGs have two supported Airflow connections: `trino_search` for search-domain workloads and `trino_recsys` for recommendation-system workloads. Propose the connection that matches the source and workload context; if the context does not determine it unambiguously, present both options and use the user's final choice. Do not silently lock in a connection when the contract has not been confirmed.
- For ClickHouse-source DAGs, always ask the user for the Airflow connection id before scaffolding or editing files, because ClickHouse access is RBAC-sensitive. Do not infer the connection from nearby DAGs.
- For Trino/ClickHouse-source layer or dataset jobs, use a separate Airflow/Python pattern that reads through the confirmed source connection and writes the repository-managed output to Iceberg with `pyiceberg`. Do not model these jobs as SparkApplications unless the user explicitly chooses Spark execution.
- For Trino/ClickHouse-source layer or dataset jobs, propose `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` as the default runtime image. If the job needs third-party libraries that are not available in the default image and cannot be safely delivered by repository code, ask the user whether to create or use a custom image before scaffolding runtime files.
- Migrations are executed by CI after merge to `master`; jobs may keep defensive table/column checks, but schema evolution belongs in migrations.
- Treat `config.yaml` as the single source of truth for repository-managed table identifiers. Do not duplicate values in constants such as `GOLD_IDENTIFIER`, `TABLE_IDENTIFIER`, or hard-coded `"schema.table"` strings. DAGs and jobs must load `table.catalog`, `table.schema`, and `table.name` from the owning entity config and pass them explicitly.
- `table.meta.create_dbt_pr` and `table.meta.create_maintenance_pr` are optional bool flags controlling whether master-side CI creates missing downstream PRs in `DayMarket/dbt-trino` and `DayMarket/pyspark-etl` Iceberg maintenance. Both default to `true` when absent. `true` means the table is eligible for automatic downstream onboarding if the corresponding record is missing. `false` means CI must skip creating that downstream PR entry for this table. A `false` flag is not a destructive delete signal: CI must not remove existing dbt-trino source/DQ records or existing maintenance records only because the flag is false.
- Be careful with dirty worktrees and generated files. Do not revert unrelated user changes.

## Where To Find Facts

Use repository files as the source of truth. Do not duplicate their contents in this handbook.

- Layer table name, schema, primary key, and ownership: `layers/**/config.yaml`.
- Dataset table name, schema, primary key, ownership, team, domain, and version: `datasets/**/config.yaml` plus the repository path.
- Output columns and comments: `layers/**/migrations/*.sql` and `datasets/**/migrations/*.sql`.
- Sources, joins, filters, formulas, windows, null handling, and write behavior: `layers/**/job/*.py` and `datasets/**/job/*.py`.
- Human-readable feature or dataset contract and caveats: `layers/**/README.md` and `datasets/**/README.md`.
- Orchestration, schedule, sensors, task names, and Airflow details: `layers/**/dag.py`, `datasets/**/dag.py`, each entity's `config.yaml` `spark` or `spark_applications` block, and any local `config/factory.py`.
- Spark runtime template and resource profiles: `config/spark/layer_spark_application.yaml`, `config/spark/resources.yaml`, and each entity's `config.yaml` `spark` or `spark_applications` block. These are Spark-specific and should not be forced onto Trino/ClickHouse-source Airflow/Python jobs.
- Ranking-service publication: `upload/features_service_upload/v1/config.yaml` and `upload/features_service_upload/v1/ranking_service_input.yaml`.
- CI and generated downstream sync behavior: `.drone.yaml`, `scripts/`, and `ci_test/`.

Useful discovery commands:

```bash
find layers -path '*/config.yaml' -print | sort
find datasets -path '*/config.yaml' -print 2>/dev/null | sort
rg -n "<feature_or_column_or_table_name>" layers upload scripts docs
rg -n "<feature_or_column_or_table_name>" datasets 2>/dev/null
rg -n "<source_table_or_filter_value>" layers/**/job layers/**/README.md
rg -n "<source_table_or_filter_value>" datasets/**/job datasets/**/README.md 2>/dev/null
rg -n "<column_name>" layers/**/migrations upload/features_service_upload/v1
rg -n "<column_name>" datasets/**/migrations 2>/dev/null
```

When a table is declared under `layers/**/config.yaml` or `datasets/**/config.yaml`, treat `ml-feature-platform` as the owner of that table. When a table is only read by jobs and is not declared under those config paths, treat it as an upstream external source and confirm its contract before depending on undocumented semantics.

## Duplicate Feature Or Dataset Check

Before adding or renaming any feature or dataset:

- Search feature names, dataset names, labels, and close variants with `rg` across `layers/`, `datasets/`, `upload/`, `scripts/`, `docs/`, and README files.
- Inspect candidate target-table migrations under `layers/**/migrations/*.sql` and `datasets/**/migrations/*.sql`; upload validation checks ranking features against migration columns.
- Inspect `upload/features_service_upload/v1/config.yaml` and `upload/features_service_upload/v1/ranking_service_input.yaml` for downstream usage and required feature order.
- Inspect the PySpark or Airflow/Python transformation that writes the candidate source table. Similar column names can have different windows, grains, formulas, filters, label definitions, leakage boundaries, and null semantics.
- If the requested feature already exists with the same grain and semantics, do not create a duplicate. Report where it is produced, what table stores it, and whether/how it is uploaded.
- If a similar feature exists but differs in grain, window, formula, filter, or null handling, call out the difference and ask whether a new feature is still required.

## Source And MCP Decision

Use this decision flow before implementation:

- If the requested feature or dataset appears buildable from repository-managed feature-platform tables, inspect their configs, migrations, jobs, and README files first. Do not query production just to rediscover facts already encoded in the repo.
- Finding a plausible source table in the repository is not enough to proceed when the source choice, filter values, or business semantics are not explicit in the user's request or in a relevant existing feature or dataset contract. Pause and ask the user to confirm the source contract or allow MCP inspection through Trino or ClickHouse.
- If the source engine is unclear from the request, existing README/job/config, or selected source contract, ask whether the source should be read through Spark/Iceberg, Trino, or ClickHouse. Do not ask when the engine is already explicit in context.
- If the requested feature or dataset needs upstream external tables, source enum values, schemas, sample values, partition/freshness checks, or business semantics that are not explicitly covered by the request or repository contract, pause and ask the user to provide the contract or allow MCP inspection.
- If MCP inspection is approved, query only the minimum needed schema, sample, or distinct-value information. Summarize what came from repository files and what came from MCP.
- Never silently treat a literal filter value, table name, or column name as proof of business meaning.
- Never silently add a feature-platform table or dataset that owns upstream ingestion for data this repo does not already produce.

## Adding Or Changing A Feature

Use this workflow:

- Classify the requested output: reusable pre-aggregate goes to `silver`; final model feature goes to `gold`; offline training sample goes to `datasets`; downstream publication goes to `upload`.
- Run the duplicate feature or dataset check before scaffolding anything.
- Present meaningful implementation options when more than one is reasonable, especially add-column vs new-table, internal table vs ranking upload, generated vs completed orders, and whether `{{ ds }}` is included.
- Clarify entity grain, source tables, source engine when unclear, source connection, join keys, attribution/filter spaces, date boundaries, lookbacks, DAG launch time in UTC, generated/completed/returned semantics, null/zero denominator behavior, Iceberg write mode, upstream DQ/freshness contract, ranking publication or offline-only dataset status, ownership, alerts, and on-call settings.
- For Spark jobs, clarify the Spark resource profile or driver/executor resource expectations. For Trino/ClickHouse-source jobs, clarify the Airflow/Python runtime image and whether third-party libraries require a custom image.
- For new entities, ownership and alerting are never safely inferred. Explicitly confirm `table.meta.team`, `dag.team`, `alerts.team`, alert severity, and on-call webhook before scaffolding configs or DAGs.
- If a feature depends on source tables, source values, or business semantics that are not explicit in the current context, ask whether to use MCP tools such as Trino or ClickHouse or whether the user will provide the contract. Do this even when a plausible table or column was found in the repository.
- After duplicate checks and clarification, summarize the selected contract back to the user before editing files.
- Choose the entity grain and primary key. Include `date` for scheduled snapshots unless there is a deliberate exception documented in README and code.
- Add or update migrations first. Use idempotent DDL and include comments for all output columns. Every new `migrations/create_table.sql` for a repository-managed Iceberg output table must include `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` in the `CREATE TABLE IF NOT EXISTS` statement.
- Implement Spark jobs using existing local PySpark patterns. Prefer Spark functions/DataFrame API where it improves maintainability; Spark SQL is acceptable when it mirrors a validated analytical SQL clearly.
- Implement Trino/ClickHouse-source jobs using an Airflow/Python pattern: read from the confirmed source connection, transform according to the approved SQL/source contract, and write the final result to Iceberg through `pyiceberg`.
- Keep source table names visible in transformation code or config; do not hide lineage behind opaque constants.
- Update the full entity surface together: `config.yaml`, `dag.py`, runtime configuration, factory or helper code when used, entrypoint/job code, migrations, and README.
- In every changed entity README, include an explicit orchestration section with the exact DAG id and an explicit output section with the fully qualified table name. Do not make readers infer either value from a path or prose.
- Add DQ sensor dependencies on DQ DAGs for feature-platform source tables. For external upstream tables, use the producing team's documented DAG/DQ contract.
- For new Airflow/Spark jobs, pass interval boundaries with `{{ data_interval_start }}` and `{{ data_interval_end }}`-based templates instead of `{{ ds }}` and `{{ next_ds }}`. Convert them to the business timezone explicitly when the feature contract is timezone-specific.
- When a requested DAG schedule is expressed in a business timezone, confirm or derive the actual Airflow cron timezone before writing `schedule_interval`; if Airflow schedules in UTC, convert the cron expression explicitly and document the business-time equivalent in the README or DAG comments.
- When deriving a partition date from an interval argument in Python, parse the timestamp explicitly through a tested helper instead of using string slicing such as `partition_start[:10]` or a single-format `datetime.strptime(partition_start, "%Y-%m-%d %H:%M:%S")`. Reuse an existing entity-local helper when available; otherwise add one beside the job code. New parsers must accept Airflow/Pendulum ISO timestamps with and without timezone (`2026-06-17T00:00:00`, `2026-06-17T00:00:00+00:00`, `2026-06-17T00:00:00Z`, `2026-06-17 00:00:00+00:00`) as well as the shared-template format `YYYY-MM-DD HH:MM:SS`, and should raise a clear error that includes the unsupported value. Add or update a small regression test for these accepted formats whenever adding or touching partition-date parsing.
- Decide whether generated DQ is enough. Propose table-specific DQ tests only when they are part of the feature contract and are not likely to be noisy or expensive.
- Add ranking upload config only if the model/service needs the feature now. Never add ranking upload config for `datasets/**` outputs.
- Run local validation commands before finishing.
- Do not update this handbook just because a feature was added. Update it only when workflow, repository structure, CI, deployment, MCP policy, or downstream contract rules change.

Schema-change checklist:

- Update `migrations/create_table.sql` for new environments.
- For every newly created repository-managed Iceberg table, include `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` in `migrations/create_table.sql`.
- Add an idempotent migration for existing environments, for example `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- Update PySpark select/write columns.
- Update README feature or dataset descriptions.
- Update ranking upload config if the feature is published. Dataset tables must not be added to ranking upload config.
- Ensure migration comments are present.

Final response checklist for new/changed tables:

- Target table, repository area (`layers` or `datasets`), and repository path.
- Grain and primary key.
- Source tables or paths, join keys, important filters, and source-contract caveats.
- Collection semantics: windows, date boundaries, `data_interval_start`/`data_interval_end` inclusion or exclusion, lookbacks, query normalization, denominator/null behavior, and non-obvious logic.
- Output columns, features, or labels.
- DQ behavior and whether table-specific DQ was added or intentionally left out.
- Runtime/deployment: Spark image with `git-sync`, or Airflow/Python image for Trino/ClickHouse-source jobs, plus any custom image reason.
- Downstream usage: ranking upload group or note that there is no upload.
- For dataset tables, state explicitly that the output is offline-only and is not uploaded to ranking-service or any inference service.
- Post-master follow-up for new tables: check generated dbt-trino DQ PRs and `DayMarket/pyspark-etl` Iceberg maintenance PRs.

## Adding Or Changing A Dataset

Use this workflow for training sample tables under `datasets/`:

- Before creating any files, ask the user to explicitly confirm `team`, `domain`, and `version`. The required path is `datasets/<team>/<domain>/<version>/`; do not infer these values from source tables, ownership metadata, branch names, or nearby directories.
- Confirm the dataset purpose, training/evaluation consumer, entity grain, primary key, label definition, sample inclusion/exclusion rules, leakage boundary, positive/negative sampling logic, source tables, source engine, source connection, date boundaries, lookbacks, freshness/DQ expectations, write mode, launch time in UTC, ownership, alerts, and on-call settings.
- Confirm whether the dataset is a one-time/backfill artifact or a scheduled repository-managed table. Scheduled datasets should include `date` or another explicit snapshot/partition key in `table.primary_key` unless the user approves and documents a deliberate exception.
- Dataset outputs are repository-managed Iceberg tables and must use `table.catalog: iceberg`. Keep `config.yaml` as the single source of truth for `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, and `table.meta.team`.
- Dataset tables must not be uploaded to ranking-service, inference services, or online serving systems from this repository. Do not edit `upload/features_service_upload/v1/config.yaml` or `upload/features_service_upload/v1/ranking_service_input.yaml` for a dataset output.
- Run the duplicate feature or dataset check before scaffolding. Existing feature tables or older dataset versions may already contain the same labels or samples with different windows, filters, grains, or leakage boundaries.
- After duplicate checks and clarification, summarize the selected dataset contract back to the user before editing files.
- Keep the full dataset surface together inside `datasets/<team>/<domain>/<version>/`: `config.yaml`, `dag.py`, runtime configuration, factory or helper code when used, entrypoint/job code, migrations, and README.
- Dataset DAG ids must use `feature-platform.datasets.<team>.<domain>.<version>`. Add `dag.group_tag` when the dataset belongs to a related DAG group, and state the tag in the README.
- Dataset README files must include explicit output, orchestration, and dataset-contract sections: fully qualified table, exact DAG id, path, grain, primary key, sources, label semantics, windows, leakage boundary, null handling, and operational caveats.
- Add DQ sensor dependencies on DQ DAGs for every repository-managed source table from `layers/**` or `datasets/**`. For external upstream tables, use the producing team's documented DAG/DQ contract.
- Implement Spark datasets with the same shared Spark image plus `git-sync` pattern used by layer jobs. Implement Trino/ClickHouse-source datasets with the Airflow/Python plus `pyiceberg` pattern after the source connection is confirmed.
- Add or update migrations first. Use idempotent DDL and include comments for all output columns, including labels, weights, split columns, and sampling metadata. Every new dataset `migrations/create_table.sql` must include `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` in the `CREATE TABLE IF NOT EXISTS` statement.
- Decide whether generated DQ is enough. For datasets, consider table-specific checks for label accepted values, split accepted values, non-negative weights, primary-key uniqueness, and partition completeness only when they are part of the training contract and not noisy or expensive.
- Run local validation commands before finishing. If current CI scripts only discover `layers/**/config.yaml`, update the relevant CI scripts and tests before adding the first dataset table.

## Removing Or Deprecating A Feature Or Dataset

Do not treat feature or dataset removal as a single file deletion. A repository table may have ranking upload usage, training consumers, other feature-platform jobs, external model consumers, dbt DQ sources, Iceberg maintenance registration, Airflow orchestration, and physical Iceberg data.

Use this workflow:

- First classify the requested action: deprecate only, stop producing new partitions, remove from repository ownership, remove from ranking upload, remove a dataset training consumer, or physically drop/archive data.
- Search downstream usage with `rg` across `layers/`, `datasets/`, `upload/`, `scripts/`, `docs/`, README files, and ranking upload configs.
- Inspect the table's `config.yaml`, migrations, README, PySpark job, and any downstream jobs that read it.
- If repository files do not prove that there are no external consumers, ask the user to confirm the consumer contract or allow MCP/catalog inspection before removal.
- If the feature is published to ranking, remove it from `upload/features_service_upload/v1/config.yaml` and update `ranking_service_input.yaml` only after confirming serving compatibility and feature order changes.
- Prefer a staged removal when consumer risk is unclear: mark the feature, dataset, or table as deprecated in README or config, stop downstream upload first, then stop production after an agreed grace period.
- Stopping production may mean pausing/removing the DAG or removing the layer or dataset from the repo, but physical Iceberg data should remain until an explicit drop/archive decision is approved.
- Do not add destructive `DROP`, `DELETE`, or `TRUNCATE` migrations to ordinary repository migrations. CI rejects destructive statements, and physical table deletion must be a separate approved operational runbook.
- Removing a layer or dataset config lets dbt source sync remove managed dbt source blocks, but Iceberg maintenance removals require manual review. Do not silently remove maintenance entries or external registrations.
- In the final response, state exactly what was removed, what remains physically in storage, which downstream configs changed, and what follow-up PRs or manual operations are still required.

## Lineage Answers

When asked for lineage, answer from final feature or dataset back to all sources. Include:

- Final feature or dataset column and final table.
- Entity grain and primary key.
- Source layers, datasets, tables, paths, and join keys.
- Formula, including smoothing constants, `log1p`, windows, filters, null/zero denominator behavior, and normalization.
- DQ dependencies and orchestration facts only after reading the relevant `dag.py`/config files; do not answer those from memory or from this handbook.
- Migration, code, and README paths.
- Ranking upload feature group and feature order if published; for datasets, state that there is no inference upload.
- Known caveats from the implementation.

## Production Comments And Docstrings

Comments and docstrings in production DAGs and jobs are part of the maintained contract. Keep them strict, short, and specific to the actual entity and values:

- Prefer a one-line module docstring that states the current responsibility, for example `"""Write the daily geo activity snapshot to Iceberg."""`. Use a longer docstring only when a non-obvious input, output, invariant, or failure mode cannot be expressed clearly in one or two additional lines.
- Do not add narrative module headers that describe repository history, announce that an implementation is the first of its kind, explain how the agent arrived at the design, or broadly introduce the platform. Phrases such as “This is the first non-Spark runtime” do not belong in production code.
- Do not duplicate README, `AGENTS.md`, deployment, image, connection, warehouse, credential, or dependency documentation in code comments. Put the durable operational contract in the entity README and keep runtime values in `config.yaml` or the appropriate configuration source.
- Comments must describe the concrete entity behavior or the reason for a non-obvious decision. Do not restate the following code, advertise implementation details, or use generic template prose such as “shared runtime for feature-platform jobs.”
- Adapt every comment to the values actually used by that entity. Verify table layer or dataset path/name, source engine, connection, partition field, interval boundary, schedule, and dependency names; never copy a comment from another DAG without reconciling those values.
- Keep comments adjacent to the invariant they protect. If code or configuration makes a comment false or redundant, update or remove the comment in the same change.
- Lazy imports may have a short reason such as `# Runtime-only dependency; unavailable in migration validation.` Do not use a multi-paragraph docstring to list packages, images, catalog wiring, or credential sources.
- Treat oversized or stale explanatory headers as a production-readiness defect. Agents must shorten or remove them while editing the affected DAG/job, even when the functional code is otherwise correct.

## Layer Layout

Implemented Spark layer pipelines generally follow this shape:

- `dag.py`: Airflow DAG definition using `SparkKubernetesOperator` and `config.factory.get_deployment`.
- `config.yaml`: table metadata used by DAG factories, CI, dbt source sync, maintenance sync, and upload validation.
- `config/factory.py`: fills placeholders using `config.yaml`, shared Spark resources, Airflow connections, random suffixes, and Airflow date macros.
- `config.yaml` `spark` or `spark_applications`: points to the shared SparkApplication template, entrypoint, application name, and resource profile.
- `config/spark/layer_spark_application.yaml`: shared SparkApplication template for standard layer jobs.
- `config/spark/resources.yaml`: shared JSON resource profiles for Spark driver/executor resources and infrastructure placeholders.
- `job/arguments.py`: parses `--partition_start`, `--partition_end`, and `--table_name`.
- `job/entities.py`: dataclass for runtime arguments.
- `job/getting_*.py`: main PySpark transformation and write logic.
- `entrypoints/*.py`: executable Spark entrypoint that creates `SparkSession`, parses args, calls `job.run`, and stops Spark.
- `migrations/create_table.sql`: Iceberg DDL with `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` for new tables. Add extra migration files for schema changes.
- `README.md`: Russian-language human summary of purpose, sources, grain, key formulas, and operational notes.

Trino/ClickHouse-source layer pipelines may use a separate Airflow/Python shape instead of SparkApplication. They should still live under `layers/<layer>/<primary_key_group>/<entity>/v1`, keep migrations and README alongside the code, read through explicitly confirmed Airflow connections, and write the output Iceberg table with `pyiceberg`. There may be no current examples in this repository; when generating one, keep the pattern explicit in README and do not reuse Spark-specific `spark`/`spark_applications` config unless Spark execution is actually used.

For Trino/ClickHouse-source pipelines, the same entity boundary applies to runtime code: each output table is materialized by code and a DAG in its own entity directory. A gold DAG must not execute the source queries or writes belonging to silver entities. Coordinate gold through silver dbt DQ sensors instead.

## Dataset Layout

Dataset pipelines generally follow the same table contract as layer pipelines, but live under `datasets/<team>/<domain>/<version>/`.

- `dag.py`: Airflow DAG definition using either the Spark Kubernetes pattern or the approved Airflow/Python pattern for Trino/ClickHouse-source jobs.
- `config.yaml`: table metadata used by DAG factories, CI, dbt source sync, maintenance sync, and local validation.
- `config/factory.py`: optional local factory when the entity needs templated runtime configuration.
- `job/`: transformation code that builds the training sample table, including label, split, sampling, and leakage-boundary logic.
- `entrypoints/`: executable Spark entrypoint when Spark execution is used.
- `migrations/create_table.sql`: Iceberg DDL with `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')` for new tables. Add extra migration files for schema changes.
- `README.md`: Russian-language human summary of dataset purpose, training/evaluation consumer, sources, grain, labels, windows, leakage boundaries, output table, exact DAG id, and operational notes.

Dataset directories must not contain ranking upload configuration. Dataset versions are path-level contracts; create a new version when changing sample semantics in a way that existing training consumers cannot safely treat as the same dataset.

Configuration constraints:

- Existing CI parsers read `config.yaml` with a simple nested key parser. Keep layer and dataset configs simple: nested mappings are fine, but avoid YAML anchors, complex lists, and non-obvious syntax unless the CI parser is updated.
- Local DAG factories that read `config.yaml` with a simple parser must not pass quoted scalar values through to Airflow or Spark. Either keep scalar config values unquoted when the syntax is safe, or strip matching surrounding quotes in the factory before using values such as `dag.schedule`, `dag.start_date`, table identifiers, or resource paths. Test parsed values when adding a new local factory; a value like `"2026-06-09T00:00:00Z"` must reach `pendulum.parse` as `2026-06-09T00:00:00Z`.
- Required table fields are `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, and `table.meta.team`.
- Repository-managed output tables must keep `table.catalog: iceberg`. Current dbt source sync reads `table.catalog`, `table.schema`, `table.name`, `table.primary_key`, and `table.meta.team`; current Iceberg maintenance sync reads Iceberg `table.schema` and `table.name`. Additional source/runtime config must not break these simple parsers.
- Primary keys should include `date` for daily/hourly feature and dataset tables unless there is a deliberate exception documented in README and code.

## PyIceberg Catalog And Identifier Contract

Use these rules for every Airflow/Python job that reads or writes Iceberg with `pyiceberg`:

- SQL/Trino/Spark names and PyIceberg catalog identifiers are different contracts. A fully qualified repository table is `<catalog>.<schema>.<name>`, for example `iceberg.silver.feature_platform_geo_geointellect_features`. After `load_catalog(<catalog>, ...)` has selected the catalog, pass the table to catalog APIs as the two-part tuple `(<schema>, <name>)`, for example `("silver", "feature_platform_geo_geointellect_features")`.
- Do not pass the catalog name to `catalog.load_table`, `catalog.table_exists`, or other table APIs. Do not pass a one-part table name. For Hive Catalog, require exactly one namespace component and one table component; hierarchical namespaces such as `("iceberg", "silver", "table")` are unsupported.
- Never derive a PyIceberg identifier with `split`, `rsplit`, prefix removal, or ad hoc string concatenation. In particular, converting `silver.table` to `table` drops the namespace and can produce misleading `NoSuchTableError` or `Invalid path, hierarchical namespaces are not supported` failures. Build `(config["table"]["schema"], config["table"]["name"])` directly from the owning `config.yaml`.
- Add one strict identifier helper per entity/runtime boundary if needed. It must validate that catalog, schema, and name are non-empty, verify that the configured catalog matches the loaded catalog, and return a two-element tuple. Fail before querying a source or writing data when the identifier is malformed.
- Migrations own table creation and schema evolution. A runtime job must not silently create a missing table. Before the first read/write, use `table_exists((schema, name))` or catch `NoSuchTableError` from `load_table`, then raise a diagnostic error containing the configured fully qualified name, catalog implementation, namespace, and the fact that migrations must have completed. Do not report every identifier-shape failure as a missing migration.
- Run a preflight for every configured input and output table before expensive source extraction. For a gold job, preflight all silver inputs and the gold output; for a dataset job, preflight all repository-managed source tables and the dataset output. This catches wrong namespaces, catalog wiring, and unapplied migrations before ClickHouse/Trino work begins.
- Keep catalog configuration consistent with the migration/Spark environment: catalog type, Hive Metastore URI, warehouse, object-store endpoint, region, path-style setting, and credentials source must refer to the same environment. Log non-secret catalog type/URI, warehouse, namespace, and table name; never log access keys or connection extras.
- Read and write through the schema returned by the loaded Iceberg table. Select and order Arrow columns explicitly, reject missing required columns and unexpected incompatible types, and preserve the table's field names. Do not use DataFrame column order as an implicit schema contract.
- For partition replacement, validate that the partition field exists and that every outgoing row has the intended partition value. Use an explicit Iceberg expression such as `EqualTo("date", partition_date)` and keep the overwrite idempotent. Do not broaden an overwrite filter to compensate for identifier or schema errors.
- Add unit tests for identifier construction and malformed inputs, including accidental values `table`, `schema.table`, and `catalog.schema.table`. Mock the catalog to assert that calls receive exactly `(schema, table)`. Where runtime dependencies are unavailable locally, these contract tests must still run without connecting to production.

## Deployment Standard

- Spark layer and dataset table jobs use shared default Spark image `ghcr.io/daymarket/spark:v3.5.5-scala2.12-java17-ubuntu-python3`.
- Job code is delivered through a `git-sync` initContainer.
- `mainApplicationFile` should point to `local:///git/repo/layers/.../entrypoints/*.py` or `local:///git/repo/datasets/.../entrypoints/*.py`.
- Standard Spark layer and dataset jobs should use `config/spark/layer_spark_application.yaml` and a named profile from `config/spark/resources.yaml`; choose the profile in the entity `config.yaml`.
- Do not create per-entity Spark resource files for standard jobs. Add a new shared named profile only after confirming the expected driver/executor resources with the user.
- Airflow variable `gitsync_branch` chooses the branch cloned into Spark pods.
- Do not build a new image for ordinary PySpark code, SQL, config, README, migration, or resource changes.
- Some older layer directories may contain inactive `Dockerfile`, `entrypoint.sh`, or `pyproject.toml`. Treat them as inactive unless the active SparkApplication template references them.
- Use a custom Spark image only for Python libraries, truststores, binaries, or runtime files not available in the default image and not deliverable by `git-sync`.
- If a custom image is required, add/update Dockerfile, pinned requirements, Drone tag trigger, documentation, and the DAG/runtime image reference. Internal packages must be installed from Nexus using Drone-provided build args; never commit credentials.
- Trino/ClickHouse-source Airflow/Python layer and dataset jobs should use `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` by default and write to Iceberg with `pyiceberg`. If required third-party libraries are missing from that image and cannot be delivered through repository code, ask the user to confirm a new or existing custom image before editing runtime files.
- Trino-source jobs should use `trino_search` for search-domain workloads or `trino_recsys` for recommendation-system workloads. Propose the context-appropriate connection; when the context is ambiguous, present both and use the user's final choice. ClickHouse-source jobs must ask for the exact Airflow connection id because RBAC is connection-specific.

## Custom Image Workflow

Use this workflow only after confirming that the default Spark image or default Airflow/Python image is insufficient for the job's runtime dependencies.

- Create or update the Dockerfile for the image that the job actually uses. For Trino/ClickHouse-source Airflow/Python layer or dataset jobs, base it on `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`; for Spark jobs, base it on the active Spark image unless a different base is explicitly approved.
- Add a nearby `requirements.txt` when Python packages are needed. Pin all package versions.
- If a package is internal, install it through Nexus using Drone-provided build args/secrets such as Nexus username/password. Do not commit credentials, tokens, or private index URLs containing secrets.
- Add or update a dedicated Drone image-build pipeline in `.drone.yaml`. The existing repository pipeline currently builds only the ranking upload custom image on tags matching `spark-feature-platform-ranking-upload-*`; new Airflow/Python or feature-specific images need their own registry repo, Dockerfile path, and tag trigger.
- Use a descriptive tag pattern for the new pipeline, for example `feature-platform-airflow-<entity>-*` for an Airflow/Python image or another approved project-specific prefix. Drone builds the image from the state of `master` at the pushed tag.
- Update the DAG/runtime config to reference the published image tag. For Spark jobs this may be the SparkApplication image; for Airflow/Python jobs it is the task/runtime image used by the DAG pattern.
- Document why the custom image exists in the entity README or relevant docs: missing library/binary/truststore, why the default image was not enough, and which image tag the DAG uses.
- Merge the code to `master`, then create and push one tag for the image build. Rebuild with a new tag only when Dockerfile or runtime dependencies change; ordinary job code, SQL, config, README, or migration changes should not require a new image.

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

Additional DQ tests may be proposed when they match the feature or dataset contract:

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
- Builds the ranking upload custom image only on tags matching `spark-feature-platform-ranking-upload-*`. Any new custom image needs an explicit additional Drone pipeline with its own tag trigger, Dockerfile path, registry repo, and required build secrets.

Trigger policy:

- The main Drone pipeline is triggered on pushes to `dev`/`master` and pull requests targeting `dev`/`master`.
- Pull requests run validation-only checks and must not apply real migrations, sync dbt source PRs, create/update Iceberg maintenance registration, or push Airflow submodule references.
- Push/merge to `dev` runs validation-only checks.
- Push/merge to `master` may run validation plus real side-effecting sync/apply steps.

Migration CI:

- Real migration execution reads Spark/Iceberg settings from `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `HIVE_METASTORE_URIS`, optional `ICEBERG_WAREHOUSE`, and S3/AWS region settings.
- Validation uses a disposable local Spark/Iceberg warehouse and does not require production Hive Metastore or S3 credentials.
- Migration discovery for repository-managed output tables must walk both `layers/**/config.yaml` and `datasets/**/config.yaml` with SQL files under `migrations/`. If current CI scripts only walk `layers/**/config.yaml`, update the scripts and tests before adding the first dataset table.
- `{target_table}` is substituted with the Spark table name from `config.yaml`.
- `create_table.sql` runs first, then remaining migrations in filename order.
- New `create_table.sql` migrations for repository-managed Iceberg tables must include `TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')`.
- Idempotency validation requires `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`; destructive `DROP`/`DELETE`/`TRUNCATE` statements are rejected.

dbt source sync:

- `ci_config.yaml` maps `iceberg` to `dwh-iceberg`.
- `scripts/sync_dbt_sources.py` must discover repository-managed layer and dataset configs and write source blocks to `models/ml_feature_platform/sources.yaml`. If it only discovers layer configs, update it before adding the first dataset table.
- It creates one source block per effective schema, named `ml_feature_platform_<schema>`, and removes stale table blocks from managed `ml_feature_platform_*` source blocks.
- Side-effecting dbt source sync runs only on `master` push.

Iceberg maintenance sync:

- `ml-feature-platform` owns maintenance registration only for Iceberg tables it creates from `layers/**/config.yaml` or `datasets/**/config.yaml`.
- Include repository-created `silver`, `gold`, and dataset tables; do not add upstream external dependency tables.
- Maintenance removals need manual review; do not remove entries automatically just because a table disappeared locally.
- Side-effecting maintenance sync runs only on `master` push.

## Ranking Feature Upload

Ranking upload lives in `upload/features_service_upload/v1`.

Configuration rules:

- Each feature group reads exactly one repository-managed `gold` source table.
- Dataset tables under `datasets/**` are not valid ranking upload sources and must not be published to inference services from this repository.
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

- Legacy jobs may still use `{{ ds }}` as the partition date being written. Some business logic intentionally uses data strictly before that date; inspect the job and README before assuming inclusion/exclusion.
- Trino table names such as `"dwh-iceberg".silver.table` map to Spark names such as `iceberg.silver.table`.
- Trino and ClickHouse can be source engines for new jobs, but final `silver`, `gold`, and dataset outputs remain Iceberg tables declared in `layers/**/config.yaml` or `datasets/**/config.yaml`.
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` support can differ by engine. Repository migrations run through PySpark, so validate syntax against Spark/Iceberg.
- Spark worker imports must be available on executors. If a UDF imports project code, configure executor `PYTHONPATH` and `git-sync` for executors.
- Do not coalesce null/zero denominators to `0.0` unless the feature or dataset contract says so.
- Query normalization is feature- or dataset-specific. Inspect the current job before changing lower/trim/space-collapse/tokenization behavior.
- Production ranking upload must not keep `source.limit`.
- Production ranking upload must not include dataset tables.
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

- Workflow for discovering, validating, creating, or publishing features and datasets.
- Feature/dataset/table deprecation and removal workflow.
- Repository structure or required files, including `layers/**` and `datasets/**`.
- MCP/source-contract policy.
- Deployment, CI, DQ, maintenance, or ranking-upload rules.
- Custom image policy or non-standard runtime dependency rules.

Do not add a section for each new table, feature, dataset, DAG, or upload group. Put feature- or dataset-specific facts in the owning README, migration, config, DAG, and job files. For agent-specific files such as `CLAUDE.md`, do not duplicate repository knowledge; point them to this `AGENTS.md`.
