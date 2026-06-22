# Gold Pairwise Features Query/SKU Group Legacy

DAG id: `feature-platform.layers.gold.query_sku_group_id.query_skg_pairwise_features_legacy`.

Пайплайн восстанавливает legacy pairwise-слой для `fs_search_query_skg_v3`.

Целевая таблица: `iceberg.gold.feature_platform_query_skg_pairwise_features_legacy`.

Источник: `iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy`.

Зерно: `date`, `query`, `sku_group_id`.

Основная логика:

- читает aggregated-таблицу за период от `{{ ds }} - 30 days` до `{{ ds }}` включительно;
- для каждой пары `query`, `sku_group_id` сортирует строки по `date desc`;
- оставляет последнюю доступную строку;
- записывает ее в партицию `date = {{ ds }}`;
- сохраняет полный набор legacy pairwise-колонок, а ranking upload публикует из них только `fs_search_query_skg_v3`.

Эта таблица является новым источником группы `fs_search_query_skg_v3` в `upload/ranking_features/v1/config.yaml`.

