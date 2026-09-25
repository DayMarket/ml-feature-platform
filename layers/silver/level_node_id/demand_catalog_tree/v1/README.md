# Дерево текущего SKU-каталога

Выход: `iceberg.silver.feature_platform_demand_catalog_tree`.
Путь: `layers/silver/level_node_id/demand_catalog_tree/v1`, ключ `(date, level, node_id)`.
DAG: `feature-platform.layers.silver.level_node_id.demand_catalog_tree`.
Группа DAG: `demand-forecast`.

## Вход и семантика

Единственный вход — текущий snapshot `iceberg.silver.feature_platform_demand_catalog_sku`
(id пишется в `catalog_sku_snapshot_id`), один запрос в Trino `trino_search`:
уникальные рёбра market → l1 → l2 → l3 → l4 → l5 → leaf из SKU с
`category_path_status = 'valid'`.

- `node_id` — `market` или `<level>:<category_id>`, `parent_id` у market NULL;
- `level_code`: market=0 … leaf=6;
- `is_passthrough` — узел L2+ повторяет категорию непосредственного родителя;
- `date` и `catalog_version` наследуются из SKU-каталога.

Узел с несколькими родителями даёт повтор ключа и отклоняется DQ.

## Запись и оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`.
Штатный запуск ждёт `dq` SKU-каталога (`execution_delta` 0).
Ручной запуск сенсоры пропускает (`upstream_gate`) и читает текущие данные upstream.

Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.
