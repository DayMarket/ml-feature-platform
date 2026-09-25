# Полный текущий каталог продавцов

Выход: `iceberg.silver.feature_platform_demand_catalog_seller`.
Путь: `layers/silver/seller_id/demand_catalog_seller/v1`, ключ `(date, seller_id)`.
DAG: `feature-platform.layers.silver.seller_id.demand_catalog_seller`.
Группа DAG: `demand-forecast`.

## Источник и поля

Весь ClickHouse `marts.sellers_info` через `clickhouse_dwh_team_logistics`, без фильтров
по SKU или активности. Контакты и реквизиты не читаются.

- `source_master_seller_id` — исходный master (NULL и пустая строка различаются);
- непустой master → `matched`, `master_seller_id` = master без пробелов;
- пустой master → `unmatched`, `master_seller_id` = `seller_id` строкой;
- NULL → `unavailable` (запрещён DQ), `master_seller_id`/`has_master` NULL;
- `seller_registered_at` — регистрация в UTC; `is_1p` NULL не становится FALSE.

Совпадение fallback `seller_id` с реальным master блокирует запись.
`date` — дата захвата в Asia/Tashkent, `catalog_version = catalog:<run_id>`;
SKU-каталог и дерево наследуют эту версию.

## Запись и оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`.
Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.
