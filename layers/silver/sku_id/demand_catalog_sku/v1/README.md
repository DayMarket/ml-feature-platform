# Текущий SKU-каталог

Выход: `iceberg.silver.feature_platform_demand_catalog_sku`, ключ `(date, sku_id)`.
Путь: `layers/silver/sku_id/demand_catalog_sku/v1`.
DAG: `feature-platform.layers.silver.sku_id.demand_catalog_sku`.
Группа DAG: `demand-forecast`.

## Источники

ClickHouse через `clickhouse_dwh_team_logistics` (`job/query.py`):

- `dict.sku` — SKU, карточка, категория, seller/shop, статус, создание (UTC);
- `dict.category` — raw L1–L6 и названия;
- `matching.mdm_golden_sku` — текущее состояние merge на golden UUID
  (`argMax(tuple(is_merged, merged_into), updated_at)`);
- `matching.mdm_golden_meta_links` + словарь `matching.meta_sku_id_dict` — активные
  связи Uzum SKU → golden (orphan-связи без meta отбрасываются).

Master/`is_1p`/регистрация продавца — из текущего snapshot
`iceberg.silver.feature_platform_demand_catalog_seller`; его id пишется в
`catalog_seller_snapshot_id`, `catalog_version` наследуется.

## Семантика

- Путь: market → L1 → L2..L5 (нулевой уровень наследует предыдущий) → `leaf:<category_id>`.
  Без L1 — `missing` и пустой путь. Узел с несколькими родителями делает все пути через
  него `conflict` (запрещено DQ). Raw L1–L6 сохраняются.
- Golden: merge-цепочки разрешаются до конечного golden (pointer jumping в numpy).
  Один конечный golden → `matched`, `unit_id = g:<uuid>`; цикл или несколько конечных →
  `conflict`, `unit_id = s:<sku_id>`; связь в отсутствующий golden → `unavailable`
  (запрещено DQ); связи нет → `unmatched`, `unit_id = s:<sku_id>`.
  Отсутствующая цель merge в графе блокирует запись.
- Seller: `seller_id`, которого нет в seller-snapshot, → `unavailable` (DQ warning).

Все соединения — колоночные (`index_in`/`take` в Arrow), без построчной обработки.
Pod: 2 CPU / 32 GiB (≈10,5 млн SKU на 2026-09-09).

## Запись и оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`.
Штатный запуск ждёт `dq` seller-каталога (`execution_delta` 0).
Ручной запуск сенсоры пропускает (`upstream_gate`) и читает текущие данные upstream.

Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.
