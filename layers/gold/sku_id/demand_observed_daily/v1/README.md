# Gold дневной observed-панели SKU

Выход: `iceberg.gold.feature_platform_demand_observed_daily`.
Путь: `layers/gold/sku_id/demand_observed_daily/v1`.
Ключ: `(date, sku_id)`, identity partition по `date`.
DAG: `feature-platform.layers.gold.sku_id.demand_observed_daily`.
Группа DAG: `demand-forecast`.

## Источники и семантика

- `iceberg.silver.feature_platform_demand_sales_daily` — продажи по дате создания заказа;
- `iceberg.silver.feature_platform_demand_stock_daily` — SKU с положительным EOD.

Каждый день — один `FULL OUTER JOIN` по `sku_id` в Trino (`trino_search`), без
построения `catalog × days`. Поля продаж сохраняются как есть, технические поля sales
получают префикс `sales_`. `sales_component_present` — есть sales-строка,
`is_in_stock_eod` — есть stock-строка. Для stock-only SKU поля продаж NULL.

Оба входа читаются на snapshot, зафиксированном в начале запуска
(`FOR VERSION AS OF`); его id и UUID таблиц пишутся в `*_snapshot_id`/`*_table_uuid`.
Если за день нет строк sales или stock, день не пишется: пустая stock-партиция означала
бы «всё не в наличии».

Панель не содержит уровней запасов, цен, seller/статусов, календаря и окон.

## Оркестрация

Ежедневно в `05:00 UTC`, `max_active_runs=1`, окно — последние
`runtime.refresh_days` = 7 завершённых дней.
Штатный запуск ждёт `dq` sales и stock (оба в `04:00 UTC`, `execution_delta` 1 час).
Ручной запуск сенсоры пропускает (`upstream_gate`) и читает текущие данные upstream.

Ручной запуск («Trigger DAG w/ config») принимает только интервал дат, обе границы
включительно:

```json
{"start": "2026-08-01", "end": "2026-08-31"}
```

Пустые `start`/`end` означают штатное окно; `end` должен быть раньше текущего UTC-дня.
Каждый день перезаписывается отдельным атомарным `overwrite` своей партиции `date`;
день, для которого источник вернул 0 строк, не перезаписывается и роняет задачу.
Длинную историю удобно грузить помесячными запусками.

Заполнение истории: сначала seller-продажи и stock за интервал, затем SKU-продажи,
затем observed — с одними и теми же `start`/`end`.

## DQ и feature_stats

`write` возвращает список записанных дат. Таски `dq` и `feature_stats` получают его
через `partition_date_template` (`… | join(",")`), проверяют каждую дату и сохраняют
результаты одним commit'ом. `feature_stats` — отдельный скан партиции в Trino
(`trino_search`) на каждую дату окна; `query_timeout_seconds` ограничивает всю таску.
