# Дневная когорта продаж SKU

Выход: `iceberg.silver.feature_platform_demand_sales_daily`.
Путь: `layers/silver/sku_id/demand_sales_daily/v1`, ключ `(date, sku_id)`.
DAG: `feature-platform.layers.silver.sku_id.demand_sales_daily`.
Группа DAG: `demand-forecast`. Ranking upload и модельных X/y нет.

## Источник и семантика

Единственный вход — `iceberg.silver.feature_platform_demand_seller_sales_observed_daily`
(владелец `feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily`),
читается через Trino `trino_search`. Каждый день — один `GROUP BY date, sku_id`
(`job/query.py`):

- units, каналы и пять денежных сумм складываются; NULL хотя бы одного seller-вклада
  даёт NULL итога;
- `sales_orders`/`sales_order_items` — точные уники SKU-дня из seller-silver
  (`sku_sales_*`), а не сумма seller-уников;
- курс и время его фиксации одинаковы для всех строк дня;
- `source_updated_at` — максимум по продавцам.

## Оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`, окно — последние
`runtime.refresh_days` = 7 завершённых дней.
Штатный запуск ждёт `dq` seller-продаж (`execution_delta` 0 — то же расписание).
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

## DQ и feature_stats

`write` возвращает список записанных дат. Таски `dq` и `feature_stats` получают его
через `partition_date_template` (`… | join(",")`), проверяют каждую дату и сохраняют
результаты одним commit'ом. `feature_stats` — отдельный скан партиции в Trino
(`trino_search`) на каждую дату окна; `query_timeout_seconds` ограничивает всю таску.
