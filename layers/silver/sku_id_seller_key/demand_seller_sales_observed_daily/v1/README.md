# Дневные продажи SKU × продавец

Выход: `iceberg.silver.feature_platform_demand_seller_sales_observed_daily`.
Путь: `layers/silver/sku_id_seller_key/demand_seller_sales_observed_daily/v1`.
Ключ: `(date, sku_id, seller_key)`, identity partition по `date`.
DAG: `feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily`.
Группа DAG: `demand-forecast`.

## Источник и поля

ClickHouse `marts.order_items FINAL` через `clickhouse_dwh_team_logistics`, один
запрос на день (`job/query.py`):

- когорта создания заказа в Asia/Tashkent (полуоткрытые UTC-границы дня),
  `order_item_status NOT IN ('CREATED', 'NOT_CREATED')`, `sku_id > 0`;
- продавец из позиции заказа, без текущего каталога: положительный `seller_id` →
  `seller:<id>`, исходный 0/NULL → `unknown` (`seller_id` NULL);
- количества, уники заказов/позиций, пять денежных сумм в исходном масштабе
  `DECIMAL(38,0)` и разбивка FBO/FBS/DBS/other/unknown;
- `GROUPING SETS` считает также итог SKU-дня: точные `sku_sales_orders` и
  `sku_sales_order_items` повторяются во всех seller-строках SKU и **не суммируются**;
- USD = сумма / курс дня. Курс — точный из `dict.currency_rates`, иначе последний
  официальный (`fx_rate_source = latest_available`); без курса USD остаётся NULL
  (`unavailable`).

Определения колонок — в `migrations/create_table.sql`.

## Оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`. Штатный запуск перезаписывает последние
`runtime.refresh_days` = 7 завершённых дней.

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
(`trino_search`) на каждую дату окна; `query_timeout_seconds` ограничивает всю таску. Отрицательный `seller_id` ловится DQ `accepted_range`.
