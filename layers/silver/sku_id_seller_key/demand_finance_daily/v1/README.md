# Финансовые события SKU и продавца

Выход: `iceberg.silver.feature_platform_demand_finance_daily`.
Путь: `layers/silver/sku_id_seller_key/demand_finance_daily/v1`.
Ключ: `(date, sku_id, seller_key)`; `seller_key` = `seller:<id>` или `unknown`,
`seller_id` nullable. Продавец берётся из финансового факта, не из текущего каталога.
DAG: `feature-platform.layers.silver.sku_id_seller_key.demand_finance_daily`.
Группа DAG: `demand-forecast`.

## Источник и поля

ClickHouse `marts_b2c.finance_margin_by_order_item` через
`clickhouse_dwh_team_logistics`, ровно `dt` финансового события, без cohort-фильтра.
Источник — ReplicatedMergeTree: `FINAL` не применяется, повтор `order_item_id` в разных
событиях не является дублем.

28 мер: generated/assembled/delivered/completed/returned/net, возвраты текущих и
предыдущих периодов, суммы без промо, скидки. Денежные суммы — `DECIMAL(38,0)` в
исходном масштабе (signed), единицы — `BIGINT`; для 20 денежных мер есть USD по курсу
дня (как в seller-продажах). `source_rows` — число исходных строк группы.
Для join с SKU-гранулярностью сначала сверните продавцов.

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
(`trino_search`) на каждую дату окна; `query_timeout_seconds` ограничивает всю таску.
