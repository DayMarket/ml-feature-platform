# Silver дневного наличия и цен SKU

Выход: `iceberg.silver.feature_platform_demand_stock_daily`.
Путь: `layers/silver/sku_id/demand_stock_daily/v1`.
Грейн и ключ: `(date, sku_id)`, identity partition по `date`.
DAG: `feature-platform.layers.silver.sku_id.demand_stock_daily`.
Группа DAG: `demand-forecast`.

## Источник и семантика

ClickHouse `marts.daily_sku_quantity_eod FINAL` через `clickhouse_dwh_team_logistics`,
`dt` сохраняется без сдвига. Строка пишется только при
`quantity_active_eod > 0 OR quantity_fbs_eod > 0`, поэтому таблица хранит множество
доступных SKU, а не уровни запасов:

- строка `(date, sku_id)` — положительный доступный EOD-остаток;
- отсутствие строки в записанной и прошедшей DQ партиции — ноль;
- отсутствующая или не прошедшая DQ партиция — unknown, а не OOS.

Для каждой строки пишутся цены на конец дня из того же источника (UZS, `BIGINT`):

| Колонка | Источник | Смысл |
|---|---|---|
| `purchase_price_eod` | `purchase_price_eod` | цена покупки для покупателя (тот же смысл, что `order_item_purchase_price` в GMV) |
| `sell_price_eod` | `sell_price_eod` | цена продавца из карточки |
| `full_price_eod` | `full_price_eod` | цена до скидки (зачёркнутая) |

Источник — снимок `public.sku` с протяжкой (последнее значение за `dt`, иначе вчерашнее),
внутридневные изменения не видны. `0` в источнике (история до миграции 183, где цены
ещё не заполнялись) пишется как `NULL`. Цены есть только у SKU с положительным остатком:
для дней без остатка строки нет. Колонки добавлены миграцией
`migrations/20260917_add_eod_prices.sql`; партиции, записанные раньше, содержат `NULL`
до перезаливки истории ручным запуском.

`source_manifest_id` — `run_id` Airflow-запуска, `source_contract_version` — версия
контракта из конфига (`source_eod_positive_availability_prices_v2`), `ingested_at` — время захвата UTC. Это не модельные признаки.

## Оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`. Штатный запуск перезаписывает последние
`runtime.refresh_days` = 7 завершённых UTC-дней: `[run_after − 7 дней, run_after − 1 день]`.

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
