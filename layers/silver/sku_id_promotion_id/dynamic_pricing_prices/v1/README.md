# iceberg.silver.feature_platform_dynamic_pricing_daily_prices

Дневной latest `dynamic_discount` на уровне SKU и promotion.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_dynamic_pricing_daily_prices`.
- DAG: `feature-platform.layers.silver.sku_id_promotion_id.dynamic_pricing_prices` (`layers/silver/sku_id_promotion_id/dynamic_pricing_prices/v1/dag.py`).
- Групповой тег Airflow: `dynamic-pricing-prices`.
- Расписание: ежедневно в 01:00 UTC, `0 1 * * *`.
- `start_date=2026-06-29T00:00:00Z`, `catchup=False`.
- DAG не ждет solution DAG; эта зависимость находится в gold-витрине.

## Грейн / ключ

`date, sku_id, promotion_id`.

`date` - закрытый UTC-день, равный `data_interval_end - 1 day`.

## Источники

- `promotions.public.dynamic_discount` - расчеты динамической скидки за закрытый UTC-день.

Trino-запрос выбирает все `promotion_id` с префиксом `dyno_pricing_`. Список моделей в
`config.yaml` не поддерживается.

## Логика

Для каждой пары `sku_id, promotion_id` берется последняя запись за `date` по `created_at DESC`.
Фильтр `starts_with(promotion_id, 'dyno_pricing_')` применяется в исходном Trino-запросе.
Фильтр окна строится от Airflow interval, а не от физического `now()`:
`created_at >= date 00:00:00 UTC` и `created_at < date + 1 day 00:00:00 UTC`.

В silver нет join с `kazanexpress.public.sku` и нет расчета финальной цены. Эти операции выполняются
в gold-витрине, чтобы raw Trino-скан оставался дневным и небольшим.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение выполняется через connection
`trino_search`, запись - через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Запись идемпотентна: партиция `date` перезаписывается целиком через PyIceberg `overwrite`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
