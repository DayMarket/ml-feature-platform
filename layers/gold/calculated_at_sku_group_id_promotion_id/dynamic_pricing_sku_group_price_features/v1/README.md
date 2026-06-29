# iceberg.gold.feature_platform_dynamic_pricing_sku_group_price_features

Агрегаты dynamic-pricing цен и скидок по SKU group и promotion.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_dynamic_pricing_sku_group_price_features`.
- DAG: `feature-platform.layers.gold.calculated_at_sku_group_id_promotion_id.dynamic_pricing_sku_group_price_features` (`layers/gold/calculated_at_sku_group_id_promotion_id/dynamic_pricing_sku_group_price_features/v1/dag.py`).
- Групповой тег Airflow: `dynamic-pricing-prices`.
- Расписание: каждые 3 часа, `0 */3 * * *` UTC.
- `start_date=2026-06-29T00:00:00Z`, `catchup=False`.

## Грейн / ключ

`calculated_at, sku_group_id, promotion_id`.

## Источник

- `iceberg.gold.feature_platform_dynamic_pricing_price_features` - SKU-level dynamic-pricing цены и скидки.

## Логика

Для текущего `calculated_at` таблица агрегирует `sell_price`, `discount` и `discount_fraction` по
`sku_group_id, promotion_id`: `min`, `max`, `avg`.

## Зависимости

- `feature-platform.layers.gold.calculated_at_sku_id_promotion_id.dynamic_pricing_price_features`.

## Рантайм

Trino/Iceberg-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Trino connection:
`trino_search`. Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Запись идемпотентна: snapshot с точным `calculated_at` перезаписывается целиком через PyIceberg
`overwrite` по фильтру `calculated_at`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
