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

Дефолтный `promotion_id = '0'` приходит из SKU-level источника как baseline-срез без dynamic
discount: `discount = 0`, цена равна текущему `sell_price`.

## Зависимости

- `feature-platform.layers.gold.calculated_at_sku_id_promotion_id.dynamic_pricing_price_features`.

## Рантайм

Spark/Iceberg пайплайн на shared Spark image с `git-sync`. Resource profile: `large`.

Spark читает `iceberg.gold.feature_platform_dynamic_pricing_price_features`, фильтрует точный
`calculated_at` из `data_interval_end`, агрегирует признаки и перезаписывает snapshot с этим
`calculated_at` через Iceberg writer.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
