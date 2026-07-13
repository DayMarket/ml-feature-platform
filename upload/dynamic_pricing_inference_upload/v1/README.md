# Dynamic pricing inference upload

Публикует агрегаты dynamic-pricing цен и скидок в сервис инференса.

## Оркестрация

- DAG: `feature-platform.upload.dynamic_pricing_inference_upload`.
- Расписание: каждые 3 часа, `0 */3 * * *` UTC.
- `start_date=2026-07-09T00:00:00+00:00`, `catchup=False`.
- Сенсор: `feature-platform.layers.gold.calculated_at_sku_group_id_promotion_id.dynamic_pricing_sku_group_price_features`.

## Источник

- `iceberg.gold.feature_platform_dynamic_pricing_sku_group_price_features`.
- Режим чтения: после успешного producer DAG берется максимальный `calculated_at` из таблицы.

## Kafka

- Connection: `kafka_ranking`.
- Topic: `ranking.features.updates`.
- Feature sets: `fs_dynamic_pricing_skg_promotion_price_features_v1`,
  `fs_dynamic_pricing_skg_promotion_price_inference_v1`.
- Каталог: `SKU_GROUP_TO_PROMO`, ключ `sku_group_id, promotion_id`.

## Фичи

Основная группа публикует `avg_sell_price`, а также `min`, `max`, `avg` для `discount`
и `discount_fraction`. Отдельная inference-группа публикует только `avg_sell_price`
для формулы.
