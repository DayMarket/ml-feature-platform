# Dynamic pricing inference upload

Публикует агрегаты dynamic-pricing цен и скидок в сервис инференса.

## Оркестрация

- DAG: `feature-platform.upload.dynamic_pricing_inference_upload`.
- Расписание: каждые 3 часа, `0 */3 * * *` UTC.
- `start_date=2026-06-29T00:00:00+00:00`, `catchup=False`.
- Сенсор: `dbt.source.trino.ml_feature_platform_gold.feature_platform_dynamic_pricing_sku_group_price_features.dq`.

## Источник

- `iceberg.gold.feature_platform_dynamic_pricing_sku_group_price_features`.
- Режим чтения: после успешного DQ берется максимальный `calculated_at` из таблицы.

## Kafka

- Connection: `kafka_ranking`.
- Topic: `ranking.features.updates`.
- Feature set: `fs_dynamic_pricing_skg_promotion_price_features_v1`.
- Каталог: `SKU_GROUP_TO_PROMOTION`, ключ `sku_group_id, promotion_id`.

## Фичи

Публикуются `min`, `max`, `avg` для `sell_price`, `discount` и `discount_fraction`.
