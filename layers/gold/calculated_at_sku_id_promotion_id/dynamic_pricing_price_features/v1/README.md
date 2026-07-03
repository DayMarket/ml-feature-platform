# iceberg.gold.feature_platform_dynamic_pricing_price_features

Финальные цены SKU с учетом dynamic pricing.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_dynamic_pricing_price_features`.
- DAG: `feature-platform.layers.gold.calculated_at_sku_id_promotion_id.dynamic_pricing_price_features` (`layers/gold/calculated_at_sku_id_promotion_id/dynamic_pricing_price_features/v1/dag.py`).
- Групповой тег Airflow: `dynamic-pricing-prices`.
- Расписание: каждые 3 часа, `0 */3 * * *` UTC.
- `start_date=2026-06-29T00:00:00Z`, `catchup=False`.

## Грейн / ключ

`calculated_at, sku_id, promotion_id`.

`calculated_at` - timestamp расчета из `data_interval_end` в UTC.

## Источники

- `iceberg.silver.feature_platform_dynamic_pricing_daily_prices` - закрытые дневные latest dynamic_discount snapshots.
- `promotions.public.dynamic_discount` - today's raw dynamic_discount за текущий UTC-день.
- `kazanexpress.public.sku` - текущие SKU, `sku_group_id`, `product_id` и текущая цена продажи.

## Зависимости

- `merge_center_solution_to_kafka_gurobi_mvp_dag`;
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_dynamic_pricing_daily_prices.dq`.

## Логика

Gold выполняет один Trino-запрос: берет actual dynamic_discount за последний день из
`promotions.public.dynamic_discount`, добавляет предыдущие `history_days - 1` дней из
silver/Iceberg через `UNION ALL`, выбирает последнюю запись по `sku_id, promotion_id` через
`created_at DESC`, разворачивает текущую таблицу SKU на все `promotion_id` из config и дефолтный
`promotion_id = '0'`, затем считает финальные цены.

Дефолтный `promotion_id = '0'` нужен для сбора цен пользователей без `account_id`. Для этого
среза `dynamic_discount` и silver lookup не читаются, поэтому итоговая цена равна текущему
`sell_price`, `discount = 0`, а `calculated_for_price` и `dynamic_discount_created_at` равны `NULL`.

Если текущий `sell_price` SKU равен `calculated_for_price` из dynamic_discount, скидка считается
примененной: `sell_price = sell_price - discount_amount`, `discount = discount_amount`. Иначе
итоговая цена остается равной текущему `sell_price`, а `discount = 0`. `discount_fraction` считается
как `discount / sell_price` и равен `NULL` при нулевой итоговой цене.

## Рантайм

Trino + Iceberg-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Trino connection:
`trino_search`. Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Запись идет по одному `promotion_id`: для каждой модели и дефолтного `0` DAG читает свой срез и
перезаписывает snapshot `(calculated_at, promotion_id)` через PyIceberg `overwrite`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
