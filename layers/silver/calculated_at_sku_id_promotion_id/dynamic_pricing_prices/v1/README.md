# iceberg.silver.feature_platform_dynamic_pricing_prices

Snapshot цен SKU с учетом динамического ценообразования.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_dynamic_pricing_prices`.
- DAG: `feature-platform.layers.silver.calculated_at_sku_id_promotion_id.dynamic_pricing_prices` (`layers/silver/calculated_at_sku_id_promotion_id/dynamic_pricing_prices/v1/dag.py`).
- Групповой тег Airflow: `dynamic-pricing-prices`.
- Расписание: каждые 3 часа, `0 */3 * * *` UTC.
- `start_date=2026-06-26T00:00:00Z`, `catchup=False`.
- DAG ждет `merge_center_solution_to_kafka_gurobi_mvp_dag` с тем же logical date.

## Грейн / ключ

`calculated_at, sku_id, promotion_id`.

`calculated_at` - timestamp расчета из `data_interval_end` в UTC. В этой таблице нет отдельной
дневной snapshot-колонки, потому что пайплайн запускается каждые 3 часа и для потребителей важен
timestamp расчета.

## Источники

- `promotions.public.dynamic_discount` - последние расчеты динамической скидки за 14 дней.
- `kazanexpress.public.sku` - текущие SKU, `sku_group_id`, `product_id` и текущая цена продажи.

Список `promotion_id` хранится в `config.yaml` (`source.promotion_ids`):

- `model_3008_sku_ext_no_filter_budget_34_with_cb_v2_sku_card_0526`;
- `model_3008_sku_ext_internal_matching_soft_budget_29_with_cb_v2_sku_card_internal_matching_soft_0526`;
- `model_3008_sku_ext_internal_matching_hard_budget_29_with_cb_v2_sku_card_internal_matching_hard_0526`.

## Логика

Для каждой пары `sku_id, promotion_id` берется последняя запись из
`promotions.public.dynamic_discount` за последние 14 дней по `created_at DESC`. Все SKU из
`kazanexpress.public.sku` разворачиваются на все `promotion_id` из config, чтобы ключ таблицы был
заполнен для каждой строки.

Если текущий `sell_price` SKU равен `calculated_for_price` из dynamic_discount, скидка считается
примененной: `sell_price = sell_price - discount_amount`, `discount = discount_amount`. Иначе
итоговая цена остается равной текущему `sell_price`, а `discount = 0`. `discount_fraction` считается
как `discount / sell_price` и равен `NULL` при нулевой итоговой цене.

Колонка `dynamic_discount_created_at` хранит timestamp последней записи dynamic_discount,
использованной в расчете. Для SKU без записи dynamic_discount по promotion_id колонка будет `NULL`.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение выполняется через connection
`trino_search`, запись - через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Запись идемпотентна: snapshot с точным `calculated_at` перезаписывается целиком через PyIceberg
`overwrite` по фильтру `calculated_at`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
