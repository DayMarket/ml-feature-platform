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

## DQ

DQ-тесты выполняются таской `dq` внутри этого DAG'а сразу после записи снапшота;
каталог тестов и правила конфигурирования описаны в `dq/README.md`.

Энтити снапшотная, а не дневная: DAG идёт раз в 3 часа и пишет отдельный
`calculated_at`, а не дописывает дневную партицию. Поэтому в блоке `dq:` стоит
`partition_granularity: timestamp`, `partition_column: calculated_at` и
`snapshot_interval_hours: 3`, а `partition_date_template` отдаёт `data_interval_end`
со временем — ровно тот момент, который джоб пишет в `calculated_at`. Проверять
дневную партицию было бы неверно: на момент запуска она дописана лишь частично.

Базовый набор: `primary_key_not_null`, `primary_key_unique`, `row_count_min`,
`row_count_growth`, `freshness`. Все они сравнивают снапшот с предыдущим снапшотом
(минус `snapshot_interval_hours`), а не с прошлыми сутками.

Пороги подобраны по истории таблицы в Trino:

- `row_count_min: 12000000` — снапшот стабильно весит 15.1M-20.7M строк;
- `row_count_growth: 0.5` — между соседними снапшотами объём гуляет в
  -25.0%..+33.4% (141 пара за июль): промо появляются и заканчиваются внутри суток,
  поэтому порог остаётся детектором обвала, а не датчиком промо-активности;
- `freshness: max_lag_days: 1` — у снапшотной энтити лаг меряется в часах, то есть
  24 часа или 8 пропущенных запусков подряд. Дефолтные 2 дня для трёхчасового DAG'а
  слишком мягки.

Дополнительно `not_null` и `non_negative` по всем 9 фичевым колонкам: за 33 снапшота
(около 515M строк) ни одного NULL и ни одного отрицательного значения.

Аплоад dynamic-pricing ждёт таску `dq` этого DAG'а, а не весь DAG и не dbt-DQ-DAG.
