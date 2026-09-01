# product_prices_daily

Дневные price-факты товара. Исторические цены относятся к `dt`, а доступность SKU
определяется по текущему состоянию на момент выполнения job.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_product_prices_daily`.
- DAG: `feature-platform.layers.silver.product_id.product_prices_daily`.
- Путь: `layers/silver/product_id/product_prices_daily/v1`.
- Групповой тег Airflow: `recsys-features`.
- Расписание: ежедневно в 19:00 UTC (`0 19 * * *`), то есть в 00:00 `Asia/Tashkent`.
- `dt` — `TIMESTAMP` начала предыдущей календарной даты (`00:00:00 Asia/Tashkent`) относительно `data_interval_end`.
- `start_date=2026-08-08T19:00:00Z`, `catchup=True`. При включении 23 августа 2026 года первый запуск рассчитывает `dt=2026-08-09`, а initial backfill покрывает 14 завершённых EOD-дат с 9 по 22 августа.

Перед материализацией `ExternalTaskSensor` ожидает успешный
`dbt.tests.dbt_clickhouse_dwh.daily_sku_quantity_eod.dq`, в котором проверяются актуальные
цены источника. DQ DAG запускается ежедневно в `06:00 UTC`, поэтому S3 с расписанием
`19:00 UTC` использует `execution_delta=13 часов` и ждёт соответствующий запуск того же дня.
Пустой срез дополнительно останавливает задачу до записи.

## Грейн и ключ

Грейн и уникальный ключ: `dt, product_id`.

SKU и SKU-group используются только внутри расчета и не публикуются.

Ценовые колонки:

- `min_sell_price_eod`, `avg_sell_price_eod`, `max_sell_price_eod`;
- `min_full_price_eod`, `max_full_price_eod`;
- `min_active_sku_sell_price_eod`, `avg_active_sku_sell_price_eod`,
  `max_active_sku_sell_price_eod`.

## Источники

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-clickhouse".marts.daily_sku_quantity_eod` — `full_price_eod` и
  `sell_price_eod` за `dt`;
- `"dwh-clickhouse".dict.sku` — mapping `sku_id → sku_group_id → product_id` и текущее
  состояние `status`, `quantity_active`, `quantity_fbs`.

В исходной спецификации mapping был выделен в `"dwh-iceberg".silver.sku`. Обе SKU-таблицы
содержат нужные mapping-колонки. Проверка актуального EOD-среза показала одинаковое покрытие и
отсутствие расхождений `product_id`/`sku_group_id`, поэтому job использует один оперативный
ClickHouse dict и не выполняет избыточный cross-catalog join. Это наблюдаемое состояние, а не
гарантия вечной идентичности источников.

Строки EOD без mapping в `dict.sku` не входят в результат. Количество исходных и
сопоставленных SKU/product, а также расхождения EOD `product_id` с current mapping логируются
для каждого запуска. Для расчета используется `product_id` из `dict.sku`, как предусмотрено
отдельным mapping-шагом контракта.

## Расчет

Из EOD-источника выбираются строки, у которых исходная `dt` равна целевой дате расчёта.
Целевая `dt` вычисляется в `Asia/Tashkent` до построения обоих Trino-запросов и сохраняется
как `TIMESTAMP` локальной полуночи. Для фильтра источника используется только календарная
часть `dt`, поскольку `daily_sku_quantity_eod.dt` имеет тип `DATE`.

Текущая доступность SKU:

```sql
status = 'ACTIVE'
AND (
    COALESCE(quantity_active, 0) > 0
    OR COALESCE(quantity_fbs, 0) > 0
)
```

Сначала цены агрегируются по `dt × product_id × sku_group_id`, затем по
`dt × product_id`:

- `min_sell_price_eod = MIN(sku_group.min_sell_price_eod)`;
- `avg_sell_price_eod = AVG(sku_group.avg_sell_price_eod)`;
- `max_sell_price_eod = MAX(sku_group.max_sell_price_eod)`;
- `min_full_price_eod = MIN(sku_group.min_full_price_eod)`;
- `max_full_price_eod = MAX(sku_group.max_full_price_eod)`;
- active-price колонки используют такую же двухэтапную агрегацию.

Для SKU, отсутствующего в dict или не удовлетворяющего условию доступности, цена не входит в
active-price агрегаты. Если у товара нет доступных SKU, все три active-price колонки равны
`NULL`. Нули вместо `NULL` не подставляются.

Историческая цена является point-in-time относительно `dt`, текущая доступность — нет.
При backfill также используется current-state dict на фактический момент запуска.

## Проверки качества

Перед записью проверяются:

- непустой source-срез и coverage mapping по SKU/product;
- уникальность `dt, product_id`;
- `product_id > 0`;
- неотрицательность всех заполненных цен;
- `min_sell_price_eod <= avg_sell_price_eod <= max_sell_price_eod`;
- `min_full_price_eod <= max_full_price_eod`;
- согласованность active-price колонок и условие `min <= avg <= max`;
- отсутствие дат, отличных от целевой `dt`.

До агрегации на уровень SKU-group `full_price_eod` и `sell_price_eod` фильтруются независимо.
Значение участвует в расчете, если оно находится в диапазоне от `0` до `1_000_000_000`
включительно. Цена `0` считается валидной и сохраняется в Silver как `0`; фильтра
`price != 0` нет. Отрицательные цены и значения выше порога заменяются на `NULL` и не
участвуют в соответствующих `AVG`, `MIN` и `MAX`.

Некорректный `full_price_eod` не исключает корректный `sell_price_eod`, и наоборот.
Active-price агрегаты используют только валидный `sell_price_eod` доступных SKU. Если после
фильтрации для метрики не осталось валидных цен, результат равен `NULL`; ноль вместо `NULL` не
подставляется. Отдельные алерты и падение DAG из-за отфильтрованных цен не предусмотрены.

После merge в master стандартная синхронизация создаст dbt DQ с уникальностью ключа и
`not_null` для ключевых колонок.

## Рантайм и потребители

Airflow/Python + `pyiceberg`, не Spark, поэтому Spark-параметр `resource_profile` здесь не
применяется. Задача использует компактный pod с `1 CPU` и `4 GiB` памяти. Образ:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`. Партиция `dt` идемпотентно
перезаписывается через PyIceberg `overwrite`.

Потребители: product price-фичи, ranks/percentiles, discount-фичи, last-clicked и
last-purchased price profiles, product ranking-фичи. Прямой ranking-service upload для этой
Silver-таблицы не настраивается.

## Владелец и алерты

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
