# product_prices_daily

Дневные price-факты товара. Исторические цены относятся к `price_date`, а доступность SKU
определяется по текущему состоянию на момент выполнения job.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_product_prices_daily`.
- DAG: `feature-platform.layers.silver.price_date_product_id.product_prices_daily`.
- Путь: `layers/silver/price_date_product_id/product_prices_daily/v1`.
- Групповой тег Airflow: `recsys-main-page-features`.
- Расписание: ежедневно в 00:00 UTC (`0 0 * * *`).
- `price_date` — предыдущая календарная дата UTC относительно `data_interval_end`.
- `start_date=2026-01-01T00:00:00Z`, `catchup=False`.

Upstream-источники внешние для feature-platform. Подтвержденный DQ DAG id для
`marts.daily_sku_quantity_eod` отсутствует, поэтому upstream sensor не добавлен. Пустой срез
останавливает задачу до записи.

## Грейн и ключ

Грейн и уникальный ключ: `price_date, product_id`.

SKU и SKU-group используются только внутри расчета и не публикуются.

## Источники

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-clickhouse".marts.daily_sku_quantity_eod` — `full_price_eod` и
  `sell_price_eod` за `price_date`;
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

Из EOD-источника выбираются строки с `dt = price_date`. Тип `dt` в Trino — `DATE`, поэтому
дополнительный `toDate(dt)` не нужен.

Текущая доступность SKU:

```sql
status = 'ACTIVE'
AND (
    COALESCE(quantity_active, 0) > 0
    OR COALESCE(quantity_fbs, 0) > 0
)
```

Сначала цены агрегируются по `price_date × product_id × sku_group_id`, затем по
`price_date × product_id`:

- `avg_sell_price_eod = AVG(sku_group.avg_sell_price_eod)`;
- `min_full_price_eod = MIN(sku_group.min_full_price_eod)`;
- `min_sell_price_eod = MIN(sku_group.min_sell_price_eod)`;
- active-price колонки используют такую же двухэтапную агрегацию.

Для SKU, отсутствующего в dict или не удовлетворяющего условию доступности, цена не входит в
active-price агрегаты. Если у товара нет доступных SKU, все три active-price колонки равны
`NULL`. Нули вместо `NULL` не подставляются.

Историческая цена является point-in-time относительно `price_date`, текущая доступность — нет.
При backfill также используется current-state dict на фактический момент запуска.

## Проверки качества

Перед записью проверяются:

- непустой source-срез и coverage mapping по SKU/product;
- уникальность `price_date, product_id`;
- `product_id > 0`;
- согласованность active-price колонок и условие `min <= avg <= max`;
- отсутствие дат, отличных от целевой `price_date`.

До агрегации на уровень SKU-group `full_price_eod` и `sell_price_eod` фильтруются независимо.
Значение участвует в расчете, если оно находится в диапазоне от `0` до `1_000_000_000`
включительно. Отрицательные цены и значения выше порога заменяются на `NULL` и не участвуют в
соответствующих `AVG`, `MIN` и `MAX`.

Некорректный `full_price_eod` не исключает корректный `sell_price_eod`, и наоборот.
Active-price агрегаты используют только валидный `sell_price_eod` доступных SKU. Если после
фильтрации для метрики не осталось валидных цен, результат равен `NULL`; ноль вместо `NULL` не
подставляется. Отдельные алерты и падение DAG из-за отфильтрованных цен не предусмотрены.

После merge в master стандартная синхронизация создаст dbt DQ с уникальностью ключа и
`not_null` для ключевых колонок.

## Рантайм и потребители

Airflow/Python + `pyiceberg`, не Spark. Образ:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`. Партиция `price_date` идемпотентно
перезаписывается через PyIceberg `overwrite`.

Потребители: product price-фичи, ranks/percentiles, discount-фичи, last-clicked и
last-purchased price profiles, product ranking-фичи. Прямой ranking-service upload для этой
Silver-таблицы не настраивается.

## Владелец и алерты

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
