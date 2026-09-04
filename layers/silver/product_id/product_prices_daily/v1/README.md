# product_prices_daily

Дневные price-факты товара. `dt` обозначает дату расчёта, исторические EOD-цены берутся за
предыдущий календарный день, а доступность SKU определяется по текущему состоянию на момент
выполнения job.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_product_prices_daily`.
- DAG: `feature-platform.layers.silver.product_id.product_prices_daily`.
- Путь: `layers/silver/product_id/product_prices_daily/v1`.
- Групповой тег Airflow: `recsys-features`.
- Ошибки задач отправляют alert уровня `P3` команде `recsys` через
  `oncall_webhook_recsys`.
- Расписание: ежедневно в 19:00 UTC (`0 19 * * *`), то есть в 00:00 `Asia/Tashkent`.
- `dt` — `TIMESTAMP` начала даты расчёта (`00:00:00 Asia/Tashkent`), определённой из
  `data_interval_end`.
- `start_date=2026-08-08T19:00:00Z`, `catchup=True`. Первый запуск записывает `dt=2026-08-10`
  и использует EOD-цены за 9 августа; initial backfill цен по-прежнему начинается с 9 августа.

Перед материализацией `ExternalTaskSensor` ожидает успешный
запуск DAG `dwh_core.quantity_eod`, который формирует актуальный EOD-срез и запускается
ежедневно в `00:00 UTC`. S3 с расписанием `19:00 UTC` использует
`execution_delta=19 часов` и ждёт соответствующий upstream-run той же logical date.
Сенсор ожидает состояние всего DAG, а не отдельную задачу. Пустой срез дополнительно
останавливает задачу до записи.

Результат основного Trino-запроса читается и записывается в Iceberg батчами по
`runtime.query_batch_rows` строк. Полная product-партиция не загружается в память Airflow
worker целиком; очистка старой партиции и добавление всех батчей фиксируются одной
Iceberg-транзакцией.

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
  `sell_price_eod` за календарный день перед `dt`;
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

Внутренний `sku_id` в coverage-запросе приводится к `BIGINT`, поскольку источники содержат
значения больше максимума `INTEGER`. SKU не публикуется в выходной product-grain таблице.

## Расчет

Целевая `dt` вычисляется из `data_interval_end` в `Asia/Tashkent` и сохраняется как
`TIMESTAMP` локальной полуночи. Из EOD-источника выбираются строки за `dt - 1 день`, поскольку
на момент расчёта текущий календарный день ещё не завершён. Поле
`daily_sku_quantity_eod.dt` имеет тип `DATE`.

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

Историческая цена является point-in-time относительно календарного дня перед `dt`, текущая
доступность — нет. При backfill также используется current-state dict на фактический момент
запуска.

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

После записи параллельно запускаются внутренние `dq` и `feature_stats` для целевой `dt`.
DQ блокирует downstream при NULL или дублях ключа `dt,product_id`; freshness, минимальный
объём и изменение объёма во время первичной раскатки имеют severity `warn`.
`feature_stats` одним полным Trino-сканом дневной партиции считает распределения всех
price-колонок. Это один дополнительный полный скан партиции в сутки.

`table.meta.create_dbt_pr: false`: собственный DQ S3 выполняется внутри DAG, поэтому CI не
создаёт для целевой таблицы новый DQ-PR в `dbt-trino`. Sensor внешнего
`dwh_core.quantity_eod` сохраняется как upstream-контракт источника. Iceberg maintenance
остаётся включённым.

## Рантайм и потребители

Airflow/Python + `pyiceberg`, не Spark, поэтому Spark-параметр `resource_profile` здесь не
применяется. Задача использует компактный pod с `1 CPU` и `4 GiB` памяти. Образ:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`. Партиция `dt` идемпотентно
перезаписывается через PyIceberg `overwrite`.

Потребители: product price-фичи, ranks/percentiles, discount-фичи, last-clicked и
last-purchased price profiles, product ranking-фичи. Прямой ranking-service upload для этой
Silver-таблицы не настраивается.

## Владелец и алерты

`table.meta.team = team::recsys`. Внешние on-call алерты для DAG отключены; ошибки остаются
видимыми в статусах и логах Airflow.
