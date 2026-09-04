# sku_cm2_inputs_daily

Общий дневной SKU-вход для независимых Main CM2 и PDP CM2 расчётов. Это единственный целевой
Silver-контракт с SKU grain в цепочке CM2.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_sku_cm2_inputs_daily`.
- DAG: `feature-platform.layers.silver.sku_id.sku_cm2_inputs_daily`.
- Путь: `layers/silver/sku_id/sku_cm2_inputs_daily/v1`.
- Airflow group tag: `recsys-features`.
- Расписание: ежедневно в `19:00 UTC`, то есть в `00:00 Asia/Tashkent`.
- `start_date=2026-08-08T19:00:00Z`, `catchup=true`: при первом включении DAG выполняет
  начальный backfill примерно за две недели.

`dt` — `TIMESTAMP` начала локальной даты выполнения расчёта:
`00:00:00 Asia/Tashkent` для даты `data_interval_end`. Например, запуск
`2026-08-23 19:00 UTC` записывает `dt = 2026-08-24 00:00:00`. Повторный запуск одного `dt`
идемпотентно заменяет эту партицию одной PyIceberg-транзакцией.

Результат Trino читается и записывается в Iceberg батчами по
`runtime.query_batch_rows` строк. Полная SKU-партиция не загружается в память Airflow worker
целиком; очистка старой партиции и добавление всех батчей фиксируются одной
Iceberg-транзакцией.

Перед материализацией `ExternalTaskSensor` ожидает успешный запуск DAG
`dwh_core.quantity_eod`, который формирует используемый EOD-срез и запускается ежедневно в
`00:00 UTC`. S6 с расписанием `19:00 UTC` использует `execution_delta=19 часов` и ждёт
соответствующий upstream-run той же logical date. Сенсор ожидает состояние всего DAG, а не
отдельную задачу.

## Grain и схема

Grain и уникальный ключ: `dt,sku_id`.

- `dt` — `TIMESTAMP` начала даты выполнения расчёта (`00:00:00 Asia/Tashkent`);
- `sku_id` — SKU типа `BIGINT`; источник содержит значения больше максимума `INTEGER`;
- `product_id` — товар для финальной агрегации в Gold;
- `dimensional_group` — `SMALL`, `MEDIUM` или `LARGE`;
- `sell_price_uzs` — EOD sell price SKU в UZS за календарный день перед `dt` либо `NULL`;
- `commission_pct` — комиссия SKU в процентах либо `NULL`;
- `n_orders_28d` — число строк заказов SKU за предыдущие 28 полных дней.

`sku_group_id`, currency rate, готовые `cm2`, `net_inflow` и `weighted_price` не публикуются.

## Источники и joins

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-clickhouse".dict.sku` — базовая SKU-population, mapping
  `id AS sku_id → product_id` и `dimensional_group`;
- `"dwh-clickhouse".marts.daily_sku_quantity_eod` — исторический `sell_price_eod`;
- `"dwh-iceberg".silver_apidb_kazanexpress.public_sku_actual_commission` —
  `sku_id → commission`;
- `"dwh-iceberg".silver.order_item_ue_buyer` — строки заказов SKU.

В результат входят все строки `dict.sku` с заполненными `sku_id` и `product_id`. Цена,
комиссия и счётчик заказов присоединяются через `LEFT JOIN` по `sku_id`. Контракт источников
предполагает уникальность mapping, комиссии и dimensional group по `sku_id`, а цены — по
`dt,sku_id`.

Исторической относительно момента расчёта является только EOD-цена. Mapping,
`dimensional_group` и комиссия читаются в фактический момент выполнения job; для backfill они
не являются point-in-time.

## Расчёт

В `00:00 Asia/Tashkent` текущий календарный день ещё не имеет EOD-цены. Поэтому выходной `dt`
хранит дату расчёта, а цена выбирается отдельным условием
`daily_sku_quantity_eod.dt = dt - INTERVAL '1' DAY`. Например, строка S6 с
`dt = 2026-08-24 00:00:00` использует EOD-цену за `2026-08-23`. Поле `dt` источника доступно в Trino
как `DATE`, поэтому дополнительное преобразование не требуется.

Постоянные правила зафиксированы непосредственно в расчёте:

- `dimensional_group IS NULL → SMALL`;
- комиссия читается из колонки `commission`;
- допустимые dimensional group: `SMALL`, `MEDIUM`, `LARGE`;
- допустимый диапазон комиссии: `[0,100]`;
- окно заказов: 28 дней.

Отсутствующая комиссия остаётся `NULL`.

`n_orders_28d` рассчитывается как `COUNT(*)` строк
`silver.order_item_ue_buyer` по `sku_id` на полуоткрытом интервале
`[data_interval_end - 28 дней, data_interval_end)` в `Asia/Tashkent`, то есть до начала даты
расчёта `dt`. UTC-время источника явно переводится в `Asia/Tashkent` внутри Trino-запроса.
Дополнительные эквивалентные границы в UTC накладываются непосредственно на
`order_created_at`, чтобы Trino мог протолкнуть временной фильтр в источник и не сканировал
всю историю заказов.

Это не `COUNT(DISTINCT order_id)`, не сумма quantity и не число уникальных покупателей. SKU
без строк заказов получает `n_orders_28d = 0`.

## Проверки качества

Перед записью producer проверяет:

- непустой результат;
- `dt` равна целевой дате;
- уникальность `dt,sku_id`;
- `sku_id` и `product_id` заполнены и являются целыми;
- `sell_price_uzs >= 0` либо `NULL`;
- `commission_pct` находится в `[0,100]` либо `NULL`;
- `n_orders_28d` является целым, заполненным и неотрицательным;
- `dimensional_group IN ('SMALL','MEDIUM','LARGE')`.

Логируются coverage цены и комиссии, доля SKU без заказов и распределение dimensional group.
После записи параллельно запускаются внутренние `dq` и `feature_stats` для целевой `dt`.
DQ блокирует downstream при NULL или дублях ключа `dt,sku_id`; freshness, минимальный объём
и изменение объёма во время первичной раскатки имеют severity `warn`. `feature_stats`
исключает технический `product_id` и одним полным Trino-сканом дневной партиции считает
распределения цены, комиссии и `n_orders_28d`; это один дополнительный полный скан в сутки.

`table.meta.create_dbt_pr: false`: CI не создаёт для таблицы новый DQ-PR в `dbt-trino`;
Iceberg maintenance остаётся включённым.

## Runtime и потребители

Пайплайн использует Airflow/Python + `pyiceberg`, образ
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` и малый pod `1 CPU / 4 GiB memory`. Для этого
runtime Spark-параметр `resource_profile` неприменим.

Потребители:

- G9a `product_cm2_main_features`;
- G9b `product_cm2_pdp_features`.

Оба Gold-контракта агрегируют результат на `product_id`. Main CM2 применяет свой fallback
комиссии, PDP CM2 исключает SKU с `NULL` commission. Price p99.9 cap, CM2 business constants и
USD rate присоединяются или применяются в Gold. Прямой ranking-service upload для S6 не
настраивается.

## Владелец и алерты

`table.meta.team = team::recsys`. Внешние on-call алерты для DAG отключены; ошибки остаются
видимыми в статусах и логах Airflow.

После merge в master нужно проверить автоматически созданный PR регистрации Iceberg
maintenance в `DayMarket/pyspark-etl`.
