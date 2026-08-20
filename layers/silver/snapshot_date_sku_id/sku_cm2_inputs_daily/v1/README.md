# sku_cm2_inputs_daily

Общий дневной SKU-вход для независимых Main CM2 и PDP CM2 расчётов. Это единственный целевой
Silver-контракт с SKU grain в цепочке CM2.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_sku_cm2_inputs_daily`.
- DAG: `feature-platform.layers.silver.snapshot_date_sku_id.sku_cm2_inputs_daily`.
- Путь: `layers/silver/snapshot_date_sku_id/sku_cm2_inputs_daily/v1`.
- Airflow group tag: `recsys-main-page-features`.
- Расписание: ежедневно в `00:00 UTC` (`0 0 * * *`).
- `start_date=2026-06-01T00:00:00Z`, `catchup=false`.

`calculated_at` равен `data_interval_end` в UTC, а `snapshot_date` — предыдущей календарной
дате UTC. Повторный запуск одной партиции идемпотентно перезаписывает её через PyIceberg
`overwrite`.

Подтверждённые upstream DQ DAG id внешних источников отсутствуют, поэтому отдельные sensors не
добавлены.

## Grain и схема

Grain и уникальный ключ: `snapshot_date,sku_id`.

- `snapshot_date` — дата входного снимка;
- `sku_id` — SKU;
- `product_id` — товар для финальной агрегации в Gold;
- `dimensional_group` — `SMALL`, `MEDIUM` или `LARGE`;
- `sell_price_uzs` — дневная sell price SKU в UZS либо `NULL`;
- `commission_pct` — комиссия SKU в процентах либо `NULL`;
- `n_orders_28d` — число строк заказов SKU за 28 дней.

`sku_group_id`, currency rate, готовые `cm2`, `net_inflow` и `weighted_price` не публикуются.

## Источники и joins

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-clickhouse".dict.sku` — базовая SKU-population, mapping
  `id AS sku_id → product_id` и `dimensional_group`;
- `"dwh-clickhouse".marts.daily_sku_quantity_eod` — исторический `sell_price_eod`;
- `"dwh-iceberg".silver_apidb_kazanexpress.public_sku_actual_commission` —
  `sku_id → comission`;
- `"dwh-iceberg".silver.order_item_ue_buyer` — строки заказов SKU.

В snapshot входят все строки `dict.sku` с заполненными `sku_id` и `product_id`. Цена,
комиссия и счётчик заказов присоединяются к этой population через `LEFT JOIN` по `sku_id`.
Контракт источников предполагает уникальность mapping, комиссии и dimensional group по
`sku_id`, а цены — по `snapshot_date,sku_id`.

Исторической относительно `snapshot_date` является только EOD-цена. Mapping,
`dimensional_group` и комиссия читаются в фактический момент выполнения job; для backfill они
не являются point-in-time.

## Расчёт

Цена выбирается условием:

```sql
daily_sku_quantity_eod.dt = snapshot_date
```

`dt` доступен в Trino как `DATE`, поэтому дополнительное преобразование `toDate(dt)` не
требуется.

Отсутствующий `dimensional_group` заменяется на `SMALL`. Другие значения не
нормализуются: допустимы только `SMALL`, `MEDIUM`, `LARGE`.

Комиссия читается из колонки `comission`, приводится к `DOUBLE` и публикуется как
`commission_pct`. Отсутствующая комиссия остаётся `NULL`.

`n_orders_28d` рассчитывается как `COUNT(*)` строк
`silver.order_item_ue_buyer` по `sku_id` на полуоткрытом UTC-интервале:

```text
[calculated_at - 28 дней, calculated_at)
```

Это не `COUNT(DISTINCT order_id)`, не сумма quantity и не число уникальных покупателей. SKU
без строк заказов получает `n_orders_28d = 0`.

## Проверки качества

Источники читаются одним Trino-запросом. Отдельный metrics-запрос не выполняется, чтобы не
сканировать повторно 28 дней заказов и остальные источники. Перед записью проверяются:

- непустой результат;
- `snapshot_date` равна целевой партиции;
- уникальность `snapshot_date,sku_id`;
- `sku_id` и `product_id` заполнены и являются целыми;
- `sell_price_uzs >= 0` либо `NULL`;
- `commission_pct` находится в `[0,100]` либо `NULL`;
- `n_orders_28d` является целым, заполненным и неотрицательным;
- `dimensional_group IN ('SMALL','MEDIUM','LARGE')`.

Нарушение уникальности mapping, дневной цены или комиссии приводит к дублированию итогового
`snapshot_date,sku_id` и останавливает запись общей проверкой уникального ключа.

Логируются coverage цены и комиссии, доля SKU без заказов и распределение dimensional group.
После merge в master стандартная синхронизация создаст dbt DQ с уникальностью ключа и
`not_null` для ключевых колонок.

## Рантайм и потребители

Airflow/Python + `pyiceberg`, образ
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`. Для финального snapshot примерно на
9,9 млн SKU pod запрашивает `4 CPU / 24 GiB`.

Потребители:

- G9a `product_cm2_main_features`;
- G9b `product_cm2_pdp_features`.

Оба Gold-контракта агрегируют результат на `product_id`. Main CM2 применяет свой fallback
комиссии, PDP CM2 исключает SKU с `NULL` commission. Price p99.9 cap, CM2 business constants и
USD rate присоединяются или применяются в Gold. Прямой ranking-service upload для S6 не
настраивается.

## Владелец и алерты

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.

После merge в master нужно проверить автоматически созданные PR для dbt-trino DQ и
регистрации Iceberg maintenance в `DayMarket/pyspark-etl`.
