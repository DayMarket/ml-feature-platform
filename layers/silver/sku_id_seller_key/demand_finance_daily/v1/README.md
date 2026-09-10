# Финансовые события SKU и продавца

## Output

`iceberg.silver.feature_platform_demand_finance_daily`, 60 колонок.
Путь `layers/silver/sku_id_seller_key/demand_finance_daily/v1`.
Ключ `date,sku_id,seller_key`; seller_key = seller:<id> или unknown,
seller_id nullable. Продавец берётся из финансового факта, не текущего каталога.

## Источник и поля

`marts_b2c.finance_margin_by_order_item` через `clickhouse_dwh_team_logistics`.
Ровно dt финансового события; без ограничения возраста заказа и фильтра sales cohort.
Источник ReplicatedMergeTree, не ReplacingMergeTree: FINAL/dedup не применять.
Повтор order_item_id по разным событиям не означает дубль.
До записи проверяется весь день: неположительный SKU/отрицательный seller блокирует,
не исчезает по WHERE. Seller=0/NULL сохраняется как unknown.

28 мер карты: generated/assembled/delivered/completed/returned/net, возвраты текущих/
предыдущих периодов, without-promo суммы, скидки. Денежные source Int64 сворачиваются
в DECIMAL(38,0), не Double; signed значения допустимы. Единицы BIGINT после безопасной
проверки диапазона Decimal-суммы. Для 20 денежных мер есть отдельные USD.
Не включаем Float64 комиссии/маржу под именем точных денежных сумм.
В источнике нет подтверждённого updated_at для этих событий: время захвата не
переименовывается в business/event/update time.

Source-аудит одного дня 2026-09-06: 860811 строк, неположительных SKU/отрицательных
seller/нулевых seller не найдено. Это не проверка всей истории.
Финансовые net-потоки не меняют target основной когорты продаж.
Для join с gold SKU сначала свернуть продавцов, иначе строки размножатся.

## Оркестрация и запуск

Один owner DAG `feature-platform.layers.silver.sku_id_seller_key.demand_finance_daily` работает ежедневно в `04:00 UTC` с `max_active_runs=1`.

Scheduled run перестраивает ровно 31 завершённую UTC-дату. Ручной диапазон запускается в том же DAG:

```json
{"mode":"manual","start":"2026-08-01","end":"2026-09-01"}
```

`end` не включается. Один metadata-preflight выполняется до записи; каждый день сам
проверяет source/FX до и после потокового чтения, атомарно коммитится и возобновляется
по receipt. DQ и feature statistics проверяют все даты на финальном snapshot. Старые
DQ-пропуски вне окна автоматически не добавляются, отдельного history/full-history DAG нет.

Ручной запуск без `start/end` использует
`[max(2022-09-01, UTC date(logical_date) - 31 дней), UTC date(logical_date))`.
В UI/API задайте logical date явно (например, `2026-09-01T00:00:00Z`).
Обе явные границы имеют приоритет; только одна граница — ошибка. Для manual
`mode` по умолчанию равен `manual`; бюджет такого запуска остаётся ручным.
Старт на 01.10.2022, затем первые числа месяцев и сегодняшняя UTC-дата дают
полное покрытие завершённых дней с допустимыми перекрытиями.

DQ проверяет все даты; штатные freshness и growth дополнительно работают для
вчерашнего дня относительно времени его записи. Growth использует штатный порог
20% в обе стороны и требует предыдущую партицию. `feature_stats` остаётся отдельной
параллельной задачей и сканирует каждую дату диапазона в Trino.
