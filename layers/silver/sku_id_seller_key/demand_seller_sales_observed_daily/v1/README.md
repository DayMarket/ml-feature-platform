# Дневные продажи SKU × продавец

Выход: `iceberg.silver.feature_platform_demand_seller_sales_observed_daily`.
Ключ: `date,sku_id,seller_key`, группа `sku_id_seller_key`.
DAG: `feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily`.
Ручные диапазоны выполняет тот же owner DAG. Общий tag: `demand-forecast`.

## Источник и поля

`marts.order_items FINAL` через `clickhouse_dwh_team_logistics`.
Когорта создания заказа в Asia/Tashkent, полуоткрытые UTC-границы дня;
CREATED/NOT_CREATED исключены, sku_id > 0. Продавец из позиции заказа, без JOIN
текущего каталога: положительный seller_id → seller:<id>, исходный 0/NULL → unknown.
Отрицательный seller_id блокирует запись; unknown-вклад сохраняется.

42 поля: date/sku/seller_key/seller_id, два контрольных distinct SKU-дня,
seller-количества/заказы/позиции, исходные Decimal(38,0) и отдельный USD,
FBO/FBS/DBS/other/unknown, source/FX metadata. Определения — migrations/create_table.sql.
Масштаб денег не меняется; signed marketplace promo допустим.
Курс дневной либо последний доступный; при неизвестном курсе USD остаётся NULL.

## Точные счётчики

GROUPING SETS одним CH-запросом считает seller-группы и subtotal SKU.
Оконный maxIf переносит subtotal в sku_sales_orders/sku_sales_order_items.
Subtotal-строки отбрасываются после окна. Общий заказ разных продавцов не удваивается.
SKU-sales обязан читать exact passed FP snapshot, проверить одинаковость контролей
и взять одно значение; SUM seller distinct запрещён. Контроли не аддитивны по SKU.
Отдельное чтение order_items потребителем запрещено.
Нагрузка production-прогона зависит от числа seller×SKU за выбранные даты.

## Обновление и оркестрация

Один owner DAG `feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily` работает ежедневно в `04:00 UTC` с `max_active_runs=1`.

Scheduled run перестраивает ровно 31 завершённую UTC-дату. Ручной диапазон запускается в том же DAG, например полный август:

```json
{"mode":"manual","start":"2026-08-01","end":"2026-09-01"}
```

Граница `end` не включается. Дни пишутся последовательно атомарными Iceberg commit с resume по receipts; DQ и feature statistics проверяют каждую дату на финальном snapshot. Старые DQ-пропуски вне окна автоматически не добавляются, отдельного history/full-history DAG нет.

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
