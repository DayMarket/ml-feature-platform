# Дневная когорта продаж SKU

Output: `iceberg.silver.feature_platform_demand_sales_daily`.
Путь: `layers/silver/sku_id/demand_sales_daily/v1`, ключ `date,sku_id`.
Owner DAG: `feature-platform.layers.silver.sku_id.demand_sales_daily`.
Группа `demand-forecast`, team operations; нет ranking upload или модельных X/y.

## Статус и источник

Подготовлены 38-полевая DDL, FP reader/rollup/writer, planner/Connections
и owner DAG. `delivery_status=prepared_local` — локально проверенная
реализация, не production apply, доступность Connections или разрешение backfill.
Весь demand-пакет ещё требует финальной сверки. Production не изменялся.

Активный DAG читает только `inputs.seller_config` через `inputs.trino_conn_id`:
`iceberg.silver.feature_platform_demand_seller_sales_observed_daily`,
владелец `feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily`.
Поток: order_items → seller-sales → SKU-sales, без второго чтения CH или FX UDF.
Секция `source` фиксирует версию свёртки seller-sales. Для запуска используется
семейство `job/seller_*.py`; прямого CH-пути и fallback при ошибке FP нет.

## Семантика 42 → 38 полей

Исходный seller-sales — когорта принятых заказов: order_date_created по Asia/Tashkent,
order_item_status NOT IN ('CREATED','NOT_CREATED'), sku_id > 0. Это не event-date
оплаты/выдачи и не история доступности изменяемых статусов. Подробнее в README источника.
SKU сохраняет исходную date без сдвига, не присоединяет текущий каталог или финансы.

Складываются units, каналы FBO/FBS/DBS/other/unknown и пять денежных сумм
GMV/payment_value/full_value/seller_promo_value/marketplace_promo_value.
Unknown канал не переименовывается в other. Marketplace promo signed.
Уникальные sales_orders и sales_order_items берутся из двух повторённых exact
SKU controls источника, не SUM seller distinct. Controls обязаны совпасть во всех
строках SKU, включая границы порций; max(seller distinct) ≤ control ≤ SUM(seller distinct).
Уники SKU не становятся аддитивными между SKU или датами.

Raw суммы DECIMAL(38,0) складываются точно без /100 и float; переполнение блокирует
день. USD складывается компенсированной суммой. NULL хотя бы одного вклада
сохраняет NULL итога, не частичную сумму. Исходный ноль остаётся нулём.
Применённые rate/date/source/capture FX сохраняются; все строки дня обязаны иметь
общую FX-привязку. Допустимы exact_date/latest_available/unavailable по контракту
источника; unknown USD не заполняется нулём. source_updated_at берётся max по SKU.
Добавляются собственные manifest/version/ingested_at; seller_key/seller_id и два
контрольных поля в 38-полевой SKU-выход не переносятся.

Память: текущий SKU и ограниченные порции (100000 строк/64 MiB), не весь день.
Reader делает ORDER BY sku_id,seller_key, проверяет строгий порядок/уникальность,
native Decimal/типы/NULL/provenance и общее число строк; cursor закрывается при отказе.

## Точный вход, запись и восстановление

job/seller_inputs.py требует passed DQ точного dag_id/run_id: полный receipt,
все выбранные даты/day_checks/counts, source contract/capture, UUID и snapshot.
Используется schema_id выбранного snapshot, не текущая схема. Более новый snapshot
не подменяет выбранный; отсутствующий snapshot/схема блокирует запуск, без latest.
Таблицы должны быть заранее созданы миграциями, runtime не создаёт таблицы/tags.

job/seller_runtime.py атомарно заменяет полную identity-date partition каждого дня.
Пустой источник
не считается известным нулём и не разрешает очистить день. Raw/USD/каналы проверяются
до commit; перед commit повторяется exact source DQ и проверяется target metadata.
Успешные дни сохраняются при сбое последующих дней; written ещё не равно passed DQ.

Source binding (run/UUID/snapshot/schema/day receipt/query hash) сохраняется в обычной
commit metadata. Resume требует тот же binding и полный read-back. Другой вход
с прежним manifest не переиспользует результат. Между днями проверяются target
UUID/head и неизменность source DQ. Отказ read-back после commit не обещает откат.

## Планирование и запуск

Один owner DAG `feature-platform.layers.silver.sku_id.demand_sales_daily` работает ежедневно в `04:00 UTC` с `max_active_runs=1`.

Scheduled run всегда перестраивает ровно 31 завершённую UTC-дату `[data_interval_end - 31 дней, data_interval_end)`. Старые пропуски DQ за пределами этого окна автоматически не подхватываются.

Ручной диапазон запускается в том же DAG. Границы полуоткрытые, поэтому календарный месяц удобно задавать первым числом соседних месяцев:

```json
{
  "mode": "manual",
  "start": "2026-08-01",
  "end": "2026-09-01",
  "reference": {
    "dag_id": "feature-platform.layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily",
    "run_id": "<exact-run-id>",
    "logical_date": "<exact-logical-date>"
  }
}
```

Внутри одного run даты записываются последовательно отдельными атомарными Iceberg commit; retry возобновляет уже подтверждённые дни. Затем DQ и feature statistics проверяют каждую дату на финальном snapshot. Отдельного history/full-history DAG и обхода DQ-покрытия нет.

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

Даже без start/end нужен `reference` на точный seller-sales run, прошедший DQ
для всего окна; формат ссылки приведён выше.
