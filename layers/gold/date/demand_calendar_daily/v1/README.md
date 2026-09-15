# Дневной календарь demand forecast

Выход: `iceberg.gold.feature_platform_demand_calendar_daily`, ключ `date`, `months(date)`.
Путь: `layers/gold/date/demand_calendar_daily/v1`.
DAG: `feature-platform.layers.gold.date.demand_calendar_daily`.
Группа DAG: `demand-forecast`.

## Источники и поля

`iceberg.silver.feature_platform_demand_calendar` и
`iceberg.silver.feature_platform_demand_event_calendar`, один запрос в Trino
(`inputs.trino_conn_id`) на snapshot'ах, зафиксированных в начале запуска.

- 16 полей календаря; сетка дат точно равна silver-календарю.
- `big_sale_created/canceled/unknown_status` — есть BIG_SALE (`source_kind =
  marketing_sale`, `source_type = BIG_SALE`) с таким статусом; при отсутствии акций —
  NULL. `big_sale_event_count` — число различных BIG_SALE на дату (0, если нет).
- `calendar_coverage_status = source_row_present`,
  `promotion_coverage_status` — `registry_rows_present`/`no_registry_rows`.
- Lineage: snapshot id обоих входов, их `source_manifest_id`, собственный `run_id`.

Флаги описывают текущий реестр, а не факт проведения акции. Ranking upload нет.

## Оркестрация

Ежедневно в `04:00 UTC`, `max_active_runs=1`.
Штатный запуск ждёт `dq` календаря (`execution_delta` 60 минут) и событий (50 минут).
Ручной запуск сенсоры пропускает (`upstream_gate`) и читает текущие данные upstream.

Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.
