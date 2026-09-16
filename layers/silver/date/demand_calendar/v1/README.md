# Календарь demand forecast

Выход: `iceberg.silver.feature_platform_demand_calendar`.
Путь: `layers/silver/date/demand_calendar/v1`.
Ключ: `date`, partition `months(date)`.
DAG: `feature-platform.layers.silver.date.demand_calendar`.
Группа DAG: `demand-forecast`.

## Источник

ClickHouse `silver.calendar` через `clickhouse_dwh_team_logistics`, весь справочник
(аудит 2026-09-08: 2191 дата, 2021-01-01…2026-12-31, включая будущие даты).
`calendar_id = uz_official` — официальный календарь Узбекистана. `date` — `dt`
источника. Числа приводятся к INT, флаги к BOOLEAN, NULL и пустые строки сохраняются.
Пропуски не достраиваются, праздники не вычисляются.

## Запись и оркестрация

Ежедневно в `03:00 UTC`, `max_active_runs=1`.
Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.

DQ проверяет всю таблицу (`scope: table`); growth/freshness отключены,
`feature_stats` для справочника отключён.
