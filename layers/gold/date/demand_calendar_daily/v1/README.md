# Дневной календарь demand forecast

## Выход и источники

`iceberg.gold.feature_platform_demand_calendar_daily`, ключ `date`, months(date).
Путь: layers/gold/date/demand_calendar_daily/v1. Группа DAG: demand-forecast.
Источники — `iceberg.silver.feature_platform_demand_calendar` и
`iceberg.silver.feature_platform_demand_event_calendar`. Конфиги владельцев — inputs
в config.yaml. Собственных запросов в ClickHouse у gold нет, silver он не пишет.

28 полей: 16 полей календаря (date вместо dt, calendar_id как lineage),
big_sale_created/canceled/unknown_status, big_sale_event_count, два coverage_status,
две пары snapshot_id/source_manifest_id входов и собственные source_manifest_id/ingested_at.
Официальные NULL/пустые строки сохраняются. Сетка дат точно равна silver-календарю:
нет синтетических дней, заполнения дыр или будущего сценария.

В реестре выбирается только source_kind=marketing_sale и source_type=BIG_SALE.
Пересекающиеся акции сворачиваются до дня без fanout. CREATED/CANCELED сохраняют
исходный смысл; остальные статусы, включая NULL, отмечаются unknown_status.
При отсутствии BIG_SALE флаги NULL, event_count=0: ноль считает строки текущего
реестра, не утверждает отсутствие фактических акций. При наличии записей флаги —
OR по статусу, count — число различных event_code. TODAY_DEALS остаётся в silver.
Поля confirmed нет: источник не подтверждает факт проведения.

calendar_coverage_status=source_row_present означает наличие даты, не заполненность
всех полей. promotion_coverage_status=registry_rows_present/no_registry_rows относится
только к BIG_SALE в текущем захвате. История доступности не доказана; этот gold сам по
себе не даёт point-in-time корректный benchmark. Labels/окна/прошлогодний сценарий —
ответственность модельного репозитория. Ranking upload отсутствует.

## Запись и DQ

Полная замена справочника через PyIceberg, без tags. Пустой/невалидный вход блокирует
запись. Preflight всех входов, выхода и DQ/stats tables до чтения snapshot. Runtime
не создаёт таблиц. Чтение только точных UUID/snapshot из успешных upstream DQ receipts;
при истечении snapshot нет fallback на latest. Events обязаны содержать receipt того
же calendar snapshot. Устаревший набор строк не сохраняется при исчезновении даты.
Захват каждого входа не старше 2 дней и не из будущего относительно сборки gold;
давний успешный DQ не заменяет свежесть входов.
Read-back сравнивает все строки/поля и проверяет current после чтения.

Full-refresh DQ без warmup: PK, непусто, рост ±20%, freshness 2 дня по ingested_at,
capture_matches; дополнительно lineage/not_null, единый набор версий, enum и диапазоны.
Штатные пороги сохранены до первого замера, не ослаблены; исходный календарь в аудите
2026-09-08 содержит 2191 дату. Совпадение сетки/NULL/status проверяется подготовкой
и writer; отдельный дорогой SQL join к silver для DQ не добавлен.
Терминальные dq и feature_stats параллельны. Stats ежедневно сканирует весь небольшой
gold-календарь в Trino trino_search; downstream ждёт только dq. Оба читают snapshot
из writer receipt. Очистка старой write-задачи при другом активном запуске запрещена.

## Оркестрация

Один owner DAG `feature-platform.layers.gold.date.demand_calendar_daily` с `max_active_runs=1` собирает gold после точных DQ calendar и events. Scheduled run идёт в `03:20 UTC`.

Для ручного полного обновления оператор последовательно запускает те же owner DAG: calendar, events с точной ссылкой на calendar, затем gold с точными ссылками на оба silver запуска. В gold передаётся `mode=manual`; отдельных history/full-history DAG нет.
