# События demand forecast

Выход: `iceberg.silver.feature_platform_demand_event_calendar`.
Путь: `layers/silver/event_code/demand_event_calendar/v1`.
Ключ: `(date, event_code)`, partition `months(date)`.
DAG: `feature-platform.layers.silver.event_code.demand_event_calendar`.
Группа DAG: `demand-forecast`.

## Источники и смысл

- праздники — строки `iceberg.silver.feature_platform_demand_calendar` с
  `is_public_holiday = TRUE`: `source_event_id` — ISO-дата,
  `event_code = calendar:uz_official:<дата>`, `event_name` — `holiday_name` как есть;
- акции — ClickHouse `silver.b2b_marketing_sale` через `clickhouse_dwh_team_logistics`:
  `source_event_id` — `id`, `event_code = marketing_sale:<id>`, `calendar_id` NULL.
  Акция разворачивается в дни Asia/Tashkent, пересекающие `[started_at, finished_at)`
  (окончание в 00:00 этот день не включает), и ограничивается датами календаря.

Статусы и типы акций сохраняются как есть (CREATED/CANCELED/…); фильтров по ним нет,
`announced_at` не подменяется `created_at`. Пустой реестр, повтор `id`,
неположительный `id` или отсутствующий/обратный интервал блокируют запись.

## Запись и оркестрация

Ежедневно в `03:10 UTC`, `max_active_runs=1`.
Штатный запуск ждёт `dq` календаря (`03:00 UTC`, `execution_delta` 10 минут).
Ручной запуск сенсоры пропускает (`upstream_gate`) и читает текущие данные upstream.

Таблица каждый запуск полностью заменяется одним `overwrite`; пустой результат
не перезаписывает прежние данные. Все строки захвата имеют одно `ingested_at`
(с точностью до секунды): `write` возвращает его в XCom, а `dq`/`feature_stats`
проверяют именно этот захват (`partition_granularity: timestamp`).
Ручной запуск параметров не принимает — это тот же полный refresh.
