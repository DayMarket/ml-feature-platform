# Календарь demand forecast

ci_test/smoke_demand_calendar_dag.py прошёл в согласованном 3.1.8-python3.11-ml-2:
Airflow 3.1.8 / PyIceberg 0.9.1, оба графа, ресурсы, рендеринг dict XCom и manual-date
сенсор без logical_date. Образ не изменён, запуск без сети и с read-only репозиторием.
В нём также прошли 67 тестов подготовки/runtime/DQ (pytest подключён отдельно read-only).
17 Iceberg/SQLite тестов выполнены в отдельной локальной среде: тестовый SqlCatalog
требует SQLAlchemy 2, которого нет в image. Production Hive/CH/Trino/S3 доступы ещё
не проверены; импорт не заменяет end-to-end проверку. Есть RequestsDependencyWarning.

Production extract использует ClickHouseHook(use_numpy=False) и нативные строки с
column_types из execute(with_column_types=True). DATE/целые/NULL переносятся в Arrow
без Pandas coercion; неожиданные исходные типы/число колонок/значения блокируют запись.
Контекст соединения закрывается после чтения. Отдельная DataFrame-инъекция оставлена
только для локальных тестов; production через неё не читает.

Выход: `iceberg.silver.feature_platform_demand_calendar`.
Ключ: `date`, partition months(date). Группа пути — `date`.
calendar_id=uz_official — официальный календарь Узбекистана. date — dt источника,
не дата загрузки. 18 полей: все 15 бизнес-полей, calendar_id и provenance
source_manifest_id/ingested_at. Числа INT, флаги BOOLEAN, неизвестные значения NULL.

## Источник и граница backfill

ClickHouse silver.calendar через clickhouse_dwh_team_logistics. Владелец источника
заполняет праздники/переносы. Проверка 2026-09-08: 2191 уникальная дата,
2021-01-01…2026-12-31. Полный SELECT без WHERE/LIMIT читает также будущие даты.
Пропуски не достраиваются, исламские праздники не рассчитываются вместо источника,
строки/статусы и рабочие переносы не нормализуются. Новая dbt-модель не нужна.

Manual — один полный захват доступного справочника, не сотни ежедневных копий.
Он не восстанавливает историю публикации календаря; historical as_known этим не доказан.
Продажи/E2/E3 не нужны для календаря. Их потребители остаются отдельными pipeline.

## Запись

job/preparation.py делает preflight target/schema до запроса CH, затем Arrow по
схеме Iceberg. job/writer.py заменяет всю таблицу календаря, включая исчезнувшие
даты в старых месяцах. Других календарей в этой date-keyed таблице нет;
calendar_id — lineage-атрибут, не область частичной записи.
Пустой захват, дубли/NULL ключей, несовместимые типы, неверные флаги/диапазоны,
неоднозначный capture отклоняются до записи. NULL и пустые исходные строки сохраняются.

Read-back читает созданный snapshot, затем refresh проверяет current. Результат
written содержит table_uuid, snapshot_id, source_manifest_id, ingested_at UTC
с микросекундами, rows_written, date_min/date_max. Это JSON для связи задач через XCom,
не отдельный manifest-store или ready. Min/max не доказывает отсутствие дыр.
Tags/защита каждого snapshot не создаются; ошибка не откатывает Iceberg main.

job/runtime.py до чтения источника проверяет существование target и служебных DQ/stats
таблиц. Использует штатный Hive/S3 каталог dq.results_writer.load_results_catalog:
spark_ycs_connection, те же Metastore/warehouse, что DQ. Секреты — Airflow Connections.
Миграции создают таблицы; runtime не выполняет CREATE.

## DQ и feature_stats

После успешной DQ и сохранения её результатов возвращается единый XCom с
dq_status=passed, dag_id, run_id и receipt записи. События читают его только после
сенсора dq по exact run_id, без include_prior_dates/latest. Старый запуск без этого
payload нужно повторить после развёртывания; written XCom не заменяет успешный DQ.
Regular DAG использует явный CronDataIntervalTimetable UTC (cron 03:00 не меняется).
В Airflow 3.1.8 scheduled run_id содержит run_after, logical_date — начало интервала.

Терминальный `dq` проверяет всю атомарно заменённую current-таблицу: PK,
непустой срез, диапазоны месяца/дня недели, provenance и единый capture. Downstream
ждёт только `dq` и получает writer receipt после успешной проверки. Growth/freshness
отключены: current не хранит предыдущий capture, а max business-date календаря не
измеряет свежесть загрузки. `feature_stats` для table-scope справочника отключён.
Все DQ-проверки блокируют с первого запуска (`warmup_days=0`).

feature_stats читает тот же snapshot и весь uz_official одним Trino-сканом числовых
полей. Connection trino_search для обеих задач. Это дополнительный небольшой скан
справочника при каждом запуске, не один business-day. Все запросы полностью
квалифицированы, UTC timestamp сохраняет микросекунды.

## DAG и запуск

Один owner DAG `feature-platform.layers.silver.date.demand_calendar` работает с `max_active_runs=1`.
По расписанию `03:00 UTC` он заменяет полный текущий справочник. Ручной полный refresh запускается в этом же DAG с JSON:

```json
{"mode": "manual"}
```

Отдельного history/full-history DAG нет. После записи owner запускает DQ и feature statistics для точного writer receipt и snapshot.

## Проверки

Локальные тесты покрывают подготовку полного справочника, атомарную замену, DQ/stats wiring и оба типа запуска owner.
