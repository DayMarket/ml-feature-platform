# События demand forecast

Статус: схема, Arrow-подготовка, preflight/чтение и Iceberg writer реализованы.
DDL в DWH не применён. Regular DAG, upstream DQ sensor и terminal DQ/stats готовы
в коде; manual запрашивает свежий календарь через его owner DAG и ждёт оба dq.
Выход: `iceberg.silver.feature_platform_demand_event_calendar`.
Ключ `(date, event_code)` внутри Iceberg snapshot; partition months(date).
Primary-key group event_code следует невременной части ключа; это календарь событий,
не ежедневное дублирование списка акций на дату загрузки.

## Источники и смысл

Подтверждено владельцем 2026-09-08: **календарь + реестр акций**, без ручного seed.
Календарь — закреплённый выпуск
`iceberg.silver.feature_platform_demand_calendar`, исходно ClickHouse silver.calendar.
Реестр — ClickHouse silver.b2b_marketing_sale. Проверка Iceberg silver по этому
точному имени эквивалента не обнаружила. Дополнительный поиск нашёл
`dwh-iceberg.silver_apidb_kazanexpress.b2b_marketing_sale`; агрегаты статусов совпали
с CH, где таблица имеет engine IcebergS3. Это кандидат прямого Iceberg-чтения,
не выбор этого источника и не полная построчная сверка. Reader остаётся на CH
через подтверждённый clickhouse_dwh_team_logistics, без переключения на кандидата.
Собственного upstream ingestion не добавляем.

Read-only инвентаризация реестра 2026-09-08: 58 CREATED/BIG_SALE,
40 CANCELED/BIG_SALE, 5 CANCELED/TODAY_DEALS; announced_at отсутствует у 92 из 103
строк. Это наблюдённые строки, не доказанная история статусов/объявлений.
По решению владельца **сохраняем исходные статусы**, CREATED не означает
подтверждённую будущую акцию, CANCELED не удаляется скрыто из нормализованной истории.
Не заполняем announced_at из created_at. Решение владельца 2026-09-08:
в X входят праздники и доступные BIG_SALE, включая явно маркированный сценарий
CREATED. CANCELED не активна и не переносится. Прочие типы сохраняются в silver,
но не входят в X. История доступности и reconstructed/as_known учитываются отдельно.
Фиксация сегодняшнего состояния не доказывает as_known.

Для календарного праздника source_event_id — ISO-дата исходной строки,
calendar_id=uz_official, event_code — calendar:<calendar_id>:<source_event_id>;
event_name — holiday_name как есть. Всего 15 полей в нормализованной схеме.
Если в названии перечислено несколько праздников, не разделяем их эвристически.
Для акции source_event_id — строковое представление исходного id,
event_code — marketing_sale:<id>, calendar_id=NULL. Переименование title не меняет ключ.
Writer/release DQ проверяет условную обязательность calendar_id для праздников и
NULL у marketing_sale; межполевые проверки подключены к terminal DQ всего справочника.
Исходные status/type/title и timestamps сохраняются типизированно; запрещены
неявная фильтрация статусов и вычисление модельного is_confirmed на этом слое.

Для нормализации принято `[started_at, finished_at)`: включаются дни Asia/Tashkent,
имеющие непустое пересечение с интервалом. Окончание в 00:00 не включает этот день;
окончание днём включает его. Название акции не источник дат. Аналитический пример:
[dbt-trino smart_discounts_sku_daily](https://github.com/DayMarket/dbt-trino/blob/cbe371839261415488f73f8563ca82eec74fda07/models/commerce/smart_discounts/smart_discounts_sku_daily.sql#L51)
использует timestamp < end_ts с fallback на sale.finished_at. Начало в этом примере —
добавление SKU, не sale.started_at; это не доказательство формального backend-контракта.
[Описание источников в Confluence](https://confluence.uzum.com/pages/viewpage.action?pageId=413381807)
подтверждает смысл finished_at/deleted_at, но не уточняет включительность.

`job/preparation.py:prepare_events` принимает Arrow календаря (поля date/calendar_id/
is_public_holiday/holiday_name) и проекцию реестра (id/title/status/type и пять
timestamps), целевую схему загруженной Iceberg-таблицы, manifest ID и время захвата.
Возвращает Arrow ровно по 15 полям DDL и JSON-совместимый отчёт покрытия, без I/O.
Праздник определяется только is_public_holiday=TRUE, название сохраняется даже при
NULL/пустой строке. NULL флага отражается в отчёте, не становится FALSE.
Акции не фильтруются по type/status. Даты берутся только из исходного календаря;
по каждой акции отчёт содержит число исходных/включённых/непокрытых дней и
complete/partial/outside. Пробелы календаря и неизвестные holiday-флаги видны отдельно.
Отсутствие event-строк само по себе не подтверждает отсутствие акции/праздника.
Пустой реестр явно отражается в отчёте и не объявляется полным или ready.

Наивные timestamps и точность выше us отвергаются; явная зона переводится в UTC.
NULL/неположительный id, повтор id, отсутствующий/пустой/обратный интервал вызывают
ошибку до получения результата, даже если акция полностью вне календаря.
Произвольных drop_duplicates, подстановки времени now и восстановления announced_at нет.
37 тестов: границы/зоны, 29.02/переход года, пробелы, статусы/NULL, негативные схемы,
пустой результат, детерминизм и Parquet round-trip. Production-данные не использовались.
Reader загружает идентификатор календаря из config его владельца, а не копии имени.
Preflight проверяет обе FP-таблицы до SELECT реестра. Календарь читается по точным
table UUID/snapshot ID из receipt: сверяются manifest, число строк, UTC-время и диапазон
дат. Недоступный snapshot блокирует загрузку; current/latest не является заменой.
SELECT реестра не содержит WHERE/LIMIT/DISTINCT; toTimeZone(..., 'UTC') явно задаёт
зону пяти timestamps. Нативные метаданные драйвера проверяются без Pandas, точность
выше us не обрезается молча. Naive datetime драйвера допустим только при явном
UTC в метаданных результата SELECT; source timezone не угадывается.
Пустой реестр блокирует запись по решению владельца. Пустой итоговый batch также не
разрешает удаление всего справочника. Отчёт покрытия и происхождение обоих источников
возвращаются внутри writer receipt и сохраняются в Airflow XCom. Это операционная
история в пределах retention Airflow, не immutable training package или вечный архив.
Сам receipt calendar со статусом written не доказывает успешный upstream DQ:
оркестратор ждёт dq именно этого запуска и проверяет successful DQ XCom до load_events.

Writer заменяет весь справочник одной Iceberg-транзакцией, включая исчезнувшие месяцы,
без tags. Перед записью проверяются schema/NULL, ключи, namespace событий, условные
поля календаря/акций, пересечение дней с интервалом и соответствие счётчиков отчёту.
Затем exact snapshot read-back сверяет все строки; смена current во время чтения —
ошибка без автоматического отката. Статус written не означает DQ-ready.
81 тест подготовки/чтения/записи прошёл локально; 21 сценарий использует настоящий
Iceberg/SQLite 0.10.0, не Hive/S3 production. Проверяются retry, correction, пустой
реестр, невалидные receipts/batches и конкурентная запись.

## Обновление и оркестрация

Один owner DAG `feature-platform.layers.silver.event_code.demand_event_calendar` с `max_active_runs=1` обновляет полный справочник в `03:10 UTC` и затем запускает DQ и feature statistics.

`feature_stats` отключён: таблица является небольшим полным справочником без
отдельной дневной партиции загрузки, а штатный профиль рассчитан на одну партицию.
DQ при этом проверяет весь точный текущий справочник.

Scheduled run использует точный успешный DQ запуска calendar owner того же интервала. Для ручного обновления сначала запускают calendar owner с `{"mode":"manual"}`, затем этот же events owner с точной ссылкой:

```json
{
  "mode": "manual",
  "calendar_reference": {
    "dag_id": "feature-platform.layers.silver.date.demand_calendar",
    "run_id": "<exact-run-id>",
    "logical_date": "<exact-logical-date>"
  }
}
```

Отдельных history/full-history DAG нет; owner не выбирает `latest` и не запускает upstream скрыто.
