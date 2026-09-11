# Дневная FP-копия E3

Утверждён режим первого выпуска: завершённые E3-runs не удаляются и не изменяются.
Отдельный hold registry и очистка отложены. Подключён обязательный ImmutableRunGuard:
декларация producer fp_source_policy, UUID/движки/отсутствие TTL и незавершённых mutations.
Установка callback True не реализует защиту. Status prepared_local не разрешает запуск:
owner и зависимые DAG остаются выключенными до новой публикации E3 и deployment audit.
В config заданы whole-run бюджеты regular 6 часов/manual 7 суток, включая
retry/DQ. Общий guard подключён ко всем owner tasks; source policy повторяется до/после DQ/stats.

## Output

`iceberg.silver.feature_platform_demand_restored_daily`, 31 колонок.
Путь `layers/silver/sku_id_estimate_kind/demand_restored_daily/v1`.
Ключ `date,sku_id,estimate_kind`, estimate_kind = provisional/final.
run_id — lineage в колонке, не часть ключа. Новая выбранная версия заменяет день;
prediction_date — модельный cutoff по Asia/Tashkent, а не UTC-день запуска Airflow.
накопления всех run в этой таблице нет. Immutable training package фиксируется отдельно.
Подтверждено владельцем: final-only полного нового run удаляет прежний provisional
за этот день. Наличие обеих оценок не требуется. Соседние даты сохраняются.

## Источник и перенос

CH `sku_sales_forecast.demand_forecast_demand_panel` и паспорт
`sku_sales_forecast.demand_forecast_run`, connection `clickhouse_dwh_team_logistics`.
Только явно выбранные prediction_date/run_id, stage=e3, status=validated/published,
положительная state_version, законченный run, input/output manifest.
written/running/failed запрещены. Состояние published не заменяет проверку полноты
output manifest и удержание source run до завершения read-back/DQ.
PREWHERE отсекает другие runs до FINAL. Без current/latest, смешения нескольких runs,
повторного расчёта спроса/цены/GMV или отбрасывания unavailable строк.

Исходные Float64 и NULL сохраняются, UTC-нормализация только у времени записи.
У E3 GMV в основных единицах currency_code: это не raw денежный масштаб order_items.
USD заново не вычисляется. Provisional/final не являются mean/quantile.
Наличие даты старше 180 дней не создаёт final автоматически: копируется source version.
Passthrough не выдаёт incomplete результат за проверенный: нужны полные day/estimate
counts и checksum из источника, точный state и source hold на время переноса.
rate_ok хранится как Iceberg INT/Arrow int32 с доменом 0/1; source CH UInt8 не меняется.

## Дневной manifest v1

В JSON output_manifest обязателен блок fp_daily_copy: version=1,
checksum_algorithm=e3_daily_rows_sha256_v1, table=<database>.<table> из source config,
точные run_id/prediction_date и days. Каждая запись days содержит ровно
date (ISO DATE), rows (положительное целое), estimate_counts (непустой словарь
provisional/final → положительное целое), sha256 (64 lowercase hex).
Сумма counts равна rows. Дни строго возрастают, каждый запрошенный день представлен.
Manifest может покрывать более широкий диапазон. Пропущенный/пустой день блокирует,
не удаляет старые данные и не считается доказанным нулём. Повторы JSON ключей запрещены.

Канонический wire задан в job/manifest.py RAW_COLUMNS: все 24 исходных поля в
фиксированном порядке, без FP metadata. date ← event_date, source_updated_at ←
updated_at с UTC-нормализацией, остальные имена исходные. Порядок строк
(date,sku_id,estimate_kind), строковый final перед provisional, не порядок CH Enum.
Payload строки — JSON-массив, ensure_ascii=true, separators=(",", ":"), allow_nan=false,
UTF-8: DATE ISO, Float64 как float.hex(), timestamp UTC с микросекундами и Z,
целые/строки/NULL в соответствующих JSON типах. Начало SHA-256 —
ASCII "e3_daily_rows_sha256_v1\\n" (последний символ — перевод строки).
Для каждой строки добавляются длина payload (8-byte unsigned big-endian) и payload.
Порции Arrow и версия Iceberg на digest не влияют; исходный -0.0 не округляется.
Source formatter e3_restore.daily_manifest в модельном репозитории совместим с этим
wire, но его подключение к E3 расчёту/паспорту ещё не выполнено.

Loader preflight проверяет target/schema/identity partition/лимиты до CH чтения.
Затем читает один точный паспорт и требует реальный hold, читает день порциями,
сверяет counts/digest и повторно проверяет неизменный паспорт/hold перед commit.
Legacy manifest без блока не принимается. После commit — полный read-back
по composite key, включая содержимое и исходный digest. Resume дополнительно
требует совпадения run/cutoff/state/output manifest и не создаёт нового snapshot.
Receipt остаётся written, не passed DQ. Контрольные суммы — обычная metadata, не tags.

## Перенос нескольких дней

`job/ranges.py` принимает список полуоткрытых диапазонов одного run/cutoff от E3.
Запрос содержит copy_id, точные даты, диапазоны и digest конфигурации; он JSON-совместим.
Неверный порядок, пересечения, смешение runs и изменение запроса/конфигурации блокируют.
Промежутки между диапазонами не заполняются. FP не рассчитывает зрелость: производитель
E3 передаёт свой план (181 день при ежедневном расчёте плюс пропуски/исправления).

До первой записи проверяются target/схема/partition, обязательный service preflight,
готовый паспорт, manifest всех запрошенных дней и hold. Один state/output manifest
закрепляется на весь перенос. Его смена между днями, перед дневным commit или после
диапазона блокирует выдачу успешного receipt. Изменение target между днями также блокирует.
Ранее завершённые дни при сбое остаются видимыми: атомарность по дням, не всему запросу.
Retry сверяет текущие данные и checkpoint каждого дня, не выгружая совпавший день повторно.
Hold обязателен и при resume. Внешняя сериализация writer + DQ всё ещё обязательна.

`require_range_dq` проверяет receipts сохранённого DQ каждого выбранного дня:
единый итоговый snapshot/table UUID, request_id, source state/manifest и число строк.
Missing/чужой/частичный DQ не превращает written в ready. Эта функция **не исполняет DQ**.
Исполнитель теперь подготовлен в top-level `dq.day_range` и штатной фабрике task=dq:
SQL каждого дня на итоговом snapshot, проверка counts/меток/capture и сохранение
результатов до общего допуска. Range feature_stats проверяет все дни того же snapshot
параллельно DQ. Обе задачи подключены к owner и используют общий timeout запуска.
Наличие range runtime не разрешает удалять или перезаписывать завершённые source runs.

## Оркестрация и запуск

У таблицы один ручной owner DAG `feature-platform.layers.silver.sku_id_estimate_kind.demand_restored_daily` с `schedule=None` и `max_active_runs=1`. Он принимает только точные E3 `selections`; отдельный `mode` не нужен:

```json
{
  "selections": [
    {
      "run_id": "<exact-e3-run-id>",
      "prediction_date": "2026-09-08",
      "start": "2026-08-01",
      "end": "2026-09-01"
    }
  ]
}
```

Несколько непересекающихся диапазонов разрешены. Owner проверяет source passport, переносит даты атомарно с resume, затем запускает DQ и feature statistics по всем выбранным датам. Отдельного history/full-history DAG нет.
