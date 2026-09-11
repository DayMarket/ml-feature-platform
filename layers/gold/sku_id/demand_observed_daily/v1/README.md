# Gold дневной observed-панели SKU

Выход: `iceberg.gold.feature_platform_demand_observed_daily`.
Путь: `layers/gold/sku_id/demand_observed_daily/v1`.
Ключ: `(date, sku_id)`, identity partition по `date`.
DAG: `feature-platform.layers.gold.sku_id.demand_observed_daily`.
Группа DAG: `demand-forecast`.

## Источники и семантика

Gold читает точные прошедшие DQ Iceberg snapshots:

- `iceberg.silver.feature_platform_demand_sales_daily` — продажи по дате создания
  заказа Asia/Tashkent;
- `iceberg.silver.feature_platform_demand_stock_daily` — множество SKU с
  положительным active/FBS EOD на исходную дату.

Полный outer join выполняется по фактическим `(date, sku_id)`, без построения
`catalog × days`. Поля продаж сохраняются как есть. `sales_component_present`
показывает наличие sales-строки, `is_in_stock_eod` — наличие sparse stock-строки.
Для sales-only SKU `is_in_stock_eod=false`; для stock-only SKU поля продаж остаются
NULL. Отсутствие SKU в полностью принятой stock-партиции означает нулевое наличие,
но отсутствующая или не прошедшая DQ партиция означает unknown и блокирует gold.

Панель не содержит уровней запасов, EOD-цен, seller/status/габаритов, календаря,
окон, OOS-эпизодов или модельных X/y. Цены моделей берутся из единой базы цен,
seller и атрибуты SKU — из текущего каталога cutoff. Точные snapshot ID и UUID обоих
входов сохраняются как lineage; локальный Parquet их не экспортирует.

## Запись и проверки

Trino reader использует `FOR VERSION AS OF`, фильтр одного дня и `ORDER BY sku_id`.
До чтения считается точный размер объединения ключей. Порции ограничены
`max_batch_rows` и `max_batch_bytes`; типы Decimal/DATE/TIMESTAMP не преобразуются
неявно. Полученные DQ receipts перечитываются перед commit.

Join выполняется в Arrow по общему префиксу SKU двух отсортированных порций.
В памяти остаются две входные порции и ограниченный выход; полного дневного
join/sort и Python-словарей на SKU нет. Trino DBAPI отдаёт строки порциями:
reader проверяет native-типы и собирает Arrow по колонкам. Fingerprint writer
считает по Arrow-значениям и NULL-маскам, независимо от chunks и null-буферов.

Writer атомарно заменяет один день, проверяет count, схему, порядок ключей и
неизменность головы output. Proof и fingerprints сохраняются в metadata того же
Iceberg commit, затем выполняется полный read-back. Retry возобновляет уже записанные
дни только при совпадении точных входов и содержимого.

DQ выполняется по каждой дате диапазона: ключи, обязательные lineage/presence поля,
положительные ID и условие `sales_component_present OR is_in_stock_eod`. Штатный
`feature_stats` сканирует числовые бизнес-поля; snapshot ID исключены.

## Оркестрация

Owner DAG работает ежедневно в `05:00 UTC`, `max_active_runs=1`, и ждёт `dq` точных
sales/stock запусков. Scheduled run перестраивает последние 31 завершённый день.
Для scheduled правая граница — UTC-дата `data_interval_end`, а не `logical_date`:
в текущем CronDataIntervalTimetable logical date обозначает начало суточного интервала.
Поэтому штатный запуск 10.09.2026 обновляет `[10.08.2026, 10.09.2026)`.
Manual принимает полуоткрытый диапазон и точные references, например август:

```json
{
  "mode": "manual",
  "start": "2026-08-01",
  "end": "2026-09-01",
  "references": {
    "sales": {"dag_id": "...", "run_id": "...", "logical_date": "..."},
    "stock": {"dag_id": "...", "run_id": "...", "logical_date": "..."}
  }
}
```

Дни пишутся последовательно отдельными атомарными commit; один DAG run может
обработать календарный месяц и продолжиться после сбоя.

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

Даже без start/end нужны `references.sales` и `references.stock` на точные
SKU-sales/stock runs для того же окна. Формат ссылки приведён выше.

## Заполнение истории по месяцам

Для каждого logical date D используйте одно и то же окно у всех дневных DAG:

1. Запустите seller-sales и stock; finance можно заполнять независимо.
2. После `dq` seller-sales запустите SKU-sales на D с `reference` на этот run.
3. После `dq` SKU-sales и stock запустите observed на D с `references` на эти runs.

Можно сначала выполнить шаги 1–2 для всей истории, затем шаг 3 для тех же D.
`run_id` и `logical_date` в references берутся из фактического upstream DagRun;
одинаковая logical date не означает одинаковый автоматически выданный manual run_id.
Каждая ссылка должна покрывать все дни выбранного окна. Следует сохранить доступность
upstream XCom `dq` и Iceberg snapshots до выполнения gold. Если snapshot уже истёк,
повторите соответствующий silver-run и передайте его новую ссылку; fallback на latest нет.

При истории с 01.09.2022 до 10.09.2026 последовательность D — 01.10.2022,
01.11.2022, …, 01.09.2026, 10.09.2026. Последний run пишет по 09.09.2026 включительно.
Перекрывающиеся дни заменяются целиком. Для точных календарных месяцев можно вместо
дефолтного окна задавать `start`/`end` первыми числами соседних месяцев.

Календарь и акции загружаются целыми справочниками, затем строится calendar gold;
им не нужны запуски на каждый месяц. Текущие каталоги seller → SKU → tree также
захватываются один раз. Restored demand заполняется отдельно после готовности E3-run.
Миграции и Connections должны быть готовы до запуска этих DAG.
