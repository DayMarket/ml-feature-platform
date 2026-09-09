# DQ в Feature Platform

DQ-тесты репозиторно-управляемых таблиц объявляются в блоке `dq:` файла `config.yaml`
энтити и исполняются таской `dq` внутри того же DAG'а сразу после записи партиции.
Downstream-DAG'и ждут именно эту таску, а не отдельный dbt-DQ-DAG.

Тесты выполняются как SQL в Trino через `TrinoHook`; таблица адресуется как
`"dwh-iceberg".<schema>.<table>` (маппинг каталога берётся из `ci_config.yaml`).
Каждый тест рендерится в запрос, возвращающий одну строку `(failed_rows, observed)`:
`failed_rows > 0` — падение, `0` — успех, `< 0` — тест не мог быть выполнен и получает
статус `skipped`.

## Быстрый старт

Блок `dq:` необязателен: базовый набор работает всегда. Добавляйте только то, что сверх него.

```yaml
dq:
  enabled: true                # default true
  trino_conn_id: trino_search  # default trino_search
  scope: partition             # default partition; table — таблица без колонки партиции
  partition_column: date       # default date
  partition_granularity: date  # default date; timestamp — энтити-снапшот
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  sample_rows: 5               # default 5
  query_timeout_seconds: 600   # default 600
  warmup_days: 1               # default 1
  # active_from: "2026-08-01"  # по умолчанию ключ отсутствует
  tests:
    - name: row_count_min
      min_rows: 50000
    - name: accepted_range
      column: conversion_rate
      min: 0
      max: 1
    - name: not_null
      columns: [orders_cnt, price_avg]
    - name: expression_is_true
      expression: "min_price <= median_price AND median_price <= max_price"
      severity: warn
```

Правила:

- Базовые тесты работают всегда, независимо от наличия блока `dq:`.
- Запись в `tests` с именем базового теста переопределяет его параметры.
- `enabled: false` внутри записи выключает конкретный тест.
- `dq.enabled: false` выключает DQ у энтити целиком и требует объяснения в README энтити —
  `scripts/validate_dq_configs.py` это проверяет.
- У каждого теста доступны `severity` (`error` | `warn`, дефолт `error`) и `where`.
- `partition_date_template` должен повторять то выражение, которым DAG выбирает
  записываемую партицию. Не угадывайте его по имени таблицы.

## Снапшотные энтити

Часть энтити пишет не дневную партицию, а снапшот: DAG идёт несколько раз в сутки и
каждый запуск добавляет новый `TIMESTAMP` в ту же дневную партицию. Проверять дневную
партицию у такой таблицы неверно — на момент запуска она дописана лишь частично, а
`row_count_growth` сравнивал бы полудописанный день с полным вчерашним.

```yaml
dq:
  partition_granularity: timestamp
  partition_column: calculated_at
  snapshot_interval_hours: 3     # обязан совпадать с шагом расписания DAG'а
  partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
```

Что меняется при `partition_granularity: timestamp`:

- фильтр партиции становится `<column> = TIMESTAMP '<snapshot> UTC'` — проверяется
  ровно записанный снапшот;
- база `row_count_growth` — предыдущий снапшот (`минус snapshot_interval_hours`), а не
  предыдущие сутки;
- `freshness` меряет лаг в часах, а `max_lag_days` пересчитывается как `N * 24` часа:
  для трёхчасового DAG'а сутки — это уже 8 пропущенных запусков;
- `warmup_days` считает различные снапшоты, а не дни.

`snapshot_interval_hours` и `partition_date_template` обязательны: без первого база
роста уедет на несуществующий снапшот и тест навсегда останется `skipped`, без второго
дефолтный шаблон отдал бы дату без времени и таска упала бы на разборе.

**Литерал времени всегда с зоной.** Сессия Trino живёт не в UTC (сейчас
`Europe/Moscow`), а снапшотные колонки — `timestamp with time zone`. Голый
`TIMESTAMP '2026-08-22 06:00:00'` коэрсится по таймзоне сессии и молча указывает на
соседний снапшот: на реальных данных два таких запроса вернули 15 654 063 и
15 657 791 строки. Поэтому рендерер всегда пишет `TIMESTAMP '... UTC'`, а шаблон
партиции обязан отдавать время именно в UTC.

## Каталог тестов

### Базовые — включены всегда

| Имя | Семейство | Параметры | Дефолт | Скан |
|---|---|---|---|---|
| `primary_key_not_null` | null_checks | — | берёт `table.primary_key` | партиция |
| `primary_key_unique` | uniqueness | — | берёт `table.primary_key` | партиция |
| `row_count_min` | consistency | `min_rows` | `0` | партиция |
| `row_count_growth` | consistency | `max_growth_ratio`, `direction` | `0.2`, `both` | 2 партиции |
| `freshness` | recency | `max_lag_days` | `2` | table-wide |

`row_count_min` повторяет семантику dbt-макроса `row_count_greater_than_for_date` дословно,
включая нестрогое сравнение: тест падает при `row_count <= min_rows`. Дефолт `0` означает
«партиция не должна быть пустой», а не «любое число строк сойдёт».

### Опциональные

| Имя | Семейство | Обязательные параметры | Опциональные |
|---|---|---|---|
| `not_null` | null_checks | `columns` | `max_null_share` |
| `null_share_below` | null_checks | `column`, `max_share` | — |
| `unique_combination` | uniqueness | `columns` | — |
| `accepted_values` | domain_values | `column`, `values` | `ignore_nulls` |
| `not_accepted_values` | domain_values | `column`, `values` | `ignore_nulls` |
| `accepted_range` | domain_values | `column`, `min`/`max` | `min_inclusive`, `max_inclusive`, `ignore_nulls` |
| `non_negative` | domain_values | `columns` | `ignore_nulls` |
| `string_not_blank` | domain_values | `columns` | — |
| `distinct_count_between` | consistency | `columns`, `min`/`max` | — |
| `columns_sum_equals` | consistency | `parts`, `total` | `tolerance` |
| `row_count_matches_reference` | consistency | `reference_table`, `reference_date_column` | `reference_where`, `tolerance_ratio` |
| `expression_is_true` | row_expr | `expression` | `where` |
| `relationships` | referential_integrity | `column`, `to_table`, `to_column` | `where` |

Семейства совпадают с таксономией `dbt-trino/scripts/check_model_tests_score.py` — это общий
словарь с DE-командой, когда речь идёт о покрытии таблицы тестами.

`expression_is_true` считает нарушением любую строку, где выражение не вычислилось в `TRUE`:
проверка идёт через `(<expression>) IS DISTINCT FROM TRUE`, поэтому `NULL` тоже считается падением.
Предиката `IS NOT TRUE` в Trino нет — само выражение из конфига тоже пишите на Trino-диалекте.
Если `NULL` для вашего контракта допустим — исключите его явно в самом выражении.

### Дорогие тесты

`relationships` сканирует чужую таблицу целиком, `row_count_matches_reference` — партицию
чужой таблицы. По умолчанию не включены нигде. Добавляйте только когда контракт join-ключа
стабилен, и фиксируйте в README энтити, почему затраты оправданы.

## Молодые таблицы и бэкфилл

**Нет базы для сравнения.** `row_count_growth` требует предыдущую партицию (для
снапшотной энтити — предыдущий снапшот). Если её нет или
она пуста, коэффициент роста не определён, и тест получает статус `skipped` с причиной, а не
`passed` и не `failed`. Поведение встроенное и не настраиваемое: тест, которому не на чем
считать, не имеет права ни зеленить таблицу, ни ронять DAG. `skipped` попадает в таблицу
результатов наравне с остальными, чтобы в Superset было видно, что проверка не работала.

**Непрогретые пороги.** `dq.warmup_days` (дефолт `1`): warmup активен, пока число различных
партиций в таблице **до текущей** меньше `warmup_days`. При дефолте `1` это ровно первый день
жизни таблицы. Пока warmup активен, все результаты severity `error` понижаются до `warn`,
а отчёт печатает `warmup: ACTIVE` и пишет флаг `warmup_active` в таблицу результатов.
Для энтити с долгим набором данных ставьте `warmup_days: 14` и снимайте, когда пороги устоятся.
При `scope: table` warmup обязан быть выключен (`warmup_days: 0`) — считать нечего, см. ниже.

**Таблица без партиции.** `dq.scope: table` описывает таблицу, у которой колонки партиции нет
вообще (справочник, append-only словарь). `scope` гасит ровно одно место — предикат партиции в
`WHERE` (`scope_predicate` возвращает `TRUE`). Всё остальное рендерит SQL по
`dq.partition_column` независимо от `scope`: `freshness` (`_render_freshness`),
`row_count_growth` (`_render_row_count_growth`), warmup (`runner._partition_history_count`) и
таска `feature_stats` (`render_stats_query`). Поэтому `scope: table` требует выключить их все
разом:

```yaml
dq:
  scope: table
  warmup_days: 0
  tests:
    - name: freshness
      enabled: false
    - name: row_count_growth
      enabled: false

feature_stats:
  enabled: false
```

`load_dq_settings` и `load_feature_stats_settings` не собирают конфиг, в котором что-то из этого
осталось включённым: иначе Trino отвечает `COLUMN_NOT_FOUND`, и таска падает с вызовом дежурного
раньше, чем появляется хоть один результат теста.

**Бэкфилл истории.** `dq.active_from: "2026-08-01"` — для партиций раньше указанной даты тесты
не запускаются вообще. Применяйте только когда старые партиции считались другой логикой и
чинить их не планируется.

**Preflight.** До первого теста runner проверяет, что таблица есть в каталоге Trino. Если нет —
падает с сообщением про непроехавшую миграцию, а не с сырым `TABLE_NOT_FOUND`. Запрос идёт в
`"<trino-catalog>".information_schema.tables`: дефолтный каталог соединения `trino_*` — `hive`,
и неквалифицированное имя вернуло бы `CATALOG_NOT_FOUND`.

## Таблица результатов

`iceberg.silver.feature_platform_dq_results`, партиционирование по `date`, конфиг и миграция
в `dq/results/`. Каждый прогон пишет строку на тест: статус, severity, `failed_rows`,
`observed`, порог, JSON параметров, отрендеренный SQL, примеры нарушающих строк, длительность,
причину пропуска и флаг warmup.

Колонка `team` хранит команду-владельца проверявшейся таблицы: берётся из `table.meta.team`
энтити, при отсутствии — `team:search`. По ней в Superset режется дашборд на зоны
ответственности, поэтому строка результата никогда не остаётся без адресата.

Запись идемпотентна: строки текущей пары `(date, dag_id)` перезаписываются целиком, поэтому
ретрай таски не плодит дубли.

Если параллельный DQ изменил общую Iceberg-ветку между чтением metadata и commit, writer
повторно загружает актуальную metadata и повторяет только запись результатов с exponential
backoff и jitter. Уже выполненные DQ-запросы при этом не пересчитываются.

Таблица помечена `create_dbt_pr: false` — она не уезжает в `dbt-trino` и не заводит DQ-тесты
сама на себя. Модуль записи называется `dq/results_writer.py`, а не `results.py`, потому что
каталог `dq/results/` как namespace-пакет перекрыл бы имя `dq.results`.

Аналитика строится в Superset поверх этой таблицы. Статус самой таски `dq` виден в
`"dwh-iceberg".silver.airflow_task_instance` по `task_id = 'dq'`.

## Отличия от прежнего поведения dbt

| Что | Раньше в dbt | Сейчас |
|---|---|---|
| `row_count_growth` направление | односторонний: ловил только рост выше порога, обвал до 10% проходил | двусторонний по умолчанию, `direction: up`/`down` при необходимости |
| `row_count_growth` без базы | `WHERE previous_row_count > 0` — молча проходил | статус `skipped` с причиной |
| `primary_key_unique` | table-wide по всей истории | по партиции; `scope: table` при необходимости |
| `not_null` по PK | генерировался, но фактически был у одной таблицы из 39 | работает везде |
| Упавшие строки | `+store_failures: false`, не сохранялись | `failed_rows`, `observed` и примеры в таблице результатов |
| Смещение дат | «текущая» партиция = `ds - 1`, база = `ds - 2` | ровно записанная партиция и `partition_date - 1` |

## Локальные проверки

```bash
python3 scripts/validate_dq_configs.py
python3 ci_test/test_dq_config.py
python3 ci_test/test_dq_sql.py
python3 ci_test/test_dq_runner.py
python3 ci_test/test_dq_report.py
python3 ci_test/test_dq_results.py
python3 ci_test/test_dq_task.py
python3 ci_test/test_dq_task_wiring.py
python3 ci_test/test_validate_dq_configs.py
```
