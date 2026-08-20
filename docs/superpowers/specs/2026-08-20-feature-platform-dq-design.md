# Перенос DQ-тестов из dbt-trino в Feature Platform

Дата: 2026-08-20
Статус: согласовано, готово к разбивке на implementation plan

## 1. Проблема

Сейчас все DQ-тесты репо-управляемых таблиц живут в `dbt-trino` и исполняются отдельными
DAG'ами `dbt.source.trino.ml_feature_platform_<schema>.<table>.dq`. Три следствия:

- DQ-DAG запускается по собственному расписанию и не гарантированно после того DAG'а,
  который писал партицию. Проверка может уехать на данные, которых ещё нет, или наоборот
  подтвердить партицию, которую расчёт ещё дописывает.
- Любое изменение набора тестов требует PR в чужой репозиторий и согласования с DE-инженером.
  ML-инженер не владеет качеством собственной таблицы.
- Набор тестов у разных таблиц расходится, потому что каждая правка — отдельная договорённость.

Цель: DQ становится частью контракта энтити в этом репозитории. ML-инженер объявляет тесты
именами в `config.yaml`, они исполняются таской внутри того же DAG'а сразу после записи,
и downstream ждёт именно эту таску.

## 2. Принятые решения

| Развилка | Решение |
|---|---|
| Движок исполнения | Trino через `TrinoHook`, таблицы адресуются как `"dwh-iceberg".<schema>.<table>` |
| Размещение общего кода | Новый top-level пакет `dq/` (не `layers/_common` — запрещён `AGENTS.md`) |
| Гранулярность | Одна таска `dq` на DAG, все тесты внутри, без остановки на первом падении |
| Severity | `error` / `warn` по-тестно, дефолт `error` |
| Дефолтный набор | Базовые 5 тестов включены всегда, даже без блока `dq:` в конфиге |
| Область данных | Партиция дня по умолчанию; table-wide только там, где иначе нельзя |
| Хранение результатов | Iceberg-таблица `iceberg.silver.feature_platform_dq_results` |
| Визуализация | Superset поверх таблицы результатов; Prometheus/Pushgateway не используем |
| Оповещение | Существующий oncall-webhook (`send_oncall_notification`) с подробным отчётом |
| Миграция downstream | Фазами: сначала таски, потом переключение сенсоров, потом отключение dbt-тестов |
| Судьба dbt-trino | `sources.yaml` остаётся (lineage и доступ чужих dbt-моделей), генерация `tests:`/`freshness:` отключается |

Отвергнуто и почему:

- **Prometheus/Pushgateway.** Проверено 2026-08-20: в `victoria-metrics`
  (uid `P3875CC3B49F869A8`) нет Pushgateway — метрики `push_time_seconds` и `pushgateway_*`
  отсутствуют. Прямая запись в VictoriaMetrics потребовала бы write-endpoint, Airflow-коннекшена
  и сетевого доступа, то есть заявки в инфраструктуру — ровно того согласования, от которого
  уходим. Дополнительно: Airflow-метрики в этом контуре statsd-name-encoded и таймеры протухают
  между запусками, так что Prometheus непригоден для run-status DQ в принципе.
- **Отдельный DQ-DAG внутри репозитория.** Не решает главную проблему — рассинхрон с записью.
- **TaskGroup с таской на тест.** Десятки тасок на DAG, шум в шедулере, downstream пришлось бы
  вешать на join-таску, алерт рассыпался бы на N сообщений.

## 3. Область изменений

Новое:

```
dq/
  __init__.py
  README.md
  config.py
  tests.py
  runner.py
  report.py
  results.py
  task.py
  results/
    config.yaml
    migrations/create_table.sql
ci_test/test_dq_config.py
ci_test/test_dq_sql.py
ci_test/test_dq_task_wiring.py
```

Изменяемое:

- `.airflowignore` — добавить `dq/**`, иначе Airflow парсит пакет как DAG-папку.
- `scripts/run_pyspark_migrations.py` — обход конфигов расширяется на `dq/**/config.yaml`.
- `scripts/sync_iceberg_maintenance.py` — то же.
- `scripts/sync_dbt_sources.py` — на фазе 4 перестаёт рендерить `tests:` и `freshness:`.
- `layers/**/dag.py`, `datasets/**/dag.py` — добавляется таска `dq`.
- `layers/**/config.yaml`, `datasets/**/config.yaml` — опциональный блок `dq:`.
- 14 мест с `ExternalTaskSensor` на `dbt.source.trino.*.dq` + `upload/*/config/factory.py`.
- `AGENTS.md` — разделы «First Rules», «Layer Layout», «DQ And Source Sync», «CI Contracts».
- Grafana `feature-platform-overview` — две DQ-панели.

## 4. Пакет `dq/`

Границы модулей: каждый файл имеет одну зону ответственности и тестируется отдельно.

- **`config.py`** — читает блок `dq:` из `config.yaml`, применяет дефолты, строго валидирует.
  Неизвестное имя теста, отсутствующий обязательный параметр, `severity` вне
  `{error, warn}` — исключение на этапе парсинга DAG'а, а не в рантайме таски.
  Возвращает список описаний тестов и общие настройки прогона.
- **`tests.py`** — реестр тестов. Каждый тест это пара «рендер SQL по параметрам» и
  «разбор строки результата в `TestResult`». Никакого исполнения, никакого Airflow —
  чистые функции, полностью покрываемые снапшот-тестами.
- **`runner.py`** — исполняет отрендеренные запросы через `TrinoHook`, применяет warmup
  и `active_from`, собирает `list[TestResult]`. Делает preflight таблицы до первого теста.
- **`report.py`** — форматирует `list[TestResult]` в текст лога и в текст oncall-сообщения.
- **`results.py`** — идемпотентная запись прогона в Iceberg через `pyiceberg`.
- **`task.py`** — `build_dq_task(config_path)` возвращает готовую Airflow-таску с
  `task_id="dq"` и собственным `on_failure_callback`.

DAG'и подключают пакет через `REPO_ROOT`, вычисленный от `__file__` — паттерн уже используется
в `layers/gold/query_text_version/search_query_id/v1/dag.py:17`.

## 5. Контракт в `config.yaml`

Блок необязателен. Пример полного:

```yaml
dq:
  enabled: true                # default true
  trino_conn_id: trino_search  # default trino_search
  scope: partition             # default partition
  partition_column: date       # default date
  sample_rows: 5               # default 5
  query_timeout_seconds: 600   # default 600
  warmup_days: 0               # default 0 (выключено)
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
- `dq.enabled: false` выключает DQ у энтити целиком; требует объяснения в README энтити,
  CI проверяет наличие такого объяснения.

Синтаксис безопасен для простого CI-парсера `_read_simple_config`
(`scripts/sync_dbt_sources.py:270`): он читает только ключи `table.*`, а элементы списков
пропускает — как уже происходит с `source.elasticsearch.fields` в
`layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/config.yaml`.
Сами DAG'и читают конфиг через `yaml.safe_load`, списки для них полноценны.

## 6. Каталог тестов

Область данных по умолчанию — партиция `<partition_column> = <partition_date>`.
Колонка «Скан» указывает тесты, которым по смыслу нужно больше одной партиции.

### Базовые (включены всегда)

| Имя | Параметры | Дефолт | Скан | Что проверяет |
|---|---|---|---|---|
| `primary_key_not_null` | — | берёт `table.primary_key` | партиция | ни одна PK-колонка не NULL |
| `primary_key_unique` | — | берёт `table.primary_key` | партиция | нет дублей по комбинации PK |
| `row_count_min` | `min_rows` | `1` | партиция | в партиции есть данные |
| `row_count_growth` | `max_growth_ratio` | `0.2` | 2 партиции | \|rows(d)/rows(d-1) − 1\| ≤ порога |
| `freshness` | `max_lag_days` | `2` | table-wide | `max(date)` не отстаёт больше чем на N дней |

Соответствие сегодняшнему `render_source_yaml` (`scripts/sync_dbt_sources.py:298`) — один в один,
кроме одного осознанного ужесточения: `min_rows` меняется с `0` на `1`. Сегодняшний `min_rows: 0`
делает тест всегда зелёным, то есть пустая партиция проходит. Энтити с легально пустыми днями
переопределяют значение у себя.

### Опциональные

| Имя | Обязательные параметры | Опциональные | Что проверяет |
|---|---|---|---|
| `not_null` | `columns` | `max_null_share` | перечисленные колонки не NULL |
| `accepted_values` | `column`, `values` | `ignore_nulls` | значения из закрытого списка |
| `accepted_range` | `column`, `min`/`max` | `min_inclusive`, `max_inclusive`, `ignore_nulls` | значение в диапазоне |
| `non_negative` | `columns` | `ignore_nulls` | шорткат `accepted_range` с `min: 0` |
| `null_share_below` | `column`, `max_share` | — | доля NULL ниже порога |
| `distinct_count_between` | `columns` | `min`, `max` | кардинальность в границах |
| `expression_is_true` | `expression` | `where` | построчное булево правило |
| `columns_sum_equals` | `parts`, `total` | `tolerance` | сумма частей равна тоталу |
| `unique_combination` | `columns` | — | уникальность по не-PK комбинации |
| `string_not_blank` | `columns` | — | строка не пустая и не из одних пробелов |
| `relationships` | `column`, `to_table`, `to_column` | `where` | внешний ключ существует |

У каждого теста дополнительно доступны `severity` (`error` \| `warn`, дефолт `error`) и `where`
для сужения выборки.

`relationships` помечается в `dq/README.md` как дорогой: он сканирует чужую таблицу целиком.
По умолчанию не включён нигде; добавляется только когда контракт join-ключа стабилен,
и в README энтити фиксируется, почему затраты оправданы.

Экранирование: все строковые значения в `accepted_values` и в `where` проходят через
единый хелпер квотирования; тесты `ci_test/test_dq_sql.py` содержат кейсы с кавычками и
не-ASCII, чтобы отловить SQL-инъекцию через конфиг.

## 7. Поведение на молодых таблицах и бэкфилле

Три независимых механизма, закрывающих три разных случая.

**Отсутствие базы для сравнения.** `row_count_growth` требует партицию `d-1`. Если её нет,
или в ней ноль строк, коэффициент роста не определён. Такой тест возвращает статус
`skipped` с причиной (`no baseline partition 2026-08-19`), а не `passed` и не `failed`.
Поведение встроенное и не настраиваемое: тест, которому не на чем считать, не имеет права
ни зеленить таблицу, ни ронять DAG. Статус `skipped` пишется в таблицу результатов наравне
с остальными, чтобы в Superset было видно, что проверка не работала — а не что всё хорошо.

**Пороги, откалиброванные на зрелых данных.** `dq.warmup_days` (дефолт `0`, выключено):
пока число различных значений `partition_column` в таблице меньше N, все результаты
severity `error` понижаются до `warn`. Отчёт в этом режиме явно печатает
`warmup active: 3/14 days`, чтобы факт снятой защиты был виден в логе и в таблице
результатов. Инженер ставит `warmup_days` при заведении новой энтити и снимает,
когда пороги устоялись.

**Бэкфилл истории с другим качеством.** `dq.active_from: "2026-08-01"` — для партиций
раньше указанной даты тесты не запускаются вообще, таска зелёная, в лог пишется причина.
Применяется только когда старые партиции считались другой логикой и чинить их не планируется.

**Preflight.** До первого теста runner проверяет существование таблицы в каталоге.
Если её нет, падает с сообщением, содержащим полное имя `<catalog>.<schema>.<name>` и
указание, что миграция не была применена, — а не сырым `TABLE_NOT_FOUND` из Trino.
Это тот же контракт, что описан в `AGENTS.md` в разделе «PyIceberg Catalog And Identifier Contract».

## 8. Таблица результатов

`iceberg.silver.feature_platform_dq_results`, партиционирование по `date` (days).

Колонки: `date` (проверявшаяся партиция), `run_ts`, `dag_id`, `task_id`, `run_id`,
`try_number`, `catalog`, `schema_name`, `table_name`, `test_name`, `test_key`,
`status` (`passed` \| `failed` \| `warned` \| `skipped` \| `errored`), `severity`,
`failed_rows`, `observed` (наблюдаемое числовое значение — доля, коэффициент, счётчик),
`threshold` (человекочитаемый порог), `params` (JSON параметров теста), `sql_text`,
`sample` (примеры нарушающих строк), `duration_ms`, `skip_reason`, `warmup_active`.

`test_key` — уникальный в рамках таблицы идентификатор теста с параметрами,
например `accepted_range[conversion_rate]`. Нужен, чтобы два `accepted_range` по разным
колонкам не схлопывались в одну строку.

`table.primary_key: date,dag_id,test_key`. Запись идемпотентна: перед вставкой строки
текущего `(date, dag_id)` перезаписываются через `pyiceberg` overwrite с выражением
`And(EqualTo("date", d), EqualTo("dag_id", dag_id))`, поэтому ретрай таски не плодит дубли.

`table.meta.create_dbt_pr: false` — таблица не уезжает в `dbt-trino` и не обзаводится
собственными DQ-тестами рекурсивно. `create_maintenance_pr: true` — таблица растёт
ежедневно, компакция нужна.

Из-за размещения в `dq/results/` придётся расширить обход конфигов в
`scripts/run_pyspark_migrations.py` и `scripts/sync_iceberg_maintenance.py` на
`dq/**/config.yaml`. Оба скрипта сейчас ходят только по `layers/**` и по `AGENTS.md`
всё равно подлежат починке под `datasets/**`.

## 9. Интеграция в DAG

Одна таска `dq` в конце графа, `trigger_rule="all_success"`:

```
... > load_to_iceberg > dq
```

Для Spark-энтити таска ставится после `SparkKubernetesOperator` и сама Spark не поднимает —
она ходит в Trino. Для Airflow/Python-энтити — обычная `@task` в том же поде.

Партиция берётся тем же выражением, что и запись. У энтити с `dag_run.conf["partition_date"]`
(например `search_query_sku_group_es_features`) — оттуда, с фолбэком на `macros.ds_add(ds, -1)`.
У энтити, пишущих `{{ ds }}` — оттуда. Выражение читается из конфига энтити, не угадывается
по имени таблицы.

Ретраи таски `dq`: `retries: 1`, без экспоненты. Один ретрай гасит флап Trino, больше —
маскирует настоящее падение.

## 10. Отчёт и оповещение

`TestResult` = `name, test_key, status, severity, failed_rows, observed, threshold, duration_ms, sql, sample_rows, skip_reason`.

Лог таски:

```
DQ  iceberg.silver.feature_platform_sku_group_id_prices  date=2026-08-19  warmup: off
PASS  primary_key_not_null       0 rows              1.2s
FAIL  primary_key_unique         1843 dup key groups 8.7s  severity=error
WARN  accepted_range[price]      12 rows > max=1e9   0.9s  severity=warn
SKIP  row_count_growth           no baseline partition 2026-08-18
...
--- FAIL primary_key_unique ---
threshold : 0 duplicate key groups
observed  : 1843
sql       : SELECT ... GROUP BY date, sku_group_id HAVING count(*) > 1 ...
samples   : (2026-08-19, 118823) x2 | (2026-08-19, 940112) x3 | ...
```

Все тесты гоняются до конца, отчёт печатается целиком, и только после этого таска падает,
если есть хоть один `error`. `warn` и `skipped` в статус таски не влияют, но попадают
и в лог, и в алерт, и в таблицу результатов.

Оповещение: тот же отчёт, обрезанный до лимита сообщения, плюс полное имя таблицы, партиция,
число упавших тестов и прямая ссылка на лог таски. Канал — существующий
`send_oncall_notification` из блока `alerts:` энтити, повешенный на `on_failure_callback`
**самой таски** `dq`, а не всего DAG'а: алерт про качество данных должен читаться иначе,
чем алерт про падение расчёта.

`sample_rows` тянет только PK-колонки плюс колонки, участвующие в конкретном тесте.
Весь ряд в лог и в мессенджер не уезжает.

## 11. Superset и Grafana

Основная аналитика — Superset поверх `feature_platform_dq_results`: последний прогон
по таблице, топ падающих тестов за период, тренд `failed_rows` и `observed` по конкретному
тесту, доля `skipped`.

Grafana `feature-platform-overview` правится по-минимуму. Обе существующие DQ-панели
(«Ошибки DQ (14д)» и «DQ-статус по таблице») фильтруют `silver.airflow_dag_run` по
`dag_id LIKE 'dbt.source.trino.ml_feature_platform_%.dq'` и после фазы 4 начнут молча
показывать пустоту. Они переставляются на `"dwh-iceberg".silver.airflow_task_instance`
с условием `task_id = 'dq' AND dag_id LIKE 'feature-platform.%'` — таблица проверена
2026-08-20, нужные колонки (`dag_id, task_id, state, start_date, end_date, duration, try_number`)
на месте. Датасорс тот же `Trino-prod` (uid `QHoiJYzSk`); в таргетах обязателен
`format: 1` — при `0` панель молча рисует «No data».

## 12. Миграция downstream

Сегодня 14 мест ссылаются на `dbt.source.trino.ml_feature_platform_<schema>.<table>.dq`
через `external_dag_id`. Новый контракт: `external_dag_id=<dag id владельца таблицы>`,
`external_task_id="dq"`.

В `upload/*/config/factory.py` `external_task_id` в `ExternalTaskSensor` уже прокинут
(`upload/features_service_upload/v1/dag.py:66`), меняется только способ собрать пару
dag/task в `_source_dependencies`.

Отдельная аккуратность нужна для DAG'ов без расписания: у них `execution_delta` неприменим,
требуется `execution_date_fn` — образец есть в
`layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1/dag.py:95`.

## 13. CI

- `ci_test/test_dq_config.py` — все блоки `dq:` в репозитории валидны. Неизвестное имя теста,
  отсутствующий обязательный параметр, колонка, которой нет в миграциях энтити,
  `dq.enabled: false` без объяснения в README — падение.
- `ci_test/test_dq_sql.py` — снапшоты отрендеренного SQL для каждого из 16 тестов, включая
  кейсы с кавычками и не-ASCII в значениях.
- `ci_test/test_dq_task_wiring.py` — в каждом DAG есть таска `dq`, она терминальная,
  и (после фазы 3) ни один сенсор не ссылается на `dbt.source.trino.ml_feature_platform_*`.
- Существующие `ci_test/test_run_pyspark_migrations.py` и `ci_test/test_sync_iceberg_maintenance.py`
  дополняются кейсами на обход `dq/**/config.yaml`.

## 14. Фазы

**Фаза 0 — фундамент.** Пакет `dq/`, таблица результатов с миграцией, расширение обхода
конфигов в двух CI-скриптах, `.airflowignore`, три новых CI-теста, `dq/README.md`,
правки `AGENTS.md`. Ни один существующий DAG не меняется.

**Фаза 1 — пилот.** Таска `dq` в двух энтити разных типов:
`layers/silver/sku_group_id/sku_group_id_prices/v1` (Spark, по расписанию) и
`layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1`
(Airflow/Python, `schedule=None`, партиция из `dag_run.conf`). Неделя параллельной работы
с dbt-DQ, сверка вердиктов на одних и тех же партициях.

**Фаза 2 — раскатка.** Таска `dq` во все остальные энтити `layers/**` и `datasets/**`.
Базовый набор везде; содержательные тесты — там, где контракт однозначно читается
из README и миграции. Где не читается — вопрос владельцу энтити, а не догадка.

**Фаза 3 — переключение зависимостей.** 14 сенсоров и `upload/*/config/factory.py`
переводятся на `external_task_id="dq"`. dbt-DQ ещё работает, так что откат — это revert одного PR.

**Фаза 4 — отключение dbt-тестов.** `sync_dbt_sources.py` перестаёт рендерить `tests:`
и `freshness:`, продолжая писать source-блоки. Один согласующий PR с DE; DQ-DAG'и уходят
сами. Панели Grafana переставляются на `airflow_task_instance`, дашборды строятся в Superset.

## 15. Риски

- **`min_rows` с 0 на 1.** Ужесточение относительно сегодняшнего поведения. Энтити с легально
  пустыми днями упадут на фазе 2. Митигация: на пилоте и в первую неделю фазы 2 смотрим
  результаты, для найденных энтити выставляем `min_rows: 0` явно с пояснением в README.
- **Стоимость Trino.** Партиционный скан дешёвый, но `freshness` и `relationships` — нет.
  `freshness` считает `max(date)`, что на Iceberg решается по метаданным партиций;
  `relationships` по умолчанию выключен.
- **Расхождение вердиктов с dbt на пилоте.** Ожидаемо для `primary_key_unique`: dbt проверяет
  уникальность по всей истории таблицы, новый тест — по партиции. Это осознанная разница,
  зафиксировать в `dq/README.md`; при необходимости конкретная энтити ставит `scope: table`.
- **Простой парсер CI.** Любое усложнение синтаксиса блока `dq:` рискует сломать
  `_read_simple_config`. Митигация: `ci_test/test_dq_config.py` прогоняет оба парсера
  (простой и `yaml.safe_load`) по всем конфигам репозитория.

## 16. Что не входит

- Автоматическое удаление DQ-записей из `dbt-trino` — это PR в чужой репозиторий на фазе 4.
- Дашборды Superset — строятся после фазы 2, отдельной задачей, вне этого репозитория.
- Anomaly detection и статистические тесты на дрейф распределений. Текущий набор —
  детерминированные правила, как в dbt.
- Проверки на upstream-таблицах DE-команд. Для них контракт остаётся за владеющей командой.
