# Статистики признаков для таблиц, выгружаемых в инференс

Дата: 2026-08-23
Статус: согласовано, готово к разбивке на implementation plan

## 1. Проблема

Feature Platform умеет отвечать на вопрос «данные валидны?» — этим занимается таска `dq`.
Она не отвечает на вопрос «данные те же, что вчера?». DQ-тест `not_null` пройдёт и на
партиции, где медиана признака уехала втрое; `row_count_growth` пройдёт на партиции, где
все конверсии схлопнулись в ноль при сохранении числа строк.

Модели ранжирования обучаются на распределениях, а инференс кормится тем, что выгружено
в feature service. Сдвиг распределения признака между обучением и инференсом (training/serving
skew) сегодня в репозитории ничем не измеряется и обнаруживается только по метрикам качества
поиска — то есть постфактум и без указания на конкретный признак.

Цель: для каждой репозиторно-управляемой таблицы, которая выгружается в сервис инференса,
на каждой записанной партиции считать профиль распределения каждого признака и складывать
его в Iceberg-таблицу. Это даёт основу для дашбордов дрейфа и для будущих автоматических
порогов, но само по себе ничего не блокирует.

## 2. Принятые решения

| Развилка | Решение |
|---|---|
| Движок расчёта | Trino через `TrinoHook`, один запрос на таблицу, `approx_percentile` |
| Размещение кода | Новый top-level пакет `feature_stats/`, зеркало `dq/` |
| Гранулярность таски | Одна таска `feature_stats` на DAG, параллельно таске `dq` |
| Набор колонок | Все числовые колонки из `information_schema` минус PK, колонка партиции и `exclude_columns` |
| Набор метрик | `count`, `null_share`, `mean`, `min`, `max`, перцентили `0.05 0.1 0.25 0.5 0.75 0.9 0.95` |
| Набор перцентилей | Жёстко зашит в код, не конфигурируется |
| Конфиг | Свой полный блок `feature_stats:` в `config.yaml` энтити |
| Согласованность с DQ | CI падает, если partition-настройки блоков `dq:` и `feature_stats:` расходятся |
| Хранение | Iceberg-таблица `iceberg.silver.feature_platform_feature_stats` |
| Влияние на публикацию | Нулевое: сенсоры upload'а остаются на `external_task_id="dq"` |

Отвергнуто и почему:

- **Spark.** Дал бы изолированный компьют и точные квантили, но потребовал бы entrypoint,
  SparkApplication-шаблон, resource-профиль и второй под на каждый DAG-ран (7 DAG'ов,
  из них один с 8 ранами в сутки). Максимальная работа здесь — один проход по 32M строк
  на 24 колонки, это секунды; оверхед запуска пода (1–3 минуты) превысил бы саму задачу
  на порядок. Возвращаемся к Spark, только если понадобятся точные квантили или разрезы
  по сегментам на миллиардах строк.
- **Конфигурируемый набор перцентилей.** Заставил бы хранить результат в long-формате
  (строка на пару признак-метрика) и усложнил бы любой дашборд. Набор из семи перцентилей
  закрывает и центр, и хвосты; смена набора — осознанное изменение через миграцию и код.
- **Новое семейство тестов внутри `dq`.** DQ-тест по контракту возвращает
  `(failed_rows, observed)` и имеет порог. У профиля распределения порога нет, он ничего
  не проваливает. Впихивать его в этот контракт — ломать и контракт, и отчёт.
- **Расчёт по списку признаков из upload-конфигов.** Дал бы ровно то, что видит модель,
  но завёл бы зависимость `layers/**` от `upload/**`, которой сейчас нет, и оставил бы
  без наблюдения признаки, готовящиеся к выгрузке. Список из `information_schema` дешевле
  и подхватывает новую колонку миграции сам.

## 3. Область изменений

Новое:

- Пакет `feature_stats/` с модулями `config.py`, `query.py`, `runner.py`,
  `results_writer.py`, `task.py`, а также `results/config.yaml`,
  `results/migrations/create_table.sql` и `README.md`.
- `scripts/validate_feature_stats_configs.py`.
- Тесты `ci_test/test_feature_stats_{config,sql,runner,results,task,task_wiring}.py`
  и `ci_test/test_validate_feature_stats_configs.py`.

Изменяется:

- 7 файлов `config.yaml` целевых энтити: добавляется блок `feature_stats:`.
- 7 файлов `dag.py` целевых энтити: добавляется таска, параллельная `dq`.
- `scripts/run_pyspark_migrations.py:134` — в кортеж роутов добавляется `"feature_stats"`.
- `scripts/sync_iceberg_maintenance.py:16` — то же в `TABLE_CONFIG_ROOTS`.
- `.drone.yaml` — вызов нового валидатора в основном пайплайне.
- `AGENTS.md` — раздел про контракт таски `feature_stats`.

Не изменяется:

- `scripts/sync_dbt_sources.py`. Таблица результатов, как и `feature_platform_dq_results`,
  не уезжает в dbt-trino и не заводит DQ-тесты сама на себя.
- Конфиги и DAG'и upload'а. Сенсоры остаются на таске `dq`.

## 4. Целевые таблицы

Семь репозиторно-управляемых таблиц, читаемых upload-DAG'ами
(`upload/features_service_upload/v1/config.yaml` и
`upload/dynamic_pricing_inference_upload/v1/config.yaml`).
Объёмы — замер за 30 дней на 2026-08-23.

| Таблица | Строк/партиция | Числовых колонок | Гранулярность |
|---|---|---|---|
| `feature_platform_search_query_atc_features` | 30.4–32.4M | 24 | date |
| `feature_platform_search_sku_group_id_query_atc_order_features_v2` | 5.8–6.0M | 69 | date |
| `feature_platform_sku_group_search_conversion_features_v2` | 1.90–1.97M | 89 | date |
| `feature_platform_sku_group_stock_features` | 5.2–5.5M | 8 | date |
| `feature_platform_sku_group_price_features` | 5.2–5.5M | 6 | date |
| `feature_platform_sku_group_feedback_base_stats` | 2.06–2.13M | 18 | date |
| `feature_platform_dynamic_pricing_sku_group_price_features` | 15.1–20.6M | 9 | timestamp, шаг 3ч |

Внешняя таблица `um_prod_feature_store_iceberg.cold_start_boosted_pw_convs_query_atc_order_90`
в область не входит: она принадлежит DE, у неё свой DAG и свой контракт качества.

Единственная числовая колонка, которая не является признаком и не входит в PK, —
`category_id BIGINT` в `feature_platform_sku_group_search_conversion_features_v2`.
У остальных шести таблиц `exclude_columns` пустой. В частности, `BIGINT`-счётчики отзывов
в `feature_platform_sku_group_feedback_base_stats` и `INT`-колонки
`skg_days_since_last_impression` / `skg_days_since_last_atc` — полноценные признаки
и в исключения не попадают.

## 5. Пакет `feature_stats/`

```
feature_stats/
  __init__.py
  config.py            FeatureStatsSettings, StatsContext, load_feature_stats_settings()
  query.py             рендер Trino SQL и разбор строки результата
  runner.py            определение колонок, исполнение, сборка FeatureStat
  results_writer.py    запись в Iceberg через pyiceberg
  task.py              build_feature_stats_task(config_path, repo_root), TASK_ID
  results/
    config.yaml
    migrations/create_table.sql
  README.md
```

Модуль записи называется `results_writer`, а не `results`, по той же причине, что и в `dq/`:
каталог `feature_stats/results/` как namespace-пакет перекрыл бы `feature_stats.results`.

Из `dq.tests` импортируются и не дублируются: `quote_identifier`, `quote_literal`,
`table_ref`, `partition_literal`, `partition_expression`. Это единственная связка между
пакетами; она даёт гарантию, что таска `feature_stats` смотрит ровно на ту партицию,
что и таска `dq`, включая правило «литерал времени всегда с зоной»
(`TIMESTAMP '2026-08-22 06:00:00 UTC'`). Из `dq.config` импортируется `trino_catalog_alias`.

`feature_stats.config` не импортирует `DqSettings` и не читает блок `dq:`: блоки независимы,
их согласованность обеспечивает CI-валидатор, а не рантайм.

## 6. Контракт в `config.yaml`

```yaml
feature_stats:
  enabled: true
  trino_conn_id: trino_search
  partition_column: date
  partition_granularity: date
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  snapshot_interval_hours: 3
  exclude_columns:
    - category_id
  columns_per_query: null
  query_timeout_seconds: 600
```

| Ключ | Дефолт | Смысл |
|---|---|---|
| `enabled` | `true` | Выключение требует объяснения в README, как и у `dq` |
| `trino_conn_id` | `trino_search` | Соединение Airflow |
| `partition_column` | `date` | Колонка партиции целевой таблицы |
| `partition_granularity` | `date` | `date` или `timestamp` |
| `partition_date_template` | шаблон `ds - 1` | Airflow-шаблон, отдающий партицию; при `timestamp` обязателен и обязан отдавать `YYYY-MM-DD HH:MM:SS` в UTC |
| `snapshot_interval_hours` | 24 | Обязателен и осмыслен только при `timestamp` |
| `exclude_columns` | `[]` | Числовые колонки, которые не являются признаками |
| `columns_per_query` | `null` | `null` — один запрос на всю таблицу; число — размер батча колонок |
| `query_timeout_seconds` | 600 | Таймаут запроса |

Валидация `partition_granularity: timestamp` повторяет правила `dq.config`: обязательны
`snapshot_interval_hours` (положительный) и `partition_date_template`.

`columns_per_query: null` — дефолт осознанный: каждый дополнительный батч — это ещё один
полный скан партиции. Для самой широкой таблицы (89 колонок) это означает 445 агрегатных
выражений в одном плане. Батчинг включается только если планировщик Trino на этом упрётся;
тогда цена — кратное число сканов, и она меньше цены отказа от расчёта.

### Согласованность с блоком `dq:`

`scripts/validate_feature_stats_configs.py` падает, если у энтити объявлены оба блока и
хотя бы одно из полей `partition_column`, `partition_granularity`, `partition_date_template`,
`snapshot_interval_hours` в них различается. Разные партиции у DQ и у статистики в одном
DAG-ране — всегда ошибка конфигурации: профиль считался бы не по тем данным, которые
проверял DQ, и расхождение не проявилось бы ни падением, ни пустым результатом.

## 7. Определение набора колонок

Перед расчётом раннер выполняет:

```sql
SELECT column_name, data_type
FROM "dwh-iceberg".information_schema.columns
WHERE table_schema = 'gold' AND table_name = 'feature_platform_...'
ORDER BY ordinal_position
```

Каталог квалифицируется явно: дефолтный каталог соединений `trino_*` — `hive`,
и неквалифицированный `information_schema` падает с `CATALOG_NOT_FOUND`.

Числовыми считаются типы `tinyint`, `smallint`, `integer`, `bigint`, `real`, `double`
и `decimal(...)`. Из них вычитаются колонки `table.primary_key`, колонка
`feature_stats.partition_column` и `feature_stats.exclude_columns`. Порядок сохраняется
по `ordinal_position`, чтобы SQL был воспроизводим между запусками.

Пустой итоговый список — не ошибка: таска пишет ноль строк и завершается успешно
с записью в лог. Такое возможно у таблицы, состоящей из одних ключей.

Колонка из `exclude_columns`, которой нет в таблице, — ошибка конфигурации: таска падает
до расчёта. Иначе опечатка в `exclude_columns` молча вернула бы признак в наблюдение.

## 8. SQL расчёта

Один запрос на таблицу (или на батч колонок), пять агрегатов на колонку:

```sql
SELECT
  count(*) AS rows_total,
  count("skg_return_rate_7")                                AS cnt_0,
  avg(CAST("skg_return_rate_7" AS DOUBLE))                  AS mean_0,
  min(CAST("skg_return_rate_7" AS DOUBLE))                  AS min_0,
  max(CAST("skg_return_rate_7" AS DOUBLE))                  AS max_0,
  approx_percentile(CAST("skg_return_rate_7" AS DOUBLE),
                    ARRAY[0.05,0.1,0.25,0.5,0.75,0.9,0.95]) AS pct_0,
  ...
FROM "dwh-iceberg".gold.feature_platform_sku_group_search_conversion_features_v2
WHERE CAST("date" AS DATE) = DATE '2026-08-22'
```

Для снапшотной энтити предикат вырождается в `"calculated_at" = TIMESTAMP '2026-08-22 06:00:00 UTC'`
— ровно то, что даёт `dq.tests.partition_expression` / `partition_literal`.

Разбор результата:

- `null_share = 1 - cnt / rows_total`; при `rows_total = 0` — `NULL`.
- `avg` и `approx_percentile` игнорируют NULL, поэтому `mean` считается по непустым
  значениям и согласован с перцентилями.
- При `cnt = 0` Trino возвращает `NULL` вместо массива. Раннер кладёт `NULL` во все семь
  перцентилей, `mean`, `min` и `max`, но всё равно пишет строку: факт «признак целиком
  пустой на этой партиции» — сам по себе сигнал и должен попасть в таблицу.
- Алиасы колонок нумеруются по индексу (`cnt_0`, `mean_0`, …), а не собираются из имени
  признака: имя может превысить лимит идентификатора или совпасть после нормализации.
  Соответствие «индекс → имя признака» держит раннер.
- `CAST(... AS DOUBLE)` приводит `decimal`, `bigint`, `real` к одной схеме результата.
  Для `bigint` за пределами 2^53 это потеря точности; в целевых таблицах счётчики
  на порядки меньше, и ограничение фиксируется в README пакета.

## 9. Таблица результатов

`iceberg.silver.feature_platform_feature_stats`, строка на
(партиция, `dag_id`, таблица, признак).

```sql
CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции целевой таблицы; у снапшотной энтити — календарная дата снапшота',
    partition_ts TIMESTAMP COMMENT 'Момент партиции в UTC: сам снапшот у снапшотной энтити, полночь дня у дневной',
    run_ts TIMESTAMP COMMENT 'Момент прогона',
    dag_id STRING COMMENT 'DAG, внутри которого выполнялась таска',
    task_id STRING COMMENT 'Идентификатор таски, всегда feature_stats',
    run_id STRING COMMENT 'Airflow run_id прогона',
    try_number INT COMMENT 'Номер попытки таски',
    catalog STRING COMMENT 'Каталог целевой таблицы из config.yaml',
    schema_name STRING COMMENT 'Схема целевой таблицы',
    table_name STRING COMMENT 'Имя целевой таблицы',
    team STRING COMMENT 'Команда-владелец из table.meta.team',
    feature_name STRING COMMENT 'Имя колонки-признака',
    data_type STRING COMMENT 'Тип колонки в Trino на момент расчёта',
    rows_total BIGINT COMMENT 'Всего строк в партиции',
    non_null_count BIGINT COMMENT 'Строк с непустым значением признака',
    null_share DOUBLE COMMENT 'Доля NULL: 1 - non_null_count / rows_total',
    mean DOUBLE COMMENT 'Среднее по непустым значениям',
    min_value DOUBLE COMMENT 'Минимум по непустым значениям',
    max_value DOUBLE COMMENT 'Максимум по непустым значениям',
    p05 DOUBLE COMMENT 'approx_percentile 0.05',
    p10 DOUBLE COMMENT 'approx_percentile 0.1',
    p25 DOUBLE COMMENT 'approx_percentile 0.25',
    p50 DOUBLE COMMENT 'approx_percentile 0.5',
    p75 DOUBLE COMMENT 'approx_percentile 0.75',
    p90 DOUBLE COMMENT 'approx_percentile 0.9',
    p95 DOUBLE COMMENT 'approx_percentile 0.95',
    duration_ms BIGINT COMMENT 'Длительность запроса, в котором посчитан этот признак',
    sql_text STRING COMMENT 'Отрендеренный SQL расчёта'
)
USING iceberg
COMMENT 'Профили распределения признаков таблиц Feature Platform'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
```

Объём: 24 + 69 + 89 + 8 + 6 + 18 = 214 строк в сутки по дневным энтити плюс 9 × 8 = 72
по снапшотной, итого около 286 строк в сутки.

`feature_stats/results/config.yaml` повторяет форму `dq/results/config.yaml`:
`catalog: iceberg`, `schema: silver`, `primary_key: date,partition_ts,dag_id,table_name,feature_name`,
`meta.team: team:search`, `meta.create_dbt_pr: false`, `meta.create_maintenance_pr: true`.
`primary_key` включает `partition_ts` и потому уникален и для снапшотной энтити,
которая пишет несколько партиций в одни календарные сутки.

### Идемпотентность

Перезапись через `table.overwrite` с единым фильтром для обеих гранулярностей:

```python
And(
    EqualTo("date", ctx.partition_date),
    EqualTo("dag_id", meta.dag_id),
    EqualTo("partition_ts", ctx.partition_ts),
)
```

`partition_ts` заполняется всегда: у снапшотной энтити — записанным снапшотом, у дневной —
полуночью UTC её партиции. Это не суррогат, а канонический момент партиции, и он даёт три
вещи сразу: уникальный первичный ключ, фильтр без ветвления на `IsNull` и одну ось времени
для дашборда независимо от гранулярности энтити.

`partition_ts` обязателен в фильтре именно потому, что снапшотная энтити пишет несколько
партиций в одни календарные сутки. Фильтр по одним `date` и `dag_id` затирал бы профили
предыдущих снапшотов того же дня, и в таблице оставался бы только последний из восьми.

Существующий `dq.results_writer.write_results` фильтрует ровно по `date` и `dag_id` и этим
дефектом обладает: в `feature_platform_dq_results` от DAG'а dynamic pricing остаётся только
последний снапшот суток. Это отдельный баг DQ, он зафиксирован здесь, но в область данной
работы не входит.

## 10. Интеграция в DAG

```python
from feature_stats.task import build_feature_stats_task

FEATURE_STATS_PARTITION = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'

...
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(FEATURE_STATS_PARTITION)

    collect_features >> [dq_task, stats_task]
```

- `task_id = "feature_stats"`, `retries = 1`, `on_failure_callback` — тот же
  `send_oncall_notification` с параметрами из блока `alerts:` энтити.
- Таска терминальная и параллельная `dq`. Падение статистики помечает DAG-ран упавшим
  и шлёт алерт, но не влияет на публикацию: `ExternalTaskSensor` в upload-DAG'ах ждёт
  `external_task_id="dq"`.
- Порядок `collect_features >> [dq_task, stats_task]` сохраняет прежний контракт
  «DQ идёт сразу после записи» и не удлиняет критический путь до upload'а.

## 11. CI

- `scripts/validate_feature_stats_configs.py`: обходит `layers/**/config.yaml` и
  `datasets/**/config.yaml`, для каждого блока `feature_stats:` вызывает
  `load_feature_stats_settings`, проверяет согласованность с блоком `dq:` (раздел 6),
  проверяет, что каждая колонка из `exclude_columns` встречается в SQL-миграциях энтити,
  и что при `enabled: false` в README есть объяснение. Добавляется в основной пайплайн
  `.drone.yaml` рядом с `validate_dq_configs.py`.
- `"feature_stats"` добавляется в роуты обхода конфигов в `scripts/run_pyspark_migrations.py`
  и `scripts/sync_iceberg_maintenance.py`, чтобы миграция таблицы результатов применялась
  и таблица регистрировалась в maintenance.
- Тесты:
  - `test_feature_stats_config.py` — парсинг и валидация блока, включая снапшотные правила.
  - `test_feature_stats_sql.py` — сравнение отрендеренной SQL-строки с эталоном для дневной
    и снапшотной энтити. Обязателен по `AGENTS.md`: диалект Trino локально не исполняется
    ничем, и проверка строки — единственная защита от синтаксической ошибки.
  - `test_feature_stats_runner.py` — фильтрация колонок по типу и исключениям, разбор строки
    результата, поведение при `cnt = 0` и при пустом списке колонок.
  - `test_feature_stats_results.py` — форма строк и фильтр перезаписи для обеих гранулярностей.
  - `test_feature_stats_task.py` — разбор шаблона партиции, сборка контекста.
  - `test_feature_stats_task_wiring.py` — все 7 DAG'ов содержат таску и корректный блок конфига.
  - `test_validate_feature_stats_configs.py` — сам валидатор.

## 12. Фазы

1. **Каркас.** Пакет `feature_stats/`, `results/config.yaml`, `create_table.sql`, валидатор,
   тесты, роуты в CI-скриптах, строка в `.drone.yaml`. Ни один DAG не тронут.
2. **Проверка на живом Trino.** Прогон отрендеренного SQL по
   `feature_platform_sku_group_search_conversion_features_v2` (89 колонок, 445 агрегатов) —
   подтвердить, что план строится; и по
   `feature_platform_dynamic_pricing_sku_group_price_features` — подтвердить, что литерал
   с зоной указывает на нужный снапшот. По результату — решение о дефолте `columns_per_query`.
3. **Проводка энтити.** Блоки `feature_stats:` и таски в 7 DAG'ах.
4. **Документация.** Раздел в `AGENTS.md`: контракт таски, её отношение к `dq`, правило
   согласованности partition-настроек, запрет вешать на неё downstream-сенсоры.

Дашборд поверх таблицы результатов — отдельная работа, в эту не входит.

## 13. Риски

- **445 агрегатов в одном плане Trino.** Непроверено до фазы 2. Митигация — ключ
  `columns_per_query`, который переводит таблицу на батчи ценой дополнительных сканов.
- **Нагрузка на общий кластер Trino.** Плюс 7 сканов в сутки, крупнейший — 32M строк на
  24 колонки. На фоне текущей DQ-нагрузки это шум, но за временем выполнения на фазе 2
  надо посмотреть.
- **Приближённость перцентилей.** `approx_percentile` строит t-digest; на хвостах ошибка
  порядка процента. Для наблюдения за дрейфом это допустимо, для порогов вида «ровно p95»
  — нет. Фиксируется в README пакета.
- **Молчаливое расширение набора признаков.** Новая числовая колонка в миграции попадает
  в расчёт автоматически. Это осознанный выбор в пользу полноты наблюдения; обратная
  сторона — рост стоимости запроса без правки конфига.

## 14. Что не входит

- Пороги, алерты и падение таски по факту дрейфа. Таска только считает и пишет.
- Дашборды в Superset или Grafana.
- Сравнение распределений между обучением и инференсом: датасетные таблицы под
  `datasets/**` в область не входят.
- Профили нечисловых признаков (`query`, `promotion_id`): другой набор метрик,
  отдельная работа.
- Исправление фильтра перезаписи в `dq/results_writer.py` (раздел 9).
