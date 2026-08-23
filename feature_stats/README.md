# feature_stats в Feature Platform

`feature_stats` считает профиль распределения каждого числового признака записанной
партиции репозиторно-управляемой таблицы и пишет его в
`iceberg.silver.feature_platform_feature_stats`. Задача объявляется блоком
`feature_stats:` файла `config.yaml` энтити и исполняется одноимённой таской внутри того
же DAG'а, параллельно таске `dq`.

Разница с `dq` (см. `dq/README.md`) — в вопросе, на который отвечает таска. `dq` отвечает
«данные валидны»: проверяет пороги, ключи, NULL-доли, рост относительно вчера. `feature_stats`
отвечает «данные те же, что вчера»: не проверяет ничего и не падает от плохих значений — просто
считает `count/mean/min/max` и перцентили по каждому числовому признаку и кладёт их в
таблицу, по которой в Superset видно, поехало ли распределение.

Таска ничего не блокирует: downstream `ExternalTaskSensor`-ы в аплоад-DAG'ах ждут `dq`,
а не `feature_stats` — упавший или неполный профиль не повод не публиковать валидные фичи.

Профиль считается SQL-запросом в Trino через `TrinoHook`, аналогично `dq`: таблица
адресуется как `"<trino-catalog>".<schema>.<table>` (маппинг каталога — тот же
`ci_config.yaml`, что использует `dq`).

## Быстрый старт

Блок `feature_stats:` необязателен только в том смысле, что у него есть дефолты на все
ключи, кроме тех, что требуются при `partition_granularity: timestamp`. Но сам блок
завести придётся — без него таска не подключена к DAG'у.

Минимальный блок для дневной энтити:

```yaml
feature_stats:
  trino_conn_id: trino_search
  # Дословно совпадает с dq.partition_date_template: обе таски обязаны смотреть
  # на одну партицию, иначе профиль посчитан не по тем данным, что проверял DQ.
  partition_date_template: '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
  exclude_columns:
    - category_id   # идентификатор, а не признак — профиль по нему бессмыслен
```

Проводка в `dag.py` — таска идёт параллельно `dq` после записи партиции:

```python
from feature_stats.task import build_feature_stats_task

stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

# Статистика идёт параллельно DQ и ни на что не влияет: аплоад ждёт таску dq,
# поэтому падение профилей не блокирует публикацию фич.
collect_features >> [dq_task, stats_task]
```

`DQ_PARTITION_DATE` — то же выражение шаблона партиции, что передаётся в `build_dq_task`;
у снапшотных энтити это `DQ_PARTITION_TIMESTAMP` (см. ниже).

## Ключи конфига

| Ключ | Тип | Дефолт | Смысл |
|---|---|---|---|
| `enabled` | bool | `true` | `false` выключает таску целиком |
| `trino_conn_id` | str | `trino_search` | Airflow-соединение для `TrinoHook` |
| `partition_column` | str | `date` | колонка, по которой фильтруется партиция |
| `partition_date_template` | str | `{{ macros.ds_add(ds, -1) }}` | Jinja-шаблон значения `partition_column` для текущего рана |
| `partition_granularity` | `date` \| `timestamp` | `date` | `timestamp` — снапшотная энтити (см. ниже) |
| `snapshot_interval_hours` | int | `24`; обязателен при `partition_granularity: timestamp` | шаг расписания DAG'а в часах |
| `exclude_columns` | list[str] | `[]` | колонки, которые не считать признаками, даже если они числовые |
| `columns_per_query` | int \| `null` | `null` | размер партии колонок на один SQL-запрос; `null` — один запрос на всю таблицу |
| `query_timeout_seconds` | int | `600` | таймаут запроса статистики |

Плюс `table.primary_key` из блока `table:` (общий с DQ) — колонки первичного ключа
исключаются из профиля автоматически, как и `partition_column`.

Источник истины по дефолтам и валидации — `feature_stats/config.py`
(`load_feature_stats_settings`, `KNOWN_KEYS`). Неизвестный ключ в блоке `feature_stats:`
— ошибка конфигурации, а не тихо игнорируемое поле.

### Партиции должны совпадать с `dq:`

`partition_column`, `partition_granularity`, `partition_date_template` и
`snapshot_interval_hours` обязаны быть одинаковыми в блоках `dq:` и `feature_stats:` одной
энтити. `scripts/validate_feature_stats_configs.py` роняет CI, если они разошлись: разные
партиции у двух тасок одного DAG-рана значат, что профиль посчитан не по тем данным,
которые проверял DQ, и это не видно ни по падению, ни по пустому результату — просто числа
в Superset относятся к другому моменту, чем зелёный статус DQ.

`columns_per_query` — единственный ключ, который никак не связан с `dq:` и не проверяется
этим правилом.

## Как определяется набор признаков

Список колонок и их типов таска берёт из `information_schema.columns` целевой таблицы, в
порядке `ordinal_position`. Числовыми считаются `tinyint`, `smallint`, `integer`,
`bigint`, `real`, `double` и любой `decimal(...)` — проверка точным совпадением типа, а не
префиксом (`is_numeric` в `feature_stats/runner.py`).

Из этого списка автоматически исключаются:

- колонки `table.primary_key`;
- колонка `partition_column`;
- всё нечисловое (строки, даты, массивы, JSON).

Дополнительно исключается всё, что перечислено в `feature_stats.exclude_columns` —
идентификаторы вроде `category_id`, которые формально `BIGINT`, но профиль распределения
по ним бессмыслен.

**Опечатка в `exclude_columns` роняет таску.** Если колонка из `exclude_columns`
отсутствует среди колонок таблицы, `select_feature_columns` кидает `FeatureStatsError`,
и таска падает целиком, а не тихо продолжает без исключения. Так задумано: молчаливое
поведение вернуло бы опечатавшийся признак под наблюдение, а заметить это можно было бы
только по случайности.

Если после исключений признаков не осталось (таблица из одних ключей), это не ошибка:
таска просто не пишет ни одной строки.

### Производительность и `columns_per_query`

Дефолт `columns_per_query: null` — один SQL-запрос на всю таблицу партии, со всеми
агрегатами сразу. Каждый дополнительный запрос — это ещё один полный скан партиции, а не
дешёвая добавка, поэтому дробить дорого. На самой широкой подключённой таблице
(`sku_group_search_conversion_features_v2`, 89 признаков) один запрос — это 446
агрегатных выражений в одном плане Trino, и на реальных данных (1 965 579 строк) он
выполнился примерно за 18.5 секунды. `columns_per_query` нигде среди подключённых энтити
не задан; ставьте его только если конкретная таблица упирается в лимиты Trino на число
выражений в запросе или в таймаут.

## Снапшотные энтити

Как и в `dq`, часть энтити пишет не дневную партицию, а снапшот несколько раз в сутки.
Блок `feature_stats:` для такой энтити обязан дословно повторять снапшотные ключи `dq:`:

```yaml
feature_stats:
  trino_conn_id: trino_search
  partition_granularity: timestamp
  partition_column: calculated_at
  snapshot_interval_hours: 3
  partition_date_template: '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
  exclude_columns: []
```

`snapshot_interval_hours` и `partition_date_template` в этом случае обязательны — без
шаблона с полным временем таска упала бы на разборе значения партиции.

**Литерал времени всегда с зоной.** Как и в DQ-рендерере, фильтр партиции у снапшотной
энтити — `TIMESTAMP '... UTC'`, а не голый `TIMESTAMP '...'`: сессия Trino живёт не в UTC,
и без явной зоны литерал коэрсится в соседний снапшот молча.

`partition_ts` в таблице результатов заполнен всегда: у снапшотной энтити это ровно
записанный снапшот, у дневной — полночь UTC её партиции (`partition_instant` в
`feature_stats/task.py`). Это часть первичного ключа таблицы результатов и часть фильтра
идемпотентной перезаписи — см. ниже.

## Таблица результатов

`iceberg.silver.feature_platform_feature_stats`, партиционирование по `date`, конфиг и
миграция в `feature_stats/results/`. Первичный ключ —
`date,partition_ts,dag_id,table_name,feature_name`. Одна строка — один признак одной
партиции одного прогона.

| Колонка | Тип | Смысл |
|---|---|---|
| `date` | DATE | дата партиции целевой таблицы; у снапшотной энтити — календарная дата снапшота |
| `partition_ts` | TIMESTAMP | момент партиции в UTC: сам снапшот у снапшотной энтити, полночь дня у дневной |
| `run_ts` | TIMESTAMP | момент прогона (`logical_date` таски) |
| `dag_id` | STRING | DAG, внутри которого выполнялась таска |
| `task_id` | STRING | идентификатор таски, всегда `feature_stats` |
| `run_id` | STRING | Airflow `run_id` прогона |
| `try_number` | INT | номер попытки таски |
| `catalog` | STRING | каталог целевой таблицы из `config.yaml` |
| `schema_name` | STRING | схема целевой таблицы |
| `table_name` | STRING | имя целевой таблицы |
| `team` | STRING | команда-владелец из `table.meta.team`, по умолчанию `team:search` |
| `feature_name` | STRING | имя колонки-признака |
| `data_type` | STRING | тип колонки в Trino на момент расчёта |
| `rows_total` | BIGINT | всего строк в партиции |
| `non_null_count` | BIGINT | строк с непустым значением признака |
| `null_share` | DOUBLE | `1 - non_null_count / rows_total` |
| `mean` | DOUBLE | среднее по непустым значениям |
| `min_value` | DOUBLE | минимум по непустым значениям |
| `max_value` | DOUBLE | максимум по непустым значениям |
| `p05`…`p95` | DOUBLE | `approx_percentile` на `0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95` |
| `duration_ms` | BIGINT | длительность запроса, в котором посчитан этот признак |
| `sql_text` | STRING | отрендеренный SQL расчёта |

Для дневной энтити `date` и `partition_ts` избыточны на первый взгляд (`partition_ts` —
всегда полночь `date`), но оба нужны как единая схема со снапшотной энтити и для фильтра
перезаписи ниже.

Запись идемпотентна: строки перезаписываются по фильтру `date AND dag_id AND
partition_ts` (`overwrite_filter_values` в `feature_stats/results_writer.py`).
`partition_ts` в фильтре обязателен — снапшотная энтити (например,
`dynamic_pricing_sku_group_price_features`) пишет до 8 партиций в календарные сутки, и
фильтр без `partition_ts` оставил бы в таблице только последний снапшот дня, затерев
остальные при следующем ретрае.

> Обратите внимание: `dq/results_writer.py` перезаписывает только по `date` и `dag_id`,
> без `partition_ts`. Это узкий фильтр — известный дефект в соседнем пакете `dq`, не
> предмет этой задачи, но стоит иметь в виду при чтении по аналогии.

Таблица помечена `create_dbt_pr: false` (не уезжает в `dbt-trino`) и
`create_maintenance_pr: true` в `feature_stats/results/config.yaml`.

## Известные ограничения

- **Перцентили приближённые.** `approx_percentile` в Trino — t-digest, ошибка порядка
  процента в хвостах распределения. Значения `p05`/`p95` не годятся как источник для
  порогов вида «ровно p95» или для сравнения с независимо посчитанным точным перцентилем.
- **Набор перцентилей не конфигурируется.** `PERCENTILES` и `PERCENTILE_COLUMNS` в
  `feature_stats/config.py` зашиты в код и позиционно соответствуют колонкам `p05..p95`
  таблицы результатов. Добавить или убрать перцентиль — это миграция таблицы плюс правка
  кода, а не ключ конфига.
- **Потеря точности `BIGINT`.** `mean`/`min`/`max` считаются через `CAST(... AS DOUBLE)`.
  Для `BIGINT` за пределами `2^53` это теряет точность — ни одна подключённая сейчас
  колонка-признак к этой границе не приближается, но при подключении новой широкой
  таблицы стоит проверить диапазон значений.
- **Опечатка в `exclude_columns` — это падение таски**, а не тихий пропуск исключения (см.
  выше). Осознанное поведение, но обязательно проверяйте `columns_per_query` и
  `exclude_columns` при первом подключении энтити на реальном Trino.
- **Таска ничего не блокирует.** `feature_stats` не имеет статуса «passed/failed» с точки
  зрения контракта данных — только `success`/`failed` самой Airflow-таски. Ни один
  downstream не ждёт её явно.

## Локальные проверки

```bash
python3 scripts/validate_feature_stats_configs.py
python3 ci_test/test_feature_stats_config.py
python3 ci_test/test_feature_stats_sql.py
python3 ci_test/test_feature_stats_runner.py
python3 ci_test/test_feature_stats_results.py
python3 ci_test/test_feature_stats_task.py
python3 ci_test/test_feature_stats_task_wiring.py
python3 ci_test/test_validate_feature_stats_configs.py
```
