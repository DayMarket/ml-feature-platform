# iceberg.gold.feature_platform_search_query_id

Справочник каноничных `query_id` по поисковым запросам: разные формулировки одного и того же
запроса получают один `query_id`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_search_query_id`.
- DAG: `feature-platform.layers.gold.query_text_version.search_query_id` (`layers/gold/query_text_version/search_query_id/v1/dag.py`).
- Групповой тег Airflow: `search-query-id`.
- Расписание: ежедневно, `0 5 * * *` UTC.
- `start_date=2026-08-07T00:00:00Z`, `catchup=False`, `is_paused_upon_creation=True`.

## Грейн / ключ

`query_text, version`.

`version` фиксирует алгоритм нормализации. Текущий DAG пишет только `v1`, поэтому в таблицу можно
позже добавить `v2` от другого DAG без нарушения уникальности ключа. Таблица партиционирована по
`version`.

## Источники

- `"dwh-iceberg".silver.search_logs` - сырые логи поиска, внешний DE-источник. Кандидаты берутся
  скользящим окном `lookback_days` (сейчас 30 дней), заканчивающимся закрытым днём партиции.
- Elasticsearch `_analyze` - токенизация очищенного запроса рабочим анализатором индекса.

## Зависимости

Sensor'ов нет. `silver.search_logs` - внешняя DE-таблица без DAG'а-владельца в этом репозитории;
контракт тот же, что у `layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1`,
который читает её так же без sensor'а. Окно скользящее, поэтому недогруженный на момент запуска
последний день не теряет запросы: они проходят порог на одном из следующих 29 запусков.

До перехода на логи энтити ждала
`dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`
с `execution_delta = 4 часа`. Этот источник больше не читается, sensor снят вместе с ним - в
`docs/feature_platform_map.md` стало на одно ребро легаси-dbt-DQ меньше.

## Логика

День берется из `data_interval_start` в UTC; при расписании `0 5 * * *` это предыдущие сутки.

1. Trino по `trino_search` собирает кандидатов из `silver.search_logs` за окно
   `[день партиции + 1 день - lookback_days, день партиции + 1 день)`. Окно считается от даты
   партиции, а не от `now()`, поэтому перезапуск за ту же дату даёт тот же набор кандидатов.
   Границы пишутся с явной зоной (`TIMESTAMP '... UTC'`): `logged_at` - `timestamp with time zone`,
   а сессия Trino живёт в `Europe/Moscow`, и голый литерал молча сдвинул бы окно.

   Строка берётся при `query_text != ''` и `pagination_offset = 0` (первая страница выдачи), запрос
   - как `service_query`: `corrected_query_text`, если он не пуст и не `NULL`, иначе `query_text`.
   Это ровно тот текст, который сервис отправляет в ранжирование (см. «Соответствие
   `ranking_analytics_events`»).

   Кандидаты группируются по `service_query`, считается `COUNT(DISTINCT install_id)`, и хвост
   отсекается порогами: запросы длиной `<= short_query_max_length` требуют
   `> short_query_min_installs` инсталлов, остальные `> long_query_min_installs`. Замер на окне
   `2026-08-04..2026-09-02`: из 14 591 315 различных `service_query` порог оставляет 1 522 547
   (10.4%). Коротких запросов всего 4632, отдельный порог оставляет из них 71 вместо 2145.

   В этом же запросе стоит `LEFT JOIN` к `iceberg.gold.feature_platform_search_query_id` по
   `query_text` и текущей `version` с условием `known_query.query_text IS NULL`, поэтому запросы,
   для которых `query_id` уже посчитан, повторно не обрабатываются. На том же окне анти-джойн
   оставляет 72 540 новых запросов - это размер первого прогона; дальше в него попадает только
   дневной прирост запросов, впервые перешагнувших порог.
2. `remove_stop_words` приводит запрос к нижнему регистру, вырезает стоп-слова по границам слов и
   схлопывает пробелы. Список лежит в `job/stop_words.txt`, одна запись на строку; записи могут
   быть многословными (`aksiya tavarlar`, `eng arzon narsalar`). Альтернативы в регулярном
   выражении сортируются по убыванию длины: `re` берет первое совпадение, а не самое длинное,
   поэтому без сортировки `aksiya` перекрыл бы `aksiya tavarlar` и оставил бы в запросе
   `tavarlar`.
3. Очищенный запрос отправляется в Elasticsearch `GET /search-index/_analyze` с анализатором
   `full_name_analyzer`; запросы выполняются параллельно потоками (`parallel_jobs`), с ретраями на
   уровне HTTP.
4. Токены группируются по `position`, внутри позиции дедуплицируются и сортируются, из каждой
   позиции берется первый вариант. Итоговые токены сортируются и склеиваются пробелом - это и есть
   `query_id`, поэтому порядок слов в исходном запросе на результат не влияет.
5. Строки дописываются в Iceberg через PyIceberg `append`. Перезаписи нет: `updated_at` остается
   датой первого появления запроса.

Запросы, которые после удаления стоп-слов стали пустыми, и запросы без токенов анализатора
пропускаются - для них строка не пишется, и они снова попадут в выборку на следующий день.

## Соответствие `ranking_analytics_events`

`service_query` - тот же текст, что сервис ранжирования кладёт в
`"dwh-iceberg".silver.ranking_analytics_events.search_query`, но не байт в байт. Замер за сутки
2026-09-02/03 по джойну `search_logs.query_id = ranking_analytics_events.request_id` при
`model_name LIKE 'search_unified_model_v%'`, 2 414 110 сджойненных строк:

| Сравнение | Совпало | Доля |
|---|---|---|
| точное равенство | 2 288 610 | 94.8% |
| `lower` + `trim` | 2 372 689 | 98.3% |
| + свёртка вариантов апострофа к ASCII `'` | 2 394 550 | 99.2% |
| + удаление пробелов | 2 396 865 | 99.3% |

Для сравнения, тот же джойн против `query_text` даёт 79.6%, против `result_query_text` - 77.2%,
так что `service_query` - действительно ближайшая форма.

Расхождения - это нормализация на стороне сервиса, уже после записи лога: варианты узбекского
апострофа (`koʻrpa` против `ko'rpa`, самый массовый класс), регистр, разбиение склеенных токенов
(`9mice` → `9 mice`), и оставшиеся ~0.7% - исправление опечаток, которое сервис делает сам и не
кладёт в `corrected_query_text` (`Artofish` → `yaratish`, `iayfon` → `maydon`).

На потребителей это не влияет: справочник хранит сырой текст, а потребители нормализуют обе
стороны на чтении (`build_query_id_map` в
`layers/gold/query/search_query_atc_features_qid/v1/job/getting_search_query_atc_features_qid.py`:
`lower` + `ё`→`е` + схлопывание пробелов + `trim`). Свёртка апострофов в эту цепочку не входит -
если потребителю понадобится джойн прямо от `ranking_analytics_events.search_query`, это даст
ещё ~0.9 п.п. и делается отдельной задачей на стороне потребителя.

## Рантайм

Trino + Elasticsearch-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Trino connection:
`trino_search`. Elasticsearch connection: `elasticsearch_search`. Образ задачи:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Утилиты нормализации вынесены в `job/normalize.py`, HTTP-клиент `_analyze` - в `job/analyze.py`,
Trino-запрос - в `job/query.py`, работа с каталогом и запись - в `job/runtime.py`.

## DQ и feature_stats

У справочника нет колонки даты, а партиционирован он по `version`, поэтому весь партиционный
аппарат DQ отключён: `dq.scope: table` (предикат партиции вырождается в `TRUE`, и тесты идут по
всей таблице), `dq.warmup_days: 0`, а `freshness` и `row_count_growth` выключены явно - они
рендерят SQL по `dq.partition_column` независимо от `scope`. Работают `primary_key_not_null`,
`primary_key_unique` и `row_count_min` (severity `warn`).

`feature_stats` выключен: `render_stats_query` всегда фильтрует по колонке партиции, которой
здесь нет. Профилировать тоже нечего - числовых feature-колонок у таблицы нет
(`query_id`, `query_text`, `version` - STRING, `updated_at` - TIMESTAMP).

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
