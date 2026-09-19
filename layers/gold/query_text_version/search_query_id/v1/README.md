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

- `iceberg.silver.feature_platform_search_sku_group_id_install_query` - предагрегат поисковых
  событий; берется колонка `uniqs` при `space = 'SEARCH_RESULTS'` за день интервала.
- `iceberg.silver.ranking_analytics_events` - ranking-логи; берется колонка `search_query` за окно
  `source.ranking_events.lookback_days` суток, включая день интервала, при
  `model_name LIKE 'search_unified_model_v%'`.
- Elasticsearch `_analyze` - токенизация очищенного запроса рабочим анализатором индекса.

Два Trino-источника объединяются через `UNION` (не `UNION ALL`), и анти-джойн к справочнику
применяется один раз поверх объединения - см. `job/query.py`.

### Почему добавлены ranking-логи

Предагрегат исчерпан как поставщик новизны. Замеры на 2026-09-16: из 885 280 его уникальных
запросов новыми для справочника были 197, тогда как `ranking_analytics_events` при том же фильтре
моделей дал 918 232 уникальных запроса за день и 90 682 новых. За окно в 7 дней - 4 035 576
уникальных и 479 081 новых.

Фильтр `model_name LIKE 'search_unified_model_v%'` оставляет только запросы, дошедшие до выдачи
(`v6`, `v9_cold_start`, `v10`). `speller_ranking_model_v1` (1 049 746 уникальных запросов в день) и
`search_suggests_v2` (556 819) отброшены осознанно: первый отдает запросы до исправления опечаток,
второй - префиксы набора (`calvin klein т`, `jensi aa`). Анализатор Elasticsearch их не
канонизирует, а токенизирует, поэтому каждая опечатка и каждый префикс получили бы собственный
`query_id` - справочник вырос бы примерно на 700 тыс. строк в день, не объединив ни одной
формулировки.

### Почему у ranking-логов нахлест

У `ranking_analytics_events` нет ни своего DQ-контракта (таблица принадлежит DE), ни колонки даты -
срез идет только по `fired_at`. Поздно доехавшие события ловить больше нечем, поэтому окно шире
одного дня. По Elasticsearch нахлест почти бесплатен: анти-джойн к справочнику отсекает все, что уже
посчитано, повторных `_analyze` не будет. Платим сканом Trino: около 27.9 млн строк в сутки, порядка
195 млн за семидневное окно на каждом прогоне. Предагрегат читается по-прежнему за один день -
его готовность гарантирует сенсор.

## Зависимости

- `feature-platform.layers.silver.sku_group_id_query_category.sku_group_install`, таска `dq`
  (`execution_delta = 4 часа`: владелец идёт в `01:00` UTC, этот DAG — в `05:00` UTC).

Сенсора на `ranking_analytics_events` нет: таблица DE-owned, ее DQ-контракт живет вне этого
репозитория. Так же читает ее `layers/silver/query_sku_group_id/search_query_sku_group_dssm_scores/v1`.
Роль страховки от неготовых данных играет нахлест окна.

## Логика

День берется из `data_interval_start` в UTC; при расписании `0 5 * * *` это предыдущие сутки.

1. Trino по `trino_search` собирает объединение двух выборок: `SELECT DISTINCT uniqs` из
   предагрегата за день при `space = 'SEARCH_RESULTS'` и `SELECT DISTINCT search_query` из
   ranking-логов за окно `lookback_days` при `model_name LIKE 'search_unified_model_v%'`. Поверх
   объединения стоит `LEFT JOIN` к `iceberg.gold.feature_platform_search_query_id` по `query_text` и
   текущей `version` с условием `known_query.query_text IS NULL`, поэтому запросы, для которых
   `query_id` уже посчитан, повторно не обрабатываются.
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

## Первый прогон после подключения ranking-логов

Справочник насыщался только предагрегатом, поэтому первый прогон с окном в 7 дней отправит в
Elasticsearch около 479 тыс. запросов (замер на 2026-09-16) вместо установившихся ~91 тыс. в день.
При `parallel_jobs: 24` это укладывается в `dagrun_timeout: 6h` с запасом при латентности `_analyze`
до ~1 секунды. Если нагружать индекс разом нежелательно, первый прогон можно сделать с
`lookback_days: 1` и вернуть 7 следующим PR - справочник доберет остаток за неделю обычными ранами.

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
