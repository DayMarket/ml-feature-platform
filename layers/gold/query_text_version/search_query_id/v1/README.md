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
- Elasticsearch `_analyze` - токенизация очищенного запроса рабочим анализатором индекса.

## Зависимости

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`
  (`execution_delta = 4 часа`: логическая дата DQ-прогона `01:00` UTC против `05:00` UTC у этого DAG).

## Логика

День берется из `data_interval_start` в UTC; при расписании `0 5 * * *` это предыдущие сутки.

1. Trino по `trino_search` собирает `SELECT DISTINCT uniqs` за день при `space = 'SEARCH_RESULTS'`.
   В этом же запросе стоит `LEFT JOIN` к `iceberg.gold.feature_platform_search_query_id` по
   `query_text` и текущей `version` с условием `known_query.query_text IS NULL`, поэтому запросы,
   для которых `query_id` уже посчитан, повторно не обрабатываются.
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

## Рантайм

Trino + Elasticsearch-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Trino connection:
`trino_search`. Elasticsearch connection: `elasticsearch_search`. Образ задачи:
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Утилиты нормализации вынесены в `job/normalize.py`, HTTP-клиент `_analyze` - в `job/analyze.py`,
Trino-запрос - в `job/query.py`, работа с каталогом и запись - в `job/runtime.py`.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
