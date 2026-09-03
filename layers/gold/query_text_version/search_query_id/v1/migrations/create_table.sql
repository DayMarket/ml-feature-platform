CREATE TABLE IF NOT EXISTS {target_table} (
    updated_at TIMESTAMP COMMENT 'UTC-время прогона (data_interval_end), на котором запрос впервые попал в справочник',
    query_text STRING COMMENT 'Исходный поисковый запрос как service_query из silver.search_logs: corrected_query_text, если он непустой, иначе query_text',
    query_id STRING COMMENT 'Каноничная форма запроса: токены Elasticsearch-анализатора сгруппированы по позициям, из каждой позиции взят первый вариант, результат отсортирован и склеен пробелом',
    version STRING COMMENT 'Версия алгоритма нормализации; текущий DAG пишет v1'
)
USING iceberg
COMMENT 'Справочник каноничных query_id по поисковым запросам с первой страницы выдачи'
PARTITIONED BY (version)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
