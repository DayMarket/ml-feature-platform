CREATE TABLE IF NOT EXISTS {target_table} (
    updated_at TIMESTAMP COMMENT 'UTC-время прогона (data_interval_end), на котором запрос впервые попал в справочник',
    query_text STRING COMMENT 'Исходный поисковый запрос из silver-предагрегата: uniqs при space = SEARCH_RESULTS',
    query_id STRING COMMENT 'Каноничная форма запроса: токены Elasticsearch-анализатора сгруппированы по позициям, из каждой позиции взят первый вариант, результат отсортирован и склеен пробелом',
    version STRING COMMENT 'Версия алгоритма нормализации; текущий DAG пишет v1'
)
USING iceberg
COMMENT 'Справочник каноничных query_id по поисковым запросам SEARCH_RESULTS'
PARTITIONED BY (version)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
