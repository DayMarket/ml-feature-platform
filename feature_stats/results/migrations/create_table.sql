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
    team STRING COMMENT 'Команда-владелец из table.meta.team, по умолчанию team:search',
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
