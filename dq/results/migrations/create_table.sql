CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Проверявшаяся партиция целевой таблицы',
    run_ts TIMESTAMP COMMENT 'Момент прогона DQ',
    dag_id STRING COMMENT 'DAG, внутри которого выполнялась таска dq',
    task_id STRING COMMENT 'Идентификатор таски, всегда dq',
    run_id STRING COMMENT 'Airflow run_id прогона',
    try_number INT COMMENT 'Номер попытки таски',
    catalog STRING COMMENT 'Каталог целевой таблицы из config.yaml',
    schema_name STRING COMMENT 'Схема целевой таблицы',
    table_name STRING COMMENT 'Имя целевой таблицы',
    team STRING COMMENT 'Команда-владелец целевой таблицы из table.meta.team, по умолчанию team:search',
    test_name STRING COMMENT 'Имя теста из каталога DQ',
    test_key STRING COMMENT 'Уникальный ключ теста с параметрами, например accepted_range[price]',
    test_family STRING COMMENT 'Семейство теста: null_checks, uniqueness, domain_values, referential_integrity, row_expr, consistency, recency',
    status STRING COMMENT 'passed, failed, warned, skipped или errored',
    severity STRING COMMENT 'Эффективный severity прогона: error или warn',
    failed_rows BIGINT COMMENT 'Число нарушающих строк, для агрегатных тестов 1',
    observed DOUBLE COMMENT 'Наблюдаемое числовое значение: доля, коэффициент или счётчик',
    threshold STRING COMMENT 'Человекочитаемый порог теста',
    params STRING COMMENT 'JSON параметров теста',
    sql_text STRING COMMENT 'Отрендеренный SQL теста',
    sample STRING COMMENT 'Примеры нарушающих строк',
    duration_ms BIGINT COMMENT 'Длительность выполнения теста',
    skip_reason STRING COMMENT 'Причина статуса skipped',
    warmup_active BOOLEAN COMMENT 'Был ли активен warmup на момент прогона'
)
USING iceberg
COMMENT 'История прогонов DQ-тестов Feature Platform'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
