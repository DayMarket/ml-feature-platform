CREATE TABLE IF NOT EXISTS {target_table} (
    snapshot_date DATE COMMENT 'Дата снимка в часовом поясе Asia/Tashkent',
    account_id BIGINT COMMENT 'Положительный идентификатор пользователя',
    gender STRING COMMENT 'Нормализованный gender пользователя: M, F или NULL',
    age INTEGER COMMENT 'Возраст пользователя в полных годах на snapshot_date либо NULL'
)
USING iceberg
COMMENT 'Silver: канонический дневной snapshot демографических атрибутов пользователя'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
