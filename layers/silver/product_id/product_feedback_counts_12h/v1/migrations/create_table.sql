CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 12-часового среза в часовом поясе Asia/Tashkent',
    product_id INT COMMENT 'Положительный идентификатор товара; часть уникального ключа calculated_at, product_id',
    feedback_count BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом от 1 до 5',
    rating_sum BIGINT COMMENT 'Сумма валидных рейтингов опубликованных отзывов',
    feedback_gte_4 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 4 или 5',
    feedback_lte_3 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом от 1 до 3'
)
USING iceberg
COMMENT 'Аддитивные 12-часовые feedback-факты на уровне товара'
PARTITIONED BY (hours(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
