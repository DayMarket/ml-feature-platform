CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 12-часового среза в часовом поясе Asia/Tashkent',
    product_id INT COMMENT 'Положительный идентификатор товара; часть уникального ключа calculated_at, product_id',
    n_feedbacks_1 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 1 внутри 12-часового среза',
    n_feedbacks_2 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 2 внутри 12-часового среза',
    n_feedbacks_3 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 3 внутри 12-часового среза',
    n_feedbacks_4 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 4 внутри 12-часового среза',
    n_feedbacks_5 BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 5 внутри 12-часового среза'
)
USING iceberg
COMMENT 'Аддитивные 12-часовые feedback-факты на уровне товара'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
