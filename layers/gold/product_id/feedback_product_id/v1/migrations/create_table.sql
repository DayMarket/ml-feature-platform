CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    product_id BIGINT COMMENT 'ID товара',
    product_rating DOUBLE COMMENT 'Средний рейтинг товара по опубликованным отзывам за все время на дату расчета',
    bad_reviews_count BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 1, 2 или 3 за все время на дату расчета',
    good_reviews_count BIGINT COMMENT 'Количество опубликованных отзывов с рейтингом 4 или 5 за все время на дату расчета',
    total_reviews_count BIGINT COMMENT 'Общее количество опубликованных отзывов за все время на дату расчета',
    reviews_mark_one_count BIGINT COMMENT 'Количество опубликованных отзывов с оценкой 1 за все время на дату расчета',
    reviews_mark_two_count BIGINT COMMENT 'Количество опубликованных отзывов с оценкой 2 за все время на дату расчета',
    reviews_mark_three_count BIGINT COMMENT 'Количество опубликованных отзывов с оценкой 3 за все время на дату расчета',
    reviews_mark_four_count BIGINT COMMENT 'Количество опубликованных отзывов с оценкой 4 за все время на дату расчета',
    reviews_mark_five_count BIGINT COMMENT 'Количество опубликованных отзывов с оценкой 5 за все время на дату расчета',
    total_reviews_with_text BIGINT COMMENT 'Количество опубликованных отзывов с непустым текстом за все время на дату расчета',
    ratio_reviews_mark_one DOUBLE COMMENT 'Доля отзывов с оценкой 1 от общего количества опубликованных отзывов',
    ratio_reviews_mark_two DOUBLE COMMENT 'Доля отзывов с оценкой 2 от общего количества опубликованных отзывов',
    ratio_reviews_mark_three DOUBLE COMMENT 'Доля отзывов с оценкой 3 от общего количества опубликованных отзывов',
    ratio_reviews_mark_four DOUBLE COMMENT 'Доля отзывов с оценкой 4 от общего количества опубликованных отзывов',
    ratio_reviews_mark_five DOUBLE COMMENT 'Доля отзывов с оценкой 5 от общего количества опубликованных отзывов',
    ratio_reviews_bad DOUBLE COMMENT 'Доля отзывов с оценкой 1, 2 или 3 от общего количества опубликованных отзывов',
    ratio_reviews_good DOUBLE COMMENT 'Доля отзывов с оценкой 4 или 5 от общего количества опубликованных отзывов'
)
USING iceberg
COMMENT 'Gold-фичи опубликованных отзывов и рейтинга товара на уровне product_id за все время на дату расчета'
PARTITIONED BY (date)
