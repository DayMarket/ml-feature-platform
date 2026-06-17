CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата дневной партиции в таймзоне Asia/Tashkent',
    account_id BIGINT COMMENT 'ID аккаунта, только account_id > 0',
    category_id BIGINT COMMENT 'ID категории уровня L2; при отсутствии L2 используется L1',
    n_imps BIGINT COMMENT 'Кол-во уникальных пар (session_id, product_id) из данной category_id с событием PRODUCT_IMPRESSION',
    n_clicks BIGINT COMMENT 'Кол-во уникальных пар (session_id, product_id) из данной category_id с событием PRODUCT_VIEW',
    n_atcs BIGINT COMMENT 'Кол-во уникальных пар (session_id, product_id) из данной category_id с событием ADD_TO_CART',
    n_atfs BIGINT COMMENT 'Кол-во уникальных пар (session_id, product_id) из данной category_id с событием ADD_TO_FAVORITES'
)
USING iceberg
COMMENT 'Daily event counts по account_id и L2 category_id'
PARTITIONED BY (date)
