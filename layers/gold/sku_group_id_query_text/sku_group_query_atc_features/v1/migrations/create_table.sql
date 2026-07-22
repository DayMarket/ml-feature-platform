CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования',
    sku_group_id BIGINT COMMENT 'ID sku group в поиске',
    query_text STRING COMMENT 'Текст поискового запроса',
    query_skg_conv_imp2atc_1 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 1 день',
    query_skg_conv_imp2atc_3 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 3 дня',
    query_skg_conv_imp2atc_7 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 7 дней',
    query_skg_conv_imp2atc_14 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 14 дней',
    query_skg_conv_imp2atc_21 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 21 день',
    query_skg_conv_imp2atc_30 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 30 дней',
    query_skg_conv_imp2atc_60 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 60 дней',
    query_skg_conv_imp2atc_90 DOUBLE COMMENT 'Конверсия из показа в добавление в корзину за 90 дней',
    share_of_atc_1 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 1 день',
    share_of_atc_3 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 3 дня',
    share_of_atc_7 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 7 дней',
    share_of_atc_14 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 14 дней',
    share_of_atc_21 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 21 день',
    share_of_atc_30 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 30 дней',
    share_of_atc_60 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 60 дней',
    share_of_atc_90 DOUBLE COMMENT 'Доля добавлений в корзину sku group внутри поискового запроса за 90 дней'
)
USING iceberg
COMMENT 'Gold-фичи конверсии поискового запроса и sku_group_id из показа в добавление в корзину'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
