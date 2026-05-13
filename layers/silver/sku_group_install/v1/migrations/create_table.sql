CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования',
    install_id STRING COMMENT 'Уникальный идентификатор пользователя',
    sku_group_id BIGINT COMMENT 'ID товара в поиске',
    section STRING COMMENT 'Секция или категория',
    uniqs STRING COMMENT 'Уникальные идентификатор поисковый запрос или категория',
    sum_atc BIGINT COMMENT 'Суммарное число добавлений в корзину',
    sum_clicks BIGINT COMMENT 'Суммарное количество кликов',
    sum_impressions BIGINT COMMENT 'Суммарное количество показов'
)
USING iceberg
COMMENT 'Предагрегированная статистика поиска и категорий на уровне install_id - query/category_id - sku_group_id'
PARTITIONED BY (date)
