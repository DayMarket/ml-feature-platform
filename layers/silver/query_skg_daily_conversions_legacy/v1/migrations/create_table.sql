CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата дневной партиции',
    query STRING COMMENT 'Поисковый запрос в legacy-нормализации: lower для событий, trim после джойна заказов',
    platform STRING COMMENT 'Платформа события или last_atc_platform из атрибуции заказов',
    sku_group_id BIGINT COMMENT 'ID sku group',
    uniq_impressions BIGINT COMMENT 'Количество уникальных session_id с PRODUCT_IMPRESSION в SEARCH_RESULTS',
    uniq_clicks BIGINT COMMENT 'Количество уникальных session_id с PRODUCT_VIEW после SEARCH_RESULTS',
    uniq_atcs BIGINT COMMENT 'Количество уникальных session_id с ADD_TO_CART после SEARCH_RESULTS',
    uniq_orders BIGINT COMMENT 'Количество уникальных order_item_id из SEARCH_RESULTS'
)
USING iceberg
COMMENT 'Legacy daily conversions по query, platform и sku_group_id для восстановления fs_search_query_skg_v3'
PARTITIONED BY (date)

