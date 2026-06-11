CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    category_id BIGINT COMMENT 'ID категории из iceberg.silver.sku',
    avg_advertised_rating_30d_hl14 DOUBLE COMMENT 'Средний рейтинг рекламируемых товаров категории за окно [ds - 30, ds - 1] с экспоненциальным затуханием по дням назад (half-life 14 дней). Рейтинг товара берется на момент дня показа рекламы (отзывы PUBLISHED с date_published < день показа). NULL, если ни один рекламируемый товар категории не имеет отзывов',
    advertised_sku_groups_30d BIGINT COMMENT 'Число уникальных sku_group с рекламными показами (ad_impressions > 0) в окне [ds - 30, ds - 1], отнесенных к категории',
    rated_advertised_sku_groups_30d BIGINT COMMENT 'Число уникальных рекламируемых sku_group категории, у которых был хотя бы один опубликованный отзыв на момент дня показа'
)
USING iceberg
COMMENT 'Gold-таблица признака среднего рейтинга рекламируемых товаров на уровне категории за 30 дней с экспоненциальным затуханием (half-life 14 дней)'
PARTITIONED BY (date)
