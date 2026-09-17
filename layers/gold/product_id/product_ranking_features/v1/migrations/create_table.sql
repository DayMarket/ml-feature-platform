CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница Gold snapshot в Asia/Tashkent; часть уникального ключа calculated_at, product_id',
    product_id INT COMMENT 'Положительный идентификатор товара из G7; часть уникального ключа calculated_at, product_id',
    PRODUCT_STATS__price_percentile DOUBLE COMMENT 'Global average-rank percentile min_sell_price_eod',
    PRODUCT_STATS__price_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile min_sell_price_eod внутри листовой категории',
    PRODUCT_STATS__popularity_by_orders_neg_rank DOUBLE COMMENT 'Отрицательный global average rank orders_28d по убыванию',
    PRODUCT_STATS__popularity_by_orders_neg_rank_in_cat DOUBLE COMMENT 'Отрицательный average rank orders_28d внутри листовой категории',
    PRODUCT_STATS__popularity_by_clicks_neg_rank_3d DOUBLE COMMENT 'Отрицательный global average rank clicks_3d по убыванию',
    PRODUCT_STATS__popularity_by_clicks_neg_rank_28d DOUBLE COMMENT 'Отрицательный global average rank clicks_28d по убыванию',
    PRODUCT_STATS__popularity_by_clicks_neg_rank_in_cat_3d DOUBLE COMMENT 'Отрицательный average rank clicks_3d внутри листовой категории',
    PRODUCT_STATS__popularity_by_clicks_neg_rank_in_cat_28d DOUBLE COMMENT 'Отрицательный average rank clicks_28d внутри листовой категории',
    PRODUCT_STATS__rating_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile rating внутри листовой категории',
    PRODUCT_STATS__discount_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile discount внутри листовой категории',
    PRODUCT_STATS__feedback_quantity_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile feedback_quantity внутри листовой категории',
    PRODUCT_STATS__feedback_lte_3_to_orders_rate_smoothed DOUBLE COMMENT 'All-time feedback rating 1..3 к orders_28d, сглаженное global prior с alpha 10',
    PRODUCT_STATS__feedback_lte_3_to_orders_rate_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile smoothed feedback rating 1..3 to orders rate внутри листовой категории',
    PRODUCT_STATS__category_return_rate_14d DOUBLE COMMENT 'Взвешенная доля возвращённых единиц листовой категории за 14 дней',
    PRODUCT_STATS__return_rate_smoothed_14d DOUBLE COMMENT 'Product return rate за 14 дней, сглаженный к baseline листовой категории с alpha 10',
    PRODUCT_STATS__return_rate_to_category_return_rate_14d DOUBLE COMMENT 'Product return rate к return rate листовой категории за 14 дней',
    PRODUCT_STATS__return_rate_smoothed_to_category_return_rate_14d DOUBLE COMMENT 'Smoothed product return rate к return rate листовой категории за 14 дней',
    PRODUCT_STATS__category_return_rate_28d DOUBLE COMMENT 'Взвешенная доля возвращённых единиц листовой категории за 28 дней',
    PRODUCT_STATS__return_rate_smoothed_28d DOUBLE COMMENT 'Product return rate за 28 дней, сглаженный к baseline листовой категории с alpha 10',
    PRODUCT_STATS__return_rate_to_category_return_rate_28d DOUBLE COMMENT 'Product return rate к return rate листовой категории за 28 дней',
    PRODUCT_STATS__return_rate_smoothed_to_category_return_rate_28d DOUBLE COMMENT 'Smoothed product return rate к return rate листовой категории за 28 дней',
    PRODUCT_STATS__category_return_rate_60d DOUBLE COMMENT 'Взвешенная доля возвращённых единиц листовой категории за 60 дней',
    PRODUCT_STATS__return_rate_smoothed_60d DOUBLE COMMENT 'Product return rate за 60 дней, сглаженный к baseline листовой категории с alpha 10',
    PRODUCT_STATS__return_rate_to_category_return_rate_60d DOUBLE COMMENT 'Product return rate к return rate листовой категории за 60 дней',
    PRODUCT_STATS__return_rate_smoothed_to_category_return_rate_60d DOUBLE COMMENT 'Smoothed product return rate к return rate листовой категории за 60 дней',
    PRODUCT_STATS__category_return_rate_90d DOUBLE COMMENT 'Взвешенная доля возвращённых единиц листовой категории за 90 дней',
    PRODUCT_STATS__return_rate_smoothed_90d DOUBLE COMMENT 'Product return rate за 90 дней, сглаженный к baseline листовой категории с alpha 10',
    PRODUCT_STATS__return_rate_to_category_return_rate_90d DOUBLE COMMENT 'Product return rate к return rate листовой категории за 90 дней',
    PRODUCT_STATS__return_rate_smoothed_to_category_return_rate_90d DOUBLE COMMENT 'Smoothed product return rate к return rate листовой категории за 90 дней'
)
USING iceberg
COMMENT 'Gold: population-dependent product ranks, percentiles and smoothed rates over G7'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
