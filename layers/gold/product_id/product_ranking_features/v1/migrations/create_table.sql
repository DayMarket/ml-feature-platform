CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница Gold snapshot в Asia/Tashkent; часть уникального ключа calculated_at, product_id',
    product_id INT COMMENT 'Положительный идентификатор товара из G7; часть уникального ключа calculated_at, product_id',
    PRODUCT__price_percentile DOUBLE COMMENT 'Global average-rank percentile min_sell_price_eod',
    PRODUCT__price_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile min_sell_price_eod внутри листовой категории',
    PRODUCT__popularity_by_orders_neg_rank DOUBLE COMMENT 'Отрицательный global average rank orders_28d по убыванию',
    PRODUCT__popularity_by_orders_neg_rank_in_cat DOUBLE COMMENT 'Отрицательный average rank orders_28d внутри листовой категории',
    PRODUCT__popularity_by_clicks_neg_rank_3d DOUBLE COMMENT 'Отрицательный global average rank clicks_3d по убыванию',
    PRODUCT__popularity_by_clicks_neg_rank_28d DOUBLE COMMENT 'Отрицательный global average rank clicks_28d по убыванию',
    PRODUCT__popularity_by_clicks_neg_rank_in_cat_3d DOUBLE COMMENT 'Отрицательный average rank clicks_3d внутри листовой категории',
    PRODUCT__popularity_by_clicks_neg_rank_in_cat_28d DOUBLE COMMENT 'Отрицательный average rank clicks_28d внутри листовой категории',
    PRODUCT__rating_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile rating внутри листовой категории',
    PRODUCT__discount_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile discount внутри листовой категории',
    PRODUCT__feedback_quantity_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile feedback_quantity внутри листовой категории',
    PRODUCT__feedback_lte_3_to_orders_rate_smoothed DOUBLE COMMENT 'Feedback rating 1..3 за 28 дней к orders_28d, сглаженное global prior с alpha 10',
    PRODUCT__feedback_lte_3_to_orders_rate_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile smoothed feedback rating 1..3 to orders rate внутри листовой категории',
    PRODUCT__category_return_rate_neg_28d DOUBLE COMMENT 'Взвешенная отрицательная доля строк RETURNED в листовой категории за 28 дней',
    PRODUCT__return_rate_neg_smoothed_28d DOUBLE COMMENT 'Отрицательная product return rate, сглаженная к baseline листовой категории за 28 дней с alpha 10',
    PRODUCT__return_rate_neg_to_category_return_rate_neg_28d DOUBLE COMMENT 'Отношение отрицательной product return rate к отрицательному baseline листовой категории за 28 дней',
    PRODUCT__return_rate_neg_smoothed_to_category_return_rate_neg_28d DOUBLE COMMENT 'Отношение сглаженной отрицательной product return rate к отрицательному baseline листовой категории за 28 дней'
)
USING iceberg
COMMENT 'Gold: population-dependent product ranks, percentiles and smoothed rates over G7'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
