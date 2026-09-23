CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница Gold snapshot в Asia/Tashkent; часть уникального ключа calculated_at, product_id',
    product_id INT COMMENT 'Положительный идентификатор товара из G7; часть уникального ключа calculated_at, product_id',
    PRODUCT__price_percentile DOUBLE COMMENT 'Global average-rank percentile min_sell_price_eod',
    PRODUCT__price_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile min_sell_price_eod внутри листовой категории',
    PRODUCT__price_to_avg_price_in_cat_ratio DOUBLE COMMENT 'min_sell_price_eod / средняя min_sell_price_eod по листовой категории; значение может быть больше 1',
    PRODUCT__popularity_by_orders_neg_rank DOUBLE COMMENT 'Отрицательный global average rank orders_28d по убыванию',
    PRODUCT__popularity_by_orders_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile orders_28d внутри листовой категории; более популярные товары ближе к 1',
    PRODUCT__popularity_by_clicks_neg_rank_3d DOUBLE COMMENT 'Отрицательный global average rank clicks_3d по убыванию',
    PRODUCT__popularity_by_clicks_neg_rank_28d DOUBLE COMMENT 'Отрицательный global average rank clicks_28d по убыванию',
    PRODUCT__popularity_by_clicks_percentile_in_cat_3d DOUBLE COMMENT 'Average-rank percentile clicks_3d внутри листовой категории; более популярные товары ближе к 1',
    PRODUCT__popularity_by_clicks_percentile_in_cat_28d DOUBLE COMMENT 'Average-rank percentile clicks_28d внутри листовой категории; более популярные товары ближе к 1',
    PRODUCT__rating_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile rating внутри листовой категории',
    PRODUCT__discount_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile discount внутри листовой категории',
    PRODUCT__feedback_quantity_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile feedback_quantity внутри листовой категории',
    PRODUCT__feedback_to_orders_rate_smoothed DOUBLE COMMENT 'Общая доля feedback за 28 дней к orders_28d, сглаженная global prior с alpha 10',
    PRODUCT__feedback_to_orders_rate_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile общей сглаженной feedback-to-orders rate внутри листовой категории',
    PRODUCT__feedback_gte_4_to_orders_rate_smoothed DOUBLE COMMENT 'Доля feedback rating 4..5 за 28 дней к orders_28d, сглаженная global prior с alpha 10',
    PRODUCT__feedback_gte_4_to_orders_rate_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile сглаженной feedback rating 4..5 to orders rate внутри листовой категории',
    PRODUCT__feedback_lte_3_to_orders_rate_smoothed DOUBLE COMMENT 'Feedback rating 1..3 за 28 дней к orders_28d, сглаженное global prior с alpha 10',
    PRODUCT__feedback_lte_3_to_orders_rate_percentile_in_cat DOUBLE COMMENT 'Average-rank percentile smoothed feedback rating 1..3 to orders rate внутри листовой категории',
    PRODUCT__category_return_rate_neg_28d DOUBLE COMMENT 'Взвешенная отрицательная доля строк RETURNED в листовой категории за 28 дней',
    PRODUCT__return_rate_neg_smoothed_28d DOUBLE COMMENT 'Отрицательная product return rate, сглаженная к baseline листовой категории за 28 дней с alpha 10',
    PRODUCT__return_rate_to_category_return_neg_28d DOUBLE COMMENT 'Положительная product return rate / отрицательный baseline листовой категории за 28 дней; значение не выше 0, больше — лучше',
    PRODUCT__return_rate_smoothed_to_category_return_neg_28d DOUBLE COMMENT 'Сглаженная положительная product return rate / отрицательный baseline листовой категории за 28 дней; значение не выше 0, больше — лучше',
    PRODUCT__category_return_rate_neg_90d DOUBLE COMMENT 'Взвешенная отрицательная доля строк RETURNED в листовой категории за 90 дней',
    PRODUCT__return_rate_neg_smoothed_90d DOUBLE COMMENT 'Отрицательная product return rate, сглаженная к baseline листовой категории за 90 дней с alpha 10',
    PRODUCT__return_rate_to_category_return_neg_90d DOUBLE COMMENT 'Положительная product return rate / отрицательный baseline листовой категории за 90 дней; значение не выше 0, больше — лучше',
    PRODUCT__return_rate_smoothed_to_category_return_neg_90d DOUBLE COMMENT 'Сглаженная положительная product return rate / отрицательный baseline листовой категории за 90 дней; значение не выше 0, больше — лучше'
)
USING iceberg
COMMENT 'Gold: population-dependent product ranks, percentiles and smoothed rates over G7'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
