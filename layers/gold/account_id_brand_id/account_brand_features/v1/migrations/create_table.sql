CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Момент Gold snapshot: 00:00 или 12:00 Asia/Tashkent; часть ключа calculated_at, account_id, brand_id',
    account_id INT COMMENT 'Положительный идентификатор пользователя; часть ключа таблицы',
    brand_id INT COMMENT 'Содержательный бренд из S1; NULL и технический brand_id 160078 не публикуются; часть ключа таблицы',
    ACCOUNT_BRAND__n_clicks_3d INT COMMENT 'Сумма product-level distinct session_id с PRODUCT_VIEW пользователя по товарам бренда за 3 дня',
    ACCOUNT_BRAND__n_clicks_7d INT COMMENT 'Сумма product-level distinct session_id с PRODUCT_VIEW пользователя по товарам бренда за 7 дней',
    ACCOUNT_BRAND__n_clicks_14d INT COMMENT 'Сумма product-level distinct session_id с PRODUCT_VIEW пользователя по товарам бренда за 14 дней',
    ACCOUNT_BRAND__n_clicks_28d INT COMMENT 'Сумма product-level distinct session_id с PRODUCT_VIEW пользователя по товарам бренда за 28 дней',
    ACCOUNT_BRAND__gmv_3d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 3 дня: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_7d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 7 дней: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_14d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 14 дней: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_28d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 28 дней: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_60d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 60 дней: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_90d DOUBLE COMMENT 'GMV успешных позиций пользователя с товарами бренда за 90 дней: сумма payment_price умножить на item_quantity',
    ACCOUNT_BRAND__gmv_3d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 3 дня; denominator включает товары без содержательного бренда',
    ACCOUNT_BRAND__gmv_7d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 7 дней; denominator включает товары без содержательного бренда',
    ACCOUNT_BRAND__gmv_14d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 14 дней; denominator включает товары без содержательного бренда',
    ACCOUNT_BRAND__gmv_28d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 28 дней; denominator включает товары без содержательного бренда',
    ACCOUNT_BRAND__gmv_60d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 60 дней; denominator включает товары без содержательного бренда',
    ACCOUNT_BRAND__gmv_90d_ratio DOUBLE COMMENT 'Доля GMV бренда в GMV всех покупок пользователя за 90 дней; denominator включает товары без содержательного бренда'
)
USING iceberg
COMMENT 'Account-brand rolling clicks, GMV и GMV ratios для рекомендательных моделей'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
