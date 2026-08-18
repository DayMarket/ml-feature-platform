CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции, совпадает с датой партиции buyout_item_signal_features (analyze_date снапшота)',
    sku_id BIGINT COMMENT 'ID товарной позиции (silver.sku.id) — ключ обращения сервиса невыкупов',
    product_id BIGINT COMMENT 'ID карточки товара (silver.sku.product_id)',
    category_id BIGINT COMMENT 'ID категории товара (silver.sku.category_id)',
    shop_id BIGINT COMMENT 'ID магазина (silver.sku.shop_id)',
    brand_name_id BIGINT COMMENT 'ID бренда (silver.sku.brand_name_id)',
    sku_n_delivered_90d BIGINT COMMENT 'Позиций sku доставлено за 90 дней — вес собственного сигнала при сглаживании',
    sku_n_delivered_30d BIGINT COMMENT 'Позиций sku доставлено за 30 дней',
    sku_buyout_rate_90d DOUBLE COMMENT 'Сырая выкупаемость sku в штуках за 90 дней; NULL при нулевом знаменателе',
    sku_buyout_rate_30d DOUBLE COMMENT 'Сырая выкупаемость sku в штуках за 30 дней',
    sku_no_show_rate_90d DOUBLE COMMENT 'Сырая доля NO SHOW у sku за 90 дней',
    sku_nonbuyout_rate_90d DOUBLE COMMENT 'Сырая доля клиентского невыкупа у sku за 90 дней',
    sku_buyout_rate_money_90d DOUBLE COMMENT 'Денежная выкупаемость sku за 90 дней',
    sku_cancel_before_delivery_rate_90d DOUBLE COMMENT 'Доля отмен до доставки у sku за 90 дней',
    product_n_delivered_90d BIGINT COMMENT 'Позиций карточки товара доставлено за 90 дней',
    product_buyout_rate_90d DOUBLE COMMENT 'Сырая выкупаемость карточки товара за 90 дней',
    product_no_show_rate_90d DOUBLE COMMENT 'Сырая доля NO SHOW у карточки товара за 90 дней',
    cat_n_delivered_90d BIGINT COMMENT 'Позиций категории доставлено за 90 дней',
    category_buyout_rate_90d DOUBLE COMMENT 'Выкупаемость категории за 90 дней, сглаженная к общей выкупаемости маркетплейса (k = 30)',
    category_no_show_rate_90d DOUBLE COMMENT 'Доля NO SHOW категории за 90 дней, сглаженная к общей выкупаемости маркетплейса (k = 30)',
    shop_n_delivered_90d BIGINT COMMENT 'Позиций магазина доставлено за 90 дней',
    shop_buyout_rate_90d DOUBLE COMMENT 'Сырая выкупаемость магазина за 90 дней',
    brand_n_delivered_90d BIGINT COMMENT 'Позиций бренда доставлено за 90 дней',
    brand_buyout_rate_90d DOUBLE COMMENT 'Сырая выкупаемость бренда за 90 дней',
    sku_buyout_rate_shrunk_90d DOUBLE COMMENT 'Выкупаемость sku за 90 дней, стянутая к сглаженной ставке своей категории (k = 30)',
    sku_no_show_rate_shrunk_90d DOUBLE COMMENT 'Доля NO SHOW у sku за 90 дней, стянутая к сглаженной ставке своей категории (k = 30)',
    product_buyout_rate_shrunk_90d DOUBLE COMMENT 'Выкупаемость карточки товара за 90 дней, стянутая к сглаженной ставке категории (k = 30)',
    sku_vs_product_gap_90d DOUBLE COMMENT 'Разрыв sku и карточки: sku_buyout_rate_shrunk_90d - product_buyout_rate_shrunk_90d (гипотеза размерного эффекта)'
)
USING iceberg
COMMENT 'Таблица товара для сервиса невыкупов: одна строка на sku_id за date — выкупаемость самого sku, его карточки, категории, магазина и бренда плюс сглаженные оценки. Сглаживание: shrunk = (доля_родителя · 30 + доля_sku · n_доставок) / (30 + n_доставок); категория сглаживается к общей выкупаемости маркетплейса, sku и карточка — к сглаженной доле своей категории. Партиция совпадает с feature_platform_buyout_item_signal_features. Сервис читает последнюю дату: WHERE date = (SELECT max(date) ...)'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
