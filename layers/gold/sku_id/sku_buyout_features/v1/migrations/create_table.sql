CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции, совпадает с датой партиции feature_platform_buyout_online_sku_features',
    sku_id BIGINT COMMENT 'ID товарной позиции (silver.sku.id) — ключ обращения сервиса невыкупов',
    product_id BIGINT COMMENT 'ID карточки товара (silver.sku.product_id); по нему берутся рекламные ставки CPO',
    seller_id BIGINT COMMENT 'ID продавца (silver.sku.seller_id); только для аналитики, на решение модели не влияет',
    category_id BIGINT COMMENT 'ID категории (silver.sku.category_id) — ключ джойна с dict.category; в PostgreSQL не выгружается',
    l1_category BIGINT COMMENT 'Категория 1 уровня из dict.category',
    l2_category BIGINT COMMENT 'Категория 2 уровня; при нуле в справочнике подставляется l1_category',
    l3_category BIGINT COMMENT 'Категория 3 уровня; при нуле подставляется последний ненулевой уровень выше',
    l4_category BIGINT COMMENT 'Категория 4 уровня; при нуле подставляется последний ненулевой уровень выше',
    l5_category BIGINT COMMENT 'Категория 5 уровня; при нуле подставляется последний ненулевой уровень выше',
    type VARCHAR COMMENT 'Тип товара: 1p при продавце is_1p = 1 с известной себестоимостью, иначе 3p',
    commission DECIMAL(5,2) COMMENT 'Процент комиссии из kazanexpress.public.sku.commission; NULL у 1p',
    cost_price BIGINT COMMENT 'Себестоимость за штуку из последней приёмки stock_flow_1p; NULL у 3p',
    is_not_block BOOLEAN COMMENT 'Правило «не отключать постоплату». Источника правила пока нет, колонка всегда false',
    sku_buyout DOUBLE COMMENT 'Выкупаемость sku за 90 дней, стянутая к категории (sku_buyout_rate_shrunk_90d)',
    product_buyout DOUBLE COMMENT 'Выкупаемость карточки товара за 90 дней, стянутая к категории (product_buyout_rate_shrunk_90d)',
    category_buyout DOUBLE COMMENT 'Выкупаемость категории за 90 дней, сглаженная к маркетплейсу (category_buyout_rate_90d); _shrunk-варианта не существует',
    shop_buyout DOUBLE COMMENT 'Выкупаемость магазина за 90 дней, стянутая к маркетплейсу (shop_buyout_rate_shrunk_90d)',
    category_no_show DOUBLE COMMENT 'Доля NO SHOW категории за 90 дней, сглаженная к маркетплейсу (category_no_show_rate_90d)',
    sku_n_delivered BIGINT COMMENT 'Позиций sku доставлено за 90 дней (sku_n_delivered_90d); вес сглаживания',
    product_n_delivered BIGINT COMMENT 'Позиций карточки товара доставлено за 90 дней (product_n_delivered_90d)',
    predicted_dimensional_group VARCHAR COMMENT 'Габаритная группа по сумме height+length+width; при неизвестных габаритах берётся silver.sku.dimensional_group'
)
USING iceberg
COMMENT 'Экономика корзины и выкупаемость на грейне sku_id для сервиса невыкупов: 1p/3p с себестоимостью или комиссией, выкупаемость sku/карточки/категории/магазина в _shrunk-версиях, категорийное дерево l1..l5. Строки — весь sku-универс silver.sku; у sku вне feature_platform_buyout_online_sku_features колонки выкупаемости NULL. Сервис читает последнюю дату: WHERE date = (SELECT max(date) ...)'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
