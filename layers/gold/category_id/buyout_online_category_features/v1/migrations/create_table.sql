CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции, совпадает с датой партиции buyout_item_signal_features (analyze_date снимка history_order_items)',
    category_id BIGINT COMMENT 'ID категории товара (silver.sku.category_id) — ключ подстановки для sku, которых нет в buyout_online_sku_features',
    cat_n_delivered_90d BIGINT COMMENT 'Позиций категории доставлено за 90 дней — вес собственного сигнала при сглаживании',
    category_buyout_rate_raw_90d DOUBLE COMMENT 'Сырая выкупаемость категории в штуках за 90 дней; NULL при нулевом знаменателе',
    category_no_show_rate_raw_90d DOUBLE COMMENT 'Сырая доля NO SHOW категории за 90 дней',
    category_buyout_rate_90d DOUBLE COMMENT 'Выкупаемость категории за 90 дней, сглаженная к общей выкупаемости маркетплейса (k = 30) — та же величина, что category_buyout_rate_90d в buyout_online_sku_features',
    category_no_show_rate_90d DOUBLE COMMENT 'Доля NO SHOW категории за 90 дней, сглаженная к общей доле маркетплейса (k = 30)',
    marketplace_buyout_rate_90d DOUBLE COMMENT 'Общая выкупаемость маркетплейса в штуках за 90 дней (по строкам категорий сигнала) — последний уровень подстановки',
    marketplace_no_show_rate_90d DOUBLE COMMENT 'Общая доля NO SHOW маркетплейса за 90 дней'
)
USING iceberg
COMMENT 'Таблица категории для сервиса невыкупов: одна строка на category_id за date — сглаженные выкупаемость и доля NO SHOW категории плюс общие по маркетплейсу. Нужна как подстановка для sku без строки в buyout_online_sku_features (нет заказов за 90 дней): модель невыкупов обучена на выкупаемости категории для таких позиций. Партиция совпадает с feature_platform_buyout_item_signal_features. Сервис читает последнюю дату: WHERE date = (SELECT max(date) ...)'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
