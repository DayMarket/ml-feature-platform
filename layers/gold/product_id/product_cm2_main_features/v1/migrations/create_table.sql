CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница Gold snapshot в Asia/Tashkent; часть уникального ключа calculated_at, product_id',
    product_id INT COMMENT 'Положительный идентификатор товара; часть уникального ключа calculated_at, product_id',
    PRODUCT_CM2_MAIN__cm2_main_uzs DOUBLE COMMENT 'Main CM2 товара в UZS после SKU-level расчёта и order-weighted либо mean агрегации'
)
USING iceberg
COMMENT 'Gold: независимый Main CM2 на уровне product_id'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
