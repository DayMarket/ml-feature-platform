CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница Gold snapshot в Asia/Tashkent; часть уникального ключа calculated_at, product_id',
    product_id INT COMMENT 'Положительный идентификатор товара; часть уникального ключа calculated_at, product_id',
    PRODUCT_CM2_PDP__score DOUBLE COMMENT 'PDP CM2 товара в USD; равен order-weighted либо mean net inflow',
    PRODUCT_CM2_PDP__net_inflow DOUBLE COMMENT 'Order-weighted либо mean net inflow товара в USD',
    PRODUCT_CM2_PDP__weighted_price DOUBLE COMMENT 'Order-weighted либо mean capped sell price товара в UZS',
    PRODUCT_CM2_PDP__today_rate DOUBLE COMMENT 'Последний не будущий положительный USD rate, использованный в snapshot'
)
USING iceberg
COMMENT 'Gold: независимый PDP CM2 на уровне product_id'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
