CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    price_index_status INT COMMENT 'Числовой статус price index для обратной совместимости со старой моделью'
)
USING iceberg
COMMENT 'Временная gold-таблица price index статусов на уровне sku_group_id для обратной совместимости'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
