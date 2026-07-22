CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    avg_sell_price_eod DOUBLE COMMENT 'Средняя цена продажи на конец дня по SKU внутри sku group',
    median_sell_price_eod DOUBLE COMMENT 'Медианная цена продажи на конец дня по SKU внутри sku group',
    min_sell_price_eod DOUBLE COMMENT 'Минимальная цена продажи на конец дня по SKU внутри sku group',
    max_sell_price_eod DOUBLE COMMENT 'Максимальная цена продажи на конец дня по SKU внутри sku group',
    avg_full_price_eod DOUBLE COMMENT 'Средняя полная цена на конец дня по SKU внутри sku group',
    median_full_price_eod DOUBLE COMMENT 'Медианная полная цена на конец дня по SKU внутри sku group',
    min_full_price_eod DOUBLE COMMENT 'Минимальная полная цена на конец дня по SKU внутри sku group',
    max_full_price_eod DOUBLE COMMENT 'Максимальная полная цена на конец дня по SKU внутри sku group'
)
USING iceberg
COMMENT 'Дневная silver-статистика цен на уровне sku_group_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
