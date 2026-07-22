CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    category_mean_sell_price DOUBLE COMMENT 'Средняя цена продажи SKU group внутри категории за дату расчета',
    sell_price_eod DOUBLE COMMENT 'Логарифмированная через log1p средняя цена продажи SKU group на конец дня',
    abs_discount DOUBLE COMMENT 'Абсолютная скидка: медианная полная цена минус медианная цена продажи',
    fraq_discount DOUBLE COMMENT 'Доля цены продажи от полной цены: median_sell_price_eod / median_full_price_eod',
    ratio_crnt_min_to_avg_min_full_price_14 DOUBLE COMMENT 'Отношение минимальной полной цены за вчера к средней минимальной полной цене за предыдущие 14 дней',
    ratio_crnt_min_to_avg_min_full_price_30 DOUBLE COMMENT 'Отношение минимальной полной цены за вчера к средней минимальной полной цене за предыдущие 30 дней'
)
USING iceberg
COMMENT 'Gold-таблица ценовых признаков на уровне sku_group_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
