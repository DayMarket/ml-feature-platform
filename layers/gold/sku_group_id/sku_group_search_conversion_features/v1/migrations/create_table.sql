CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    smooth_conv_imp2order_3 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 3, ds - 1]',
    smooth_conv_imp2order_7 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 7, ds - 1]',
    smooth_conv_imp2order_14 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 14, ds - 1]',
    conv_imp2order_3 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 3, ds - 1], 0 при нулевом знаменателе',
    conv_imp2order_7 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 7, ds - 1], 0 при нулевом знаменателе',
    conv_imp2order_14 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 14, ds - 1], 0 при нулевом знаменателе',
    imp2order_3_to_1 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 3 дня к raw-конверсии за 1 день',
    imp2order_21_to_14 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 21 день к raw-конверсии за 14 дней',
    imp2order_30_to_21 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 30 дней к raw-конверсии за 21 день'
)
USING iceberg
COMMENT 'Gold-таблица совместимых поисковых conversion признаков на уровне sku_group_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
