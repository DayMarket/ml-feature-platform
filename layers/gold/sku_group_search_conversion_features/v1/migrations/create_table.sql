CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    smooth_conv_imp2order_3 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 3, ds - 1]',
    smooth_conv_imp2order_7 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 7, ds - 1]',
    smooth_conv_imp2order_14 DOUBLE COMMENT 'Сглаженная конверсия из поискового показа в заказ за окно [ds - 14, ds - 1]',
    conv_imp2order_3 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 3, ds - 1], 0 при нулевом знаменателе',
    conv_imp2order_7 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 7, ds - 1], 0 при нулевом знаменателе',
    conv_imp2order_14 DOUBLE COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 14, ds - 1], 0 при нулевом знаменателе',
    skg_days_since_last_impression INT COMMENT 'Количество дней с последнего поискового показа sku_group_id в окне [ds - 90, ds - 1]',
    skg_days_since_last_atc INT COMMENT 'Количество дней с последнего поискового ATC sku_group_id в окне [ds - 90, ds - 1]',
    skg_conv_atc2order_1 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 1, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_3 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 3, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_7 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 7, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_14 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 14, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_21 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 21, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_30 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 30, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_60 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 60, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_conv_atc2order_90 DOUBLE COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 90, ds - 1]: skg_uniq_orders / skg_uniq_atcs',
    skg_return_rate_1 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 1, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_3 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 3, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_7 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 7, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_14 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 14, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_21 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 21, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_30 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 30, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_60 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 60, ds - 1]: returned_orders / orders_generated',
    skg_return_rate_90 DOUBLE COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 90, ds - 1]: returned_orders / orders_generated',
    imp2order_3_to_1 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 3 дня к raw-конверсии за 1 день',
    imp2order_21_to_14 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 21 день к raw-конверсии за 14 дней',
    imp2order_30_to_21 DOUBLE COMMENT 'Отношение raw-конверсии показ -> заказ за 30 дней к raw-конверсии за 21 день'
)
USING iceberg
COMMENT 'Gold-таблица совместимых поисковых conversion признаков на уровне sku_group_id'
PARTITIONED BY (date)
