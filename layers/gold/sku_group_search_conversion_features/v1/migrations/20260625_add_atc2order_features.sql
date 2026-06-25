ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_1 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 1, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_3 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 3, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_7 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 7, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_14 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 14, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_21 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 21, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_30 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 30, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_60 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 60, ds - 1]: skg_uniq_orders / skg_uniq_atcs';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_conv_atc2order_90 DOUBLE
COMMENT 'Конверсия из поискового ATC в заказ по sku_group_id за окно [ds - 90, ds - 1]: skg_uniq_orders / skg_uniq_atcs';
