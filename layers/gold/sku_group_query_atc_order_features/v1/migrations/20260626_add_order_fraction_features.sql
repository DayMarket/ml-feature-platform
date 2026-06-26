ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_1 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 1, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_3 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 3, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_7 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 7, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_14 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 14, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_21 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 21, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_30 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 30, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_60 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 60, ds - 1]: query_skg_orders / skg_orders';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_orders_frac_all_skg_orders_90 DOUBLE
COMMENT 'Доля заказов query и sku_group_id среди всех заказов sku_group_id за окно [ds - 90, ds - 1]: query_skg_orders / skg_orders';
