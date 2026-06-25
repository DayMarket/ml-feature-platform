ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_1 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 1, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_3 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 3, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_7 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 7, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_14 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 14, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_21 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 21, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_30 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 30, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_60 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 60, ds - 1]: returned_orders / orders_generated';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_return_rate_90 DOUBLE
COMMENT 'Доля возвращенных поисковых заказов sku_group_id за окно [ds - 90, ds - 1]: returned_orders / orders_generated';
