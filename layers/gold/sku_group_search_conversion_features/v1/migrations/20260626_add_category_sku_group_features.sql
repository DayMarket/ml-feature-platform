ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_id BIGINT COMMENT 'ID категории sku_group_id из iceberg.silver.sku';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_1 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_3 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_7 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_14 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_21 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_30 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_60 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_90 DOUBLE COMMENT 'Количество поисковых заказов sku_group_id внутри его category_id за окно [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_1 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_3 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_7 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_14 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_21 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_30 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_60 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_orders_90 DOUBLE COMMENT 'Количество поисковых заказов category_id за окно [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_1 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_3 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_7 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_14 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_21 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_30 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_60 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_90 DOUBLE COMMENT 'Количество поисковых ATC sku_group_id внутри его category_id за окно [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_1 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_3 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_7 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_14 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_21 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_30 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_60 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_atc_90 DOUBLE COMMENT 'Количество поисковых ATC category_id за окно [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_1 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_3 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_7 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_14 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_21 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_30 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_60 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_orders_frac_category_orders_90 DOUBLE COMMENT 'Доля поисковых заказов sku_group_id среди заказов его category_id за окно [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_1 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_3 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_7 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_14 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_21 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_30 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_60 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS category_skg_atc_frac_category_atc_90 DOUBLE COMMENT 'Доля поисковых ATC sku_group_id среди ATC его category_id за окно [ds - 90, ds - 1]';
