ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_3 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 3, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_3';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_7 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 7, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_7';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_14 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 14, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_14';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_21 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 21, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_21';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_30 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 30, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_30';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_60 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 60, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_60';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_orders_1_to_90 DOUBLE COMMENT 'Отношение заказов sku_group_id за вчера к заказам за окно [ds - 90, ds - 1]: skg_uniq_orders_1 / skg_uniq_orders_90';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_3 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 3, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_3';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_7 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 7, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_7';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_14 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 14, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_14';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_21 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 21, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_21';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_30 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 30, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_30';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_60 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 60, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_60';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_incr_rate_atc_1_to_90 DOUBLE COMMENT 'Отношение ATC sku_group_id за вчера к ATC за окно [ds - 90, ds - 1]: skg_uniq_atcs_1 / skg_uniq_atcs_90';
