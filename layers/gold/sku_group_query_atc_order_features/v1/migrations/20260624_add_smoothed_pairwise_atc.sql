ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_1 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 1, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_3 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 3, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_7 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 7, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_14 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 14, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_21 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 21, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_30 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 30, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_60 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 60, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_smooth_conv_imp2atc_90 DOUBLE
COMMENT 'Сглаженная pairwise-конверсия из показа в ATC по query и sku_group_id за окно [ds - 90, ds - 1]: (query_skg_atc + 100 * skg_conv_imp2atc) / (query_skg_impressions + 100)';
