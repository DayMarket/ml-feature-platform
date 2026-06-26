ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_1 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 1, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_3 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 3, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_7 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 7, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_14 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 14, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_21 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 21, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_30 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 30, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_60 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 60, ds - 1]: query_skg_atc / skg_atc';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_skg_atc_frac_all_skg_atc_90 DOUBLE
COMMENT 'Доля ATC query и sku_group_id среди всех ATC sku_group_id за окно [ds - 90, ds - 1]: query_skg_atc / skg_atc';
