ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_1 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 1 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_3 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 3 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_7 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 7 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_14 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 14 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_21 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 21 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_30 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 30 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_60 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 60 и date <= ds';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS query_skg_uniq_impressions_90 DOUBLE COMMENT 'Количество показов по query и sku_group_id при date >= ds - 90 и date <= ds';
