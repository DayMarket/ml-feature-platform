ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_days_since_last_impression INT
COMMENT 'Количество дней с последнего поискового показа sku_group_id в окне [ds - 90, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS skg_days_since_last_atc INT
COMMENT 'Количество дней с последнего поискового ATC sku_group_id в окне [ds - 90, ds - 1]';
