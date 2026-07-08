ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS normalized_linear_score DOUBLE COMMENT 'Средний normalized_linear_score из ranking analytics events для query и sku_group_id за event_date';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS linear_score DOUBLE COMMENT 'Средний linear_score из ranking analytics events для query и sku_group_id за event_date';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS dssm_score DOUBLE COMMENT 'Средний dssm_score из ranking analytics events для query и sku_group_id за event_date';
