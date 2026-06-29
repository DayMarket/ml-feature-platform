ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS calculated_for_price DOUBLE COMMENT 'Цена, для которой был рассчитан discount_amount';
