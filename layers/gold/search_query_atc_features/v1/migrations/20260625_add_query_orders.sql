ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_1 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 1, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_3 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 3, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_7 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 7, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_14 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 14, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_21 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 21, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_30 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 30, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_60 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 60, ds - 1]';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS query_orders_90 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 90, ds - 1]';
