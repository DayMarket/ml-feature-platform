ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS conv_imp2order_3 DOUBLE
COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 3, ds - 1], 0 при нулевом знаменателе';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS conv_imp2order_7 DOUBLE
COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 7, ds - 1], 0 при нулевом знаменателе';

ALTER TABLE {target_table}
ADD COLUMN IF NOT EXISTS conv_imp2order_14 DOUBLE
COMMENT 'Raw-конверсия из поискового показа в заказ за окно [ds - 14, ds - 1], 0 при нулевом знаменателе';
