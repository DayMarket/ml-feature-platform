-- Признаки, которые сервис невыкупов раньше досчитывал сам в запросе на чекауте.
-- Upload публикует сырые колонки без выражений и без join-ов (одна feature group — одна
-- таблица), поэтому индикаторы и гео-доли материализуются здесь.
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS first_completed_order_is_postpaid INT COMMENT 'Первый выкупленный заказ был постоплатным (0/1); NULL, если first_issued_payment_type пуст — выкупа на дату снимка ещё не было';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS has_asof_history INT COMMENT 'У аккаунта есть as-of история: в этой таблице всегда 1, ноль сервис подставляет сам за отсутствующий в feature store аккаунт';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS is_first_order_ever INT COMMENT 'За всю жизнь аккаунта не было ни одного созданного заказа, first_order_id_ever IS NULL (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_nonbuyout_no_show INT COMMENT 'last_nonbuyout_type = no_show (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_nonbuyout_cancel_after INT COMMENT 'last_nonbuyout_type = cancel_after_delivery (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_nonbuyout_courier_other INT COMMENT 'last_nonbuyout_type = courier_or_other (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_pay_uzumcard INT COMMENT 'last_order_payment_type = UzumCard (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_pay_nasiya INT COMMENT 'last_order_payment_type = Nasiya (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_pay_uzumcheckout INT COMMENT 'last_order_payment_type = UzumCheckout (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS last_pay_bonus INT COMMENT 'last_order_payment_type = BONUS (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS first_dp_pickup_point INT COMMENT 'first_delivery_point_type = DELIVERY_POINT (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS first_dp_franchise INT COMMENT 'first_delivery_point_type = FRANCHISE (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS first_dp_uzpost INT COMMENT 'first_delivery_point_type = UZ_POST (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS first_dp_missing INT COMMENT 'first_delivery_point_type — пустая строка: тип точки доставки первого заказа неизвестен (0/1)';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS prev_city_part_completed DOUBLE COMMENT 'part_completed_orders города последнего заказа (silver.feature_platform_order_completion_city_features за ту же партицию date); NULL, если города нет в гео-витрине';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS prev_city_part_no_show DOUBLE COMMENT 'part_no_show_from_total города последнего заказа за ту же партицию date; NULL, если города нет в гео-витрине';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS prev_region_part_completed DOUBLE COMMENT 'part_completed_orders региона последнего заказа (silver.feature_platform_order_completion_region_features за ту же партицию date); NULL, если региона нет в гео-витрине';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS prev_region_part_no_show DOUBLE COMMENT 'part_no_show_from_total региона последнего заказа за ту же партицию date; NULL, если региона нет в гео-витрине';
ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS has_prev_city INT COMMENT 'Город последнего заказа нашёлся в гео-витрине городов (0/1)';
