CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'analyze_date снапшота history_order_items, за который собран признак; потребитель мерджит к date + 1',
    order_city_id BIGINT COMMENT 'ID города доставки заказа из history_order_items.order_city_id',
    part_completed_orders DOUBLE COMMENT 'Доля заказов города со статусом COMPLETED от всех неактивных заказов за окно 120 дней по generated_at',
    part_no_show_from_total DOUBLE COMMENT 'Доля заказов города со статусом RETURNED NO SHOW от всех неактивных заказов за окно 120 дней по generated_at'
)
USING iceberg
COMMENT 'Silver: дневной снапшот долей выкупа и невыкупа заказов на уровне города доставки'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
