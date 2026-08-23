CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'analyze_date снапшота history_order_items, за который собран признак; потребитель мерджит к date + 1',
    category_level STRING COMMENT 'Уровень иерархии категории: leaf (категория позиции как есть), l1, l2, l3, l4 из dict.category',
    category_id BIGINT COMMENT 'ID категории на уровне category_level; для leaf это history_order_items.category_id',
    part_completed_orders DOUBLE COMMENT 'Доля заказов категории со статусом COMPLETED от всех неактивных заказов за окно 120 дней по generated_at',
    part_no_show_from_total DOUBLE COMMENT 'Доля заказов категории со статусом RETURNED NO SHOW от всех неактивных заказов за окно 120 дней по generated_at'
)
USING iceberg
COMMENT 'Silver: дневной снапшот долей выкупа и невыкупа заказов по уровням иерархии категорий leaf/l1/l2/l3/l4'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
