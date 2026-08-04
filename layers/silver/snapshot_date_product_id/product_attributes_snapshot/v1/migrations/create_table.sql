CREATE TABLE IF NOT EXISTS {target_table} (
    snapshot_date DATE COMMENT 'Дата point-in-time snapshot в Asia/Tashkent',
    product_id BIGINT COMMENT 'ID товара',
    l1_category_id BIGINT COMMENT 'Категория L1; первый содержательный уровень иерархии',
    l2_category_id BIGINT COMMENT 'Категория L2 либо L1, если отдельный L2 отсутствует',
    l3_category_id BIGINT COMMENT 'Категория L3 либо ближайший существующий родитель',
    l4_category_id BIGINT COMMENT 'Категория L4 либо ближайший существующий родитель',
    l5_category_id BIGINT COMMENT 'Категория L5 либо ближайший существующий родитель',
    l6_category_id BIGINT COMMENT 'Конечная category_id товара',
    brand_id BIGINT COMMENT 'Минимальный содержательный brand_name_id товара после исключения business placeholder 160078',
    shop_id BIGINT COMMENT 'ID магазина товара',
    created_at TIMESTAMP COMMENT 'Время создания товара в UTC',
    category_gender STRING COMMENT 'Gender конечной категории: M, F, U или NULL'
)
USING iceberg
COMMENT 'Дневной point-in-time Silver-справочник атрибутов товара'
PARTITIONED BY (snapshot_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
