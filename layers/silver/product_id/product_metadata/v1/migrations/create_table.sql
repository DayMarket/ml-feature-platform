CREATE TABLE IF NOT EXISTS {target_table} (
    dt TIMESTAMP COMMENT 'Начало локальной даты актуальности метаданных товара (00:00:00 Asia/Tashkent), рассчитанной из Airflow data_interval_end; часть уникального ключа dt, product_id',
    product_id INT COMMENT 'Уникальный идентификатор карточки товара из iceberg.silver.product.id; часть уникального ключа dt, product_id',
    category_id INT COMMENT 'Идентификатор листовой категории товара из iceberg.silver.product.category_id; сохраняется независимо от глубины категорийного пути',
    l1_category_id INT COMMENT 'Идентификатор верхней содержательной категории L1; технический корень category_id = 1 не включается в иерархию',
    l2_category_id INT COMMENT 'Идентификатор категории L2; при отсутствии отдельного второго уровня содержит ближайшего существующего родителя L1',
    l3_category_id INT COMMENT 'Идентификатор категории L3; при отсутствии отдельного третьего уровня содержит ближайшую существующую категорию L2 или L1',
    l4_category_id INT COMMENT 'Идентификатор категории L4; при отсутствии отдельного четвертого уровня содержит ближайшую существующую категорию L3, L2 или L1',
    l5_category_id INT COMMENT 'Идентификатор категории L5; при отсутствии отдельного пятого уровня содержит ближайшую существующую категорию L4, L3, L2 или L1',
    l6_category_id INT COMMENT 'Идентификатор шестого содержательного уровня от корня; для короткого пути содержит последнюю существующую категорию, а для глубокого пути может отличаться от листового category_id',
    brand_id INT COMMENT 'Детерминированный идентификатор бренда товара: минимальный непустой iceberg.silver.sku.brand_name_id после исключения технического значения 160078; NULL означает отсутствие содержательного бренда',
    shop_id INT COMMENT 'Идентификатор магазина-владельца карточки товара из iceberg.silver.product.shop_id; NULL сохраняется без замены',
    created_at TIMESTAMP COMMENT 'Время создания карточки товара из iceberg.silver.product.created_at в UTC; используется Gold-слоем для расчета возраста товара на calculated_at',
    category_gender STRING COMMENT 'Нормализованный доминирующий gender листовой категории из iceberg.silver.recsys_category_genders: M, F, U или NULL при отсутствии либо недопустимом значении'
)
USING iceberg
COMMENT 'Ежедневное состояние категорий, бренда, магазина, времени создания и gender-категории товара на дату dt'
PARTITIONED BY (months(dt))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
