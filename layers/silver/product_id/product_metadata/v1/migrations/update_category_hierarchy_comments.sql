ALTER TABLE {target_table} ALTER COLUMN category_id COMMENT 'Идентификатор листовой категории товара из iceberg.silver.product.category_id; сохраняется независимо от глубины категорийного пути';

ALTER TABLE {target_table} ALTER COLUMN l6_category_id COMMENT 'Идентификатор шестого содержательного уровня от корня; для короткого пути содержит последнюю существующую категорию, а для глубокого пути может отличаться от листового category_id';
