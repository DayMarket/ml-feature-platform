CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Дата партиции витрины (UTC)',
    query_id STRING COMMENT 'ID поискового запроса; строка, а не число (в Trino — varchar)',
    query_text STRING NOT NULL COMMENT 'Текст поискового запроса; при выгрузке уходит ключом query коллекции SKU_GROUP_CATEGORY_TO_QUERY и должен совпадать со строкой запроса, которую получает ranking-service',
    category_id BIGINT NOT NULL COMMENT 'Категория-кандидат; при выгрузке уходит ключом skuGroupCategoryId',
    relevance INT COMMENT 'Метка релевантности категории запросу: 0, 1 или 2; NULL при выгрузке отправляется как 0.0'
)
USING iceberg
COMMENT 'Метка релевантности категории-кандидата поисковому запросу на грейне date, category_id, query_text. Публикуется в ranking-service набором query_category_relevance (коллекция SKU_GROUP_CATEGORY_TO_QUERY) целиком: по каждой паре category_id, query_text берётся строка с самой свежей date'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
