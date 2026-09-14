CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Дата прогона (UTC, data_interval_start): партиция содержит полный снимок пар на эту дату',
    category_id BIGINT NOT NULL COMMENT 'Категория-кандидат из gold.feature_platform_query_category_relevance; при выгрузке уходит ключом skuGroupCategoryId',
    query_text STRING NOT NULL COMMENT 'lower() от формулировки запроса: исходной из витрины или любой формулировки того же query_id из gold.feature_platform_search_query_id; других преобразований нет. При выгрузке уходит ключом query',
    relevance INT COMMENT 'Метка релевантности 0/1/2 из gold.feature_platform_query_category_relevance: максимальная по паре category_id, query_text по всей витрине, даты не учитываются; NULL при выгрузке отправляется как 0.0'
)
USING iceberg
COMMENT 'Релевантность категории запросу на грейне date, category_id, query_text: витрина gold.feature_platform_query_category_relevance, расширенная всеми формулировками query_id из gold.feature_platform_search_query_id. Источник набора query_category_relevance в ranking-service'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
