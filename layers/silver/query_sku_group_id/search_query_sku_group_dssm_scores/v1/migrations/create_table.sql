CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'UTC-дата дневного окна ranking analytics, в котором впервые найдена или заметно изменилась пара query/SKU group',
    query STRING COMMENT 'search_query из ranking analytics events без дополнительной нормализации',
    sku_group_id BIGINT COMMENT 'ID SKU group из ranking_candidates',
    collected_at TIMESTAMP COMMENT 'UTC timestamp записи версии DSSM score в feature-platform',
    dssm_score DOUBLE COMMENT 'DSSM score из external_features по JSON path $.dssm_score'
)
USING iceberg
COMMENT 'Silver: версионированные DSSM scores на уровне query и sku_group_id из ranking analytics logs'
PARTITIONED BY (date)
