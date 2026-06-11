CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата дневной партиции (UTC) по fired_at',
    query STRING COMMENT 'Поисковый запрос: lower -> ё→е -> схлопывание пробелов -> trim',
    sku_group_id BIGINT COMMENT 'sku_group_id из ranking_candidates',
    avg_linear_score DOUBLE COMMENT 'Среднее linear_score за день по паре query/sku_group_id',
    avg_normalized_linear_score DOUBLE COMMENT 'Среднее normalized_linear_score за день по паре query/sku_group_id',
    observations BIGINT COMMENT 'Число позиций-кандидатов, усреднённых за день'
)
USING iceberg
COMMENT 'Дневной средний linear_score/normalized_linear_score по query и sku_group_id из ranking_analytics_events (модели search_unified_model_v*)'
PARTITIONED BY (date)
