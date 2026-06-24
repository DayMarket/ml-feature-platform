CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    query STRING COMMENT 'Нормализованный текст поискового запроса',
    query_uniq_impressions_1 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 1, ds - 1]',
    query_uniq_atcs_1 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 1, ds - 1]',
    query_uniq_impressions_3 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 3, ds - 1]',
    query_uniq_atcs_3 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 3, ds - 1]',
    query_uniq_impressions_7 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 7, ds - 1]',
    query_uniq_atcs_7 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 7, ds - 1]',
    query_uniq_impressions_14 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 14, ds - 1]',
    query_uniq_atcs_14 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 14, ds - 1]',
    query_uniq_impressions_21 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 21, ds - 1]',
    query_uniq_atcs_21 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 21, ds - 1]',
    query_uniq_impressions_30 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 30, ds - 1]',
    query_uniq_atcs_30 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 30, ds - 1]',
    query_uniq_impressions_60 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 60, ds - 1]',
    query_uniq_atcs_60 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 60, ds - 1]',
    query_uniq_impressions_90 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 90, ds - 1]',
    query_uniq_atcs_90 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 90, ds - 1]'
)
USING iceberg
COMMENT 'Gold-фичи количества поисковых показов и ATC по query'
PARTITIONED BY (date)
