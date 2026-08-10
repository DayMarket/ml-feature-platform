CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    query STRING COMMENT 'Нормализованный текст поискового запроса',
    query_id STRING COMMENT 'Ключ группы, по которой посчитаны значения строки: каноничный query_id из feature_platform_search_query_id либо сам нормализованный query при отсутствии в справочнике',
    has_query_id BOOLEAN COMMENT 'true, если query нашёлся в справочнике feature_platform_search_query_id; false для фолбэка на сам запрос',
    query_uniq_impressions_1 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 1, ds - 1]',
    query_uniq_atcs_1 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 1, ds - 1]',
    query_orders_1 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 1, ds - 1]',
    query_uniq_impressions_3 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 3, ds - 1]',
    query_uniq_atcs_3 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 3, ds - 1]',
    query_orders_3 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 3, ds - 1]',
    query_uniq_impressions_7 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 7, ds - 1]',
    query_uniq_atcs_7 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 7, ds - 1]',
    query_orders_7 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 7, ds - 1]',
    query_uniq_impressions_14 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 14, ds - 1]',
    query_uniq_atcs_14 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 14, ds - 1]',
    query_orders_14 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 14, ds - 1]',
    query_uniq_impressions_21 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 21, ds - 1]',
    query_uniq_atcs_21 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 21, ds - 1]',
    query_orders_21 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 21, ds - 1]',
    query_uniq_impressions_30 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 30, ds - 1]',
    query_uniq_atcs_30 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 30, ds - 1]',
    query_orders_30 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 30, ds - 1]',
    query_uniq_impressions_60 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 60, ds - 1]',
    query_uniq_atcs_60 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 60, ds - 1]',
    query_orders_60 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 60, ds - 1]',
    query_uniq_impressions_90 DOUBLE COMMENT 'Количество поисковых показов по query за окно [ds - 90, ds - 1]',
    query_uniq_atcs_90 DOUBLE COMMENT 'Количество ATC по query за окно [ds - 90, ds - 1]',
    query_orders_90 DOUBLE COMMENT 'Количество generated orders по query за окно [ds - 90, ds - 1]'
)
USING iceberg
COMMENT 'Gold-фичи количества поисковых показов, ATC и заказов по query, агрегированные в рамках query_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
