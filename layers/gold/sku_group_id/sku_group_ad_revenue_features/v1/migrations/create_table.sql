CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета',
    sku_group_id BIGINT COMMENT 'ID sku group',
    smooth_adrev_per_imp_7 DOUBLE COMMENT 'Сглаженный средний заработок с рекламы на показ за окно [ds - 7, ds - 1]: (995 + sum_adrev) / (50 + sum_impressions)',
    smooth_adrev_per_imp_14 DOUBLE COMMENT 'Сглаженный средний заработок с рекламы на показ за окно [ds - 14, ds - 1]: (995 + sum_adrev) / (50 + sum_impressions)',
    smooth_adrev_per_imp_30 DOUBLE COMMENT 'Сглаженный средний заработок с рекламы на показ за окно [ds - 30, ds - 1]: (995 + sum_adrev) / (50 + sum_impressions)',
    adrev_per_imp_7 DOUBLE COMMENT 'Raw средний заработок с рекламы на показ за окно [ds - 7, ds - 1]: sum_adrev / sum_impressions (NULL при нулевом знаменателе)',
    adrev_per_imp_14 DOUBLE COMMENT 'Raw средний заработок с рекламы на показ за окно [ds - 14, ds - 1]: sum_adrev / sum_impressions (NULL при нулевом знаменателе)',
    adrev_per_imp_30 DOUBLE COMMENT 'Raw средний заработок с рекламы на показ за окно [ds - 30, ds - 1]: sum_adrev / sum_impressions (NULL при нулевом знаменателе)'
)
USING iceberg
COMMENT 'Gold-таблица признаков среднего заработка с рекламы (adrev на показ) на уровне sku_group_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
