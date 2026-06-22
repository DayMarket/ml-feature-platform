CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата расчёта фич (data_interval_end в UTC)',
    h3_index BIGINT COMMENT 'H3-индекс гексагона res 9 (UInt64 источника приведён к BIGINT)',
    report_date DATE COMMENT 'Фактическая дата снимка gold.geo_client_hist (max report_date <= date)',
    users_r0 BIGINT COMMENT 'Число пользователей в радиусе <= 0 колец на report_date',
    users_r1 BIGINT COMMENT 'Число пользователей в радиусе <= 1 колец на report_date',
    users_r2 BIGINT COMMENT 'Число пользователей в радиусе <= 2 колец на report_date',
    users_r3 BIGINT COMMENT 'Число пользователей в радиусе <= 3 колец на report_date',
    users_r4 BIGINT COMMENT 'Число пользователей в радиусе <= 4 колец на report_date',
    users_r5 BIGINT COMMENT 'Число пользователей в радиусе <= 5 колец на report_date'
)
USING iceberg
COMMENT 'Silver: число пользователей по кольцам H3 на последнем снимке geo_client_hist'
PARTITIONED BY (date)
