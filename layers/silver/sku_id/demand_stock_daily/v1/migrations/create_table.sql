CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Исходная dt EOD без сдвига к полуночи Ташкента',
    sku_id BIGINT NOT NULL COMMENT 'SKU с quantity_active_eod > 0 либо quantity_fbs_eod > 0',
    source_manifest_id STRING NOT NULL COMMENT 'ID полного source capture диапазона',
    source_contract_version STRING NOT NULL COMMENT 'Версия фильтра положительного EOD-наличия',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время захвата для записи, UTC'
)
USING iceberg
COMMENT 'Разреженное дневное EOD-наличие SKU; отсутствие строки внутри принятого дня означает ноль'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
