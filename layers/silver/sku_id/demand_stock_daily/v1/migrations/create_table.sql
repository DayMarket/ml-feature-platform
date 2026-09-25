CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Исходная dt EOD без сдвига к полуночи Ташкента',
    sku_id BIGINT NOT NULL COMMENT 'SKU с quantity_active_eod > 0 либо quantity_fbs_eod > 0',
    purchase_price_eod BIGINT COMMENT 'Цена покупки для покупателя на конец дня, UZS (daily_sku_quantity_eod.purchase_price_eod); NULL при 0 в источнике',
    sell_price_eod BIGINT COMMENT 'Цена продавца из карточки на конец дня, UZS (daily_sku_quantity_eod.sell_price_eod); NULL при 0 в источнике',
    full_price_eod BIGINT COMMENT 'Цена до скидки (зачёркнутая) на конец дня, UZS (daily_sku_quantity_eod.full_price_eod); NULL при 0 в источнике',
    source_manifest_id STRING NOT NULL COMMENT 'ID полного source capture диапазона',
    source_contract_version STRING NOT NULL COMMENT 'Версия фильтра положительного EOD-наличия',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время захвата для записи, UTC'
)
USING iceberg
COMMENT 'Разреженное дневное EOD-наличие SKU с ценами на конец дня; отсутствие строки внутри принятого дня означает ноль'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
