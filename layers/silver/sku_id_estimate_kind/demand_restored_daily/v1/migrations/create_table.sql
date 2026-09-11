CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'event_date результата E3 в Asia/Tashkent',
    sku_id BIGINT NOT NULL COMMENT 'Положительный идентификатор SKU',
    estimate_kind STRING NOT NULL COMMENT 'provisional либо final, не mean/quantile',
    run_id STRING NOT NULL COMMENT 'Точный CH run, lineage, не часть ключа FP',
    prediction_date DATE NOT NULL COMMENT 'Отсечка выбранного результата E3',
    sales_units DOUBLE NOT NULL COMMENT 'Исходное значение E3 sales_units, без повторной оценки',
    lost_units DOUBLE COMMENT 'Исходное значение E3 lost_units, без повторной оценки',
    demand_units DOUBLE COMMENT 'Исходное значение E3 demand_units, без повторной оценки',
    potential_units DOUBLE COMMENT 'Исходное значение E3 potential_units, без повторной оценки',
    sales_gmv DOUBLE COMMENT 'Исходное значение E3 sales_gmv, без повторной оценки',
    lost_gmv DOUBLE COMMENT 'Исходное значение E3 lost_gmv, без повторной оценки',
    demand_gmv DOUBLE COMMENT 'Исходное значение E3 demand_gmv, без повторной оценки',
    potential_gmv DOUBLE COMMENT 'Исходное значение E3 potential_gmv, без повторной оценки',
    lost_unit_price DOUBLE COMMENT 'Исходное значение E3 lost_unit_price, без повторной оценки',
    p_active DOUBLE COMMENT 'Исходное значение E3 p_active, без повторной оценки',
    sigma DOUBLE COMMENT 'Исходное значение E3 sigma, без повторной оценки',
    currency_code STRING NOT NULL COMMENT 'Исходная валюта E3, денежные суммы в основных единицах',
    price_model_version STRING NOT NULL COMMENT 'Исходная версия оценки цены',
    rate_ok INT NOT NULL COMMENT 'Исходный признак надёжности ставки 0/1, Iceberg integer',
    settled_at DATE COMMENT 'Исходная дата наблюдения final',
    method_version STRING NOT NULL COMMENT 'Исходная версия метода восстановления',
    quality_status STRING NOT NULL COMMENT 'ok либо unavailable, не статус публикации run',
    unavailable_reason STRING NOT NULL COMMENT 'Причина недоступности, сохраняется без домысливания',
    source_updated_at TIMESTAMP NOT NULL COMMENT 'Исходное updated_at CH, нормализованное в UTC',
    source_state_version BIGINT NOT NULL COMMENT 'Зафиксированная версия состояния CH run',
    source_model_version STRING NOT NULL COMMENT 'Версия модели из паспорта CH run',
    source_code_version STRING NOT NULL COMMENT 'Версия кода из паспорта CH run',
    source_catalog_version STRING NOT NULL COMMENT 'Версия каталога из паспорта CH run',
    source_manifest_id STRING NOT NULL COMMENT 'ID проверяемого переноса выбранного run и диапазона',
    source_contract_version STRING NOT NULL COMMENT 'Версия контракта копирования, не версия модели',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время захвата для копирования UTC'
)
USING iceberg
COMMENT 'Дневная копия точного проверенного E3, без накопления run в ключе'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
