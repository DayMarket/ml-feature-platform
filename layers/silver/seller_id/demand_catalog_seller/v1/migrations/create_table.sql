CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Дата фактического захвата полного справочника Asia/Tashkent, не исторический cutoff',
    seller_id BIGINT NOT NULL COMMENT 'Положительный seller_id полного marts.sellers_info без фильтра текущими SKU',
    source_master_seller_id STRING COMMENT 'Исходная master-связь, NULL и пустая строка различаются',
    master_seller_id STRING COMMENT 'Нормализованный master либо seller_id строкой при доказанном unmatched, иначе NULL',
    seller_mapping_status STRING NOT NULL COMMENT 'matched, unmatched, unavailable или conflict, последние два блокируют DQ',
    has_master BOOLEAN COMMENT 'TRUE при matched, FALSE при unmatched, иначе NULL',
    is_1p BOOLEAN COMMENT 'Исходный признак продавца, unknown не становится FALSE',
    seller_registered_at TIMESTAMP COMMENT 'Регистрация seller в UTC, не возраст master и не первая продажа',
    catalog_version STRING NOT NULL COMMENT 'Версия текущего каталога, которую наследуют связанные SKU/tree',
    source_contract_version STRING NOT NULL COMMENT 'Версия чтения полного справочника и разрешения master-связей',
    source_manifest_id STRING NOT NULL COMMENT 'Manifest захваченного источника, схемы, полноты и проверок конфликтов',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Фактическое время захвата UTC, не историческая доступность'
)
USING iceberg
COMMENT 'Полный текущий seller-master каталог без фильтра SKU, ordinary snapshots и отдельный immutable training package'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
