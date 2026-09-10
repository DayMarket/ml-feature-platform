CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Дата захвата исходного catalog_sku, не историческая отсечка',
    level STRING NOT NULL COMMENT 'market, l1, l2, l3, l4, l5 или leaf',
    node_id STRING NOT NULL COMMENT 'market либо <level>:<category id>',
    level_code INT NOT NULL COMMENT 'market=0, l1=1, l2=2, l3=3, l4=4, l5=5, leaf=6',
    parent_id STRING COMMENT 'Единственный родитель предыдущего уровня, NULL только у market',
    is_passthrough BOOLEAN NOT NULL COMMENT 'Узел повторяет категорию непосредственного родителя для выравнивания глубины',
    catalog_version STRING NOT NULL COMMENT 'Общий catalog_version исходного SKU-каталога',
    catalog_sku_snapshot_id BIGINT NOT NULL COMMENT 'Exact проверенный SKU snapshot, UUID/schema в manifest',
    source_contract_version STRING NOT NULL COMMENT 'Версия построения дерева из сохранённых нормализованных путей',
    source_manifest_id STRING NOT NULL COMMENT 'Manifest source snapshot, проверок родителей и покрытия',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время материализации дерева UTC, не время захвата внешних справочников'
)
USING iceberg
COMMENT 'Полное дерево market-L1-L6 из одного SKU snapshot, без повторного чтения DWH'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
