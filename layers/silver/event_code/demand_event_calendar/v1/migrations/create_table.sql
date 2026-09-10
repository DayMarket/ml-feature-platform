CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'День события Asia/Tashkent в пределах подтверждённого покрытия календаря',
    event_code STRING NOT NULL COMMENT 'Стабильный ключ с namespace источника; не slug изменяемого названия',
    source_kind STRING NOT NULL COMMENT 'Источник записи: calendar или marketing_sale',
    calendar_id STRING COMMENT 'uz_official для source_kind calendar, NULL для marketing_sale без выдуманного назначения календаря',
    source_event_id STRING NOT NULL COMMENT 'Исходный id акции либо дата строки календаря ISO; не идентификатор модели',
    event_name STRING COMMENT 'Исходное название праздника или акции без скрытой нормализации',
    source_status STRING COMMENT 'Исходный статус акции, включая CREATED и CANCELED; NULL для календаря',
    source_type STRING COMMENT 'Исходный тип акции; NULL для календаря, не придуманная классификация',
    source_started_at TIMESTAMP COMMENT 'Исходное начало акции UTC; NULL для календарного праздника',
    source_finished_at TIMESTAMP COMMENT 'Исходное окончание акции UTC; NULL для календарного праздника',
    source_announced_at TIMESTAMP COMMENT 'Исходное announced_at UTC; NULL не заменяется created_at',
    source_created_at TIMESTAMP COMMENT 'Исходное created_at UTC; не доказательство объявления акции',
    source_updated_at TIMESTAMP COMMENT 'Исходное updated_at UTC; не полная история изменения статуса',
    source_manifest_id STRING NOT NULL COMMENT 'Manifest конкретных версий календаря и реестра, их покрытия и правил нормализации',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время захвата источников UTC; не доступность события на историческую отсечку'
)
USING iceberg
COMMENT 'Календарь праздников и реестра акций для demand forecast с исходными статусами и provenance'
PARTITIONED BY (months(date))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false');
