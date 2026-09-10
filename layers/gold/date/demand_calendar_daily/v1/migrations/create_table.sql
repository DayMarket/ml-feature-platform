CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'Бизнес-день официального календаря Asia/Tashkent',
    calendar_id STRING NOT NULL COMMENT 'Атрибут источника uz_official, не часть ключа',
    year INT COMMENT 'Год из официального календаря',
    quarter INT COMMENT 'Квартал из официального календаря',
    month INT COMMENT 'Месяц из официального календаря',
    month_name_en STRING COMMENT 'Название месяца из источника',
    month_abbr_en STRING COMMENT 'Сокращение месяца из источника',
    day INT COMMENT 'День месяца из источника',
    day_of_week_iso INT COMMENT 'ISO день недели из источника',
    day_name_en STRING COMMENT 'Название дня недели из источника',
    day_abbr_en STRING COMMENT 'Сокращение дня недели из источника',
    iso_week INT COMMENT 'ISO неделя из источника',
    is_weekend BOOLEAN COMMENT 'Выходной из источника, NULL сохраняется',
    is_public_holiday BOOLEAN COMMENT 'Официальный праздник из источника, NULL сохраняется',
    holiday_name STRING COMMENT 'Название праздника без нормализации пустых строк',
    is_working_day BOOLEAN COMMENT 'Рабочий день из источника с официальными переносами',
    big_sale_created BOOLEAN COMMENT 'Есть BIG_SALE со статусом CREATED, не доказательство проведения',
    big_sale_canceled BOOLEAN COMMENT 'Есть BIG_SALE со статусом CANCELED',
    big_sale_unknown_status BOOLEAN COMMENT 'Есть BIG_SALE со статусом NULL или вне CREATED/CANCELED',
    big_sale_event_count BIGINT NOT NULL COMMENT 'Число различных BIG_SALE в текущем реестре на дату, не фактически проведённых акций',
    calendar_coverage_status STRING NOT NULL COMMENT 'source_row_present: дата есть в точном silver-календаре, не гарантия заполненности всех полей',
    promotion_coverage_status STRING NOT NULL COMMENT 'registry_rows_present либо no_registry_rows, не доказательство исторической полноты',
    calendar_snapshot_id BIGINT NOT NULL COMMENT 'Точная проверенная версия входного silver-календаря',
    events_snapshot_id BIGINT NOT NULL COMMENT 'Точная проверенная версия входного silver-реестра',
    calendar_source_manifest_id STRING NOT NULL COMMENT 'Идентификатор захвата входного календаря',
    events_source_manifest_id STRING NOT NULL COMMENT 'Идентификатор захвата входного реестра',
    source_manifest_id STRING NOT NULL COMMENT 'Run id сборки gold, не историческая доступность событий',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время сборки gold UTC для freshness и DQ'
)
USING iceberg
COMMENT 'Дневной официальный календарь и BIG_SALE без модельных окон и будущих сценариев'
PARTITIONED BY (months(date))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false');
