CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE NOT NULL COMMENT 'dt источника; бизнес-день Asia/Tashkent, не дата загрузки',
    calendar_id STRING NOT NULL COMMENT 'Идентификатор официального календаря Узбекистана: uz_official, назначен контрактом источника silver.calendar',
    year INT COMMENT 'Год из исходного календаря',
    quarter INT COMMENT 'Номер квартала из источника, 1–4',
    month INT COMMENT 'Номер месяца из источника, 1–12',
    month_name_en STRING COMMENT 'Полное английское название месяца из источника',
    month_abbr_en STRING COMMENT 'Сокращённое английское название месяца из источника',
    day INT COMMENT 'День месяца из источника, 1–31',
    day_of_week_iso INT COMMENT 'ISO день недели из источника: понедельник 1, воскресенье 7',
    day_name_en STRING COMMENT 'Полное английское название дня недели из источника',
    day_abbr_en STRING COMMENT 'Сокращённое английское название дня недели из источника',
    iso_week INT COMMENT 'ISO номер недели из источника, 1–53',
    is_weekend BOOLEAN COMMENT 'Календарный выходной из источника',
    is_public_holiday BOOLEAN COMMENT 'Официальный праздник из источника',
    holiday_name STRING COMMENT 'Название праздника из источника; пустая строка сохраняется',
    is_working_day BOOLEAN COMMENT 'Рабочий день с официальными переносами из источника',
    source_manifest_id STRING NOT NULL COMMENT 'Manifest захвата и покрытия; будущая дата не доказывает доступность события в прошлом',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Время захвата источника UTC; не дата события'
)
USING iceberg
COMMENT 'Копия внешнего календаря для demand forecast без ручного seed и достраивания дат'
PARTITIONED BY (months(date))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false');
