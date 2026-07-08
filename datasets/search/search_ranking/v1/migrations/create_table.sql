CREATE TABLE IF NOT EXISTS {target_table} (
    collection_date DATE COMMENT 'Логическая дата запуска DAG в UTC; дата фактического сбора',
    event_date DATE COMMENT 'Дата поисковых событий; collection_date минус 20 дней',
    logged_at TIMESTAMP COMMENT 'Время логирования PRODUCT_IMPRESSION',
    received_at TIMESTAMP COMMENT 'Время получения PRODUCT_IMPRESSION',
    install_id STRING COMMENT 'Install ID пользователя',
    session_id STRING COMMENT 'Search session ID',
    sku_group_id BIGINT COMMENT 'ID sku group из показа',
    query STRING COMMENT 'Нормализованный поисковый запрос: trim(lower(query))',
    `position` INT COMMENT 'Позиция sku_group_id в поисковой выдаче',
    deduplicate_rank BIGINT COMMENT 'Порядковый номер показа внутри event_date, install_id, session_id, query после дедупликации impression key',
    position_duplicate_count BIGINT COMMENT 'Количество сырых кандидатов для event_date, install_id, session_id, query, position',
    widget_section_name STRING COMMENT 'Секция виджета события показа',
    widget_space_name STRING COMMENT 'Пространство виджета события показа',
    normalized_linear_score DOUBLE COMMENT 'Средний normalized_linear_score из ranking analytics events для query и sku_group_id за event_date',
    linear_score DOUBLE COMMENT 'Средний linear_score из ranking analytics events для query и sku_group_id за event_date',
    dssm_score DOUBLE COMMENT 'Средний dssm_score из ranking analytics events для query и sku_group_id за event_date',
    is_generated_order INT COMMENT 'Метка наличия атрибутированного сгенерированного заказа: 1 или 0'
)
USING iceberg
COMMENT 'Training dataset v1 для search ranking на уровне поискового показа'
PARTITIONED BY (collection_date)
