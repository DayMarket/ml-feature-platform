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
    cpo_adv_version BIGINT COMMENT 'Версия CPO рекламной кампании из event_parameters.cpo_adv_version; NULL, если поле не логировалось событием',
    bid_id BIGINT COMMENT 'Идентификатор рекламной ставки показа из плоской колонки events.bid_id; 0 - ставки не было, NULL - поле не логировалось событием',
    seller_price BIGINT COMMENT 'Цена продавца из event_parameters.seller_price; не равна events.sell_price и берется только из JSON',
    final_price BIGINT COMMENT 'Итоговая цена показа: COALESCE(event_parameters.final_price, seller_price, full_price)',
    normalized_linear_score DOUBLE COMMENT 'Средний normalized_linear_score из ranking analytics events для query и sku_group_id за event_date',
    linear_score DOUBLE COMMENT 'Средний linear_score из ranking analytics events для query и sku_group_id за event_date',
    dssm_score DOUBLE COMMENT 'Средний dssm_score из ranking analytics events для query и sku_group_id за event_date',
    is_generated_order INT COMMENT 'Метка наличия атрибутированного сгенерированного заказа: 1 или 0'
)
USING iceberg
COMMENT 'Training dataset v1 для search ranking на уровне поискового показа'
PARTITIONED BY (collection_date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
