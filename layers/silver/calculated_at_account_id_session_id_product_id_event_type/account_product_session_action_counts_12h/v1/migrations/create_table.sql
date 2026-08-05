CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 12-часового среза в UTC',
    account_id BIGINT COMMENT 'Положительный ID пользователя',
    session_id STRING COMMENT 'Идентификатор сессии',
    product_id BIGINT COMMENT 'Положительный ID товара',
    event_type STRING COMMENT 'PRODUCT_VIEW, ADD_TO_CART или ADD_TO_FAVORITES',
    n_events BIGINT COMMENT 'Количество исходных событий внутри группы',
    last_received_at TIMESTAMP COMMENT 'Последнее время события внутри группы в UTC'
)
USING iceberg
COMMENT '12-часовые action counts на уровне account, session, product и event type'
PARTITIONED BY (hours(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
