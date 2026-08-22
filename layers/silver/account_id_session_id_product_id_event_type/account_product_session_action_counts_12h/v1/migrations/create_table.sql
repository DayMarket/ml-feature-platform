CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница полуоткрытого 12-часового среза в Asia/Tashkent; часть уникального ключа calculated_at, account_id, session_id, product_id, event_type',
    account_id INT COMMENT 'Положительный идентификатор пользователя из clickstream event.account_id; часть уникального ключа таблицы',
    session_id STRING COMMENT 'Ненулловый идентификатор clickstream-сессии; часть уникального ключа таблицы',
    product_id INT COMMENT 'Положительный идентификатор товара из clickstream event.product_id; часть уникального ключа таблицы',
    event_type STRING COMMENT 'Тип product-action события: PRODUCT_VIEW, ADD_TO_CART или ADD_TO_FAVORITES; часть уникального ключа таблицы',
    n_events INT COMMENT 'Количество исходных событий данного типа внутри account_id, session_id, product_id за соответствующий 12-часовой срез',
    last_received_at TIMESTAMP COMMENT 'Максимальный event.received_at внутри группы, преобразованный из UTC в локальное время Asia/Tashkent'
)
USING iceberg
COMMENT '12-часовые action counts на уровне account, session, product и event type'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
