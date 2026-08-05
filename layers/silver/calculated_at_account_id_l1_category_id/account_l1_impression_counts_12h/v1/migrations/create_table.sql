CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 12-часового среза в UTC',
    account_id BIGINT COMMENT 'Положительный ID пользователя',
    l1_category_id BIGINT COMMENT 'Первый содержательный уровень категории',
    n_impressions BIGINT COMMENT 'Сумма distinct product impressions по сессиям'
)
USING iceberg
COMMENT '12-часовые показы на уровне account_id и L1 category'
PARTITIONED BY (hours(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
