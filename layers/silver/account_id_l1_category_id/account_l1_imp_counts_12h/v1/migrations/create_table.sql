CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница полуоткрытого 12-часового среза в Asia/Tashkent; часть уникального ключа calculated_at, account_id, l1_category_id',
    account_id INT COMMENT 'Положительный идентификатор пользователя из clickstream event.account_id; часть уникального ключа calculated_at, account_id, l1_category_id',
    l1_category_id INT COMMENT 'Идентификатор верхней содержательной категории товара, разрешённой через event.product_id и справочник product; технический корень category_id = 1 исключён',
    n_impressions INT COMMENT 'Сумма COUNT(DISTINCT product_id), сначала рассчитанного отдельно внутри каждой account_id, session_id, l1_category_id за 12-часовой срез'
)
USING iceberg
COMMENT '12-часовые показы на уровне account_id и L1 category'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
