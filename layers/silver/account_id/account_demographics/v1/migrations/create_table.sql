CREATE TABLE IF NOT EXISTS {target_table} (
    dt DATE COMMENT 'Дата расчёта в часовом поясе Asia/Tashkent; часть уникального ключа dt, account_id',
    account_id INT COMMENT 'Положительный идентификатор пользователя; часть уникального ключа dt, account_id',
    gender STRING COMMENT 'Нормализованный gender пользователя: M, F или NULL; приоритет имеет значение из UM',
    age INTEGER COMMENT 'Возраст пользователя в полных годах на dt, рассчитанный через date_diff от birth_date, либо NULL',
    city_name STRING COMMENT 'Самый частый ближайший город по валидным GEO_INFO событиям за предыдущие 28 полных дней в Asia/Tashkent либо NULL',
    platform STRING COMMENT 'Самая частая платформа по уникальным clickstream-сессиям за предыдущие 28 полных дней: IOS, ANDROID, WEB или NULL'
)
USING iceberg
COMMENT 'Silver: ежедневные демографические и контекстные атрибуты пользователя'
PARTITIONED BY (dt)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
