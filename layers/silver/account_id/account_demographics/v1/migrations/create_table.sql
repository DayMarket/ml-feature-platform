CREATE TABLE IF NOT EXISTS {target_table} (
    dt DATE COMMENT 'Дата расчёта в часовом поясе Asia/Tashkent; часть уникального ключа dt, account_id',
    account_id INT COMMENT 'Положительный идентификатор пользователя; часть уникального ключа dt, account_id',
    gender STRING COMMENT 'Нормализованный gender пользователя: MALE, FEMALE или NULL; приоритет имеет значение из UM',
    age INTEGER COMMENT 'Возраст пользователя в полных годах на dt с учётом месяца и дня рождения либо NULL',
    city_name STRING COMMENT 'Самый частый ближайший город по валидным GEO_INFO событиям за настроенное ретроспективное окно в Asia/Tashkent либо NULL',
    platform STRING COMMENT 'Самая частая платформа по уникальным сессиям за настроенное ретроспективное окно: IOS, ANDROID, WEB или NULL'
)
USING iceberg
COMMENT 'Silver: ежедневные демографические и контекстные атрибуты пользователя'
PARTITIONED BY (dt)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
