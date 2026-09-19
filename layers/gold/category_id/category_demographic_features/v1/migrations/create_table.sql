CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 28-дневного окна в Asia/Tashkent; часть уникального ключа calculated_at, category_id',
    category_id INT COMMENT 'Положительный идентификатор листовой категории из S1 product metadata; часть уникального ключа calculated_at, category_id',
    CATEGORY__female_click_share_28d DOUBLE COMMENT 'Взвешенная доля female product-session кликов среди наблюдений с известным gender за полуоткрытое окно 28 дней',
    CATEGORY__male_click_share_28d DOUBLE COMMENT 'Взвешенная доля male product-session кликов среди наблюдений с известным gender за полуоткрытое окно 28 дней',
    CATEGORY__clicker_age_p10_28d DOUBLE COMMENT 'Точный 10-й перцентиль возраста уникальных пользователей категории за 28 дней',
    CATEGORY__clicker_age_p50_28d DOUBLE COMMENT 'Точный 50-й перцентиль, или медианный возраст, уникальных пользователей категории за 28 дней',
    CATEGORY__clicker_age_p90_28d DOUBLE COMMENT 'Точный 90-й перцентиль возраста уникальных пользователей категории за 28 дней',
    CATEGORY__gender STRING COMMENT 'Gender листовой категории из S1 product metadata: M, F, U или NULL'
)
USING iceberg
COMMENT 'Gold: gender- и age-статистики product-session просмотров и уникальных пользователей на уровне листовой категории'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
