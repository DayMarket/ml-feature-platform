CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 28-дневного окна в Asia/Tashkent; часть уникального ключа calculated_at, l6_category_id',
    l6_category_id INT COMMENT 'Положительный идентификатор категории L6 из Silver product metadata; часть уникального ключа calculated_at, l6_category_id',
    category_female_product_session_share_28d DOUBLE COMMENT 'Доля female product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    category_male_product_session_share_28d DOUBLE COMMENT 'Доля male product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    n_unique_known_gender_clickers_28d INT COMMENT 'Количество уникальных пользователей L6-категории с gender MALE или FEMALE за 28 дней',
    n_unique_female_clickers_28d INT COMMENT 'Количество уникальных female-пользователей L6-категории за 28 дней',
    n_unique_male_clickers_28d INT COMMENT 'Количество уникальных male-пользователей L6-категории за 28 дней',
    category_gender STRING COMMENT 'Gender L6-категории из iceberg.silver.recsys_category_genders: M, F, U или NULL'
)
USING iceberg
COMMENT 'Gold: gender-статистики product-session просмотров и уникальных пользователей на уровне L6'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
