CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 28-дневного окна в Asia/Tashkent; часть уникального ключа calculated_at, category_id',
    category_id INT COMMENT 'Положительный идентификатор листовой категории из S1 product metadata; часть уникального ключа calculated_at, category_id',
    CATEGORY_GENDER__category_female_product_session_share_28d DOUBLE COMMENT 'Доля female product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    CATEGORY_GENDER__category_male_product_session_share_28d DOUBLE COMMENT 'Доля male product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    CATEGORY_GENDER__n_unique_known_gender_clickers_28d INT COMMENT 'Количество уникальных пользователей листовой категории с gender MALE или FEMALE за 28 дней',
    CATEGORY_GENDER__n_unique_female_clickers_28d INT COMMENT 'Количество уникальных female-пользователей листовой категории за 28 дней',
    CATEGORY_GENDER__n_unique_male_clickers_28d INT COMMENT 'Количество уникальных male-пользователей листовой категории за 28 дней',
    CATEGORY_GENDER__category_gender STRING COMMENT 'Gender листовой категории из S1 product metadata: M, F, U или NULL'
)
USING iceberg
COMMENT 'Gold: gender-статистики product-session просмотров и уникальных пользователей на уровне листовой категории'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
