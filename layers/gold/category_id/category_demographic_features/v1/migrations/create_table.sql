CREATE TABLE IF NOT EXISTS {target_table} (
    calculated_at TIMESTAMP COMMENT 'Правая граница 28-дневного окна в Asia/Tashkent; часть уникального ключа calculated_at, category_id',
    category_id INT COMMENT 'Положительный идентификатор листовой категории из S1 product metadata; часть уникального ключа calculated_at, category_id',
    CATEGORY_DEMOGRAPHICS__female_product_session_share_28d DOUBLE COMMENT 'Доля female product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    CATEGORY_DEMOGRAPHICS__male_product_session_share_28d DOUBLE COMMENT 'Доля male product-session просмотров среди product-session просмотров пользователей с известным gender за полуоткрытое окно 28 дней',
    CATEGORY_DEMOGRAPHICS__female_unique_clicker_share_28d DOUBLE COMMENT 'Доля female-пользователей среди уникальных пользователей категории с известным gender за 28 дней',
    CATEGORY_DEMOGRAPHICS__male_unique_clicker_share_28d DOUBLE COMMENT 'Доля male-пользователей среди уникальных пользователей категории с известным gender за 28 дней',
    CATEGORY_DEMOGRAPHICS__gender_balance_28d DOUBLE COMMENT 'Сбалансированность product-session gender-аудитории: 1 означает равные female/male доли, 0 означает аудиторию одного gender',
    CATEGORY_DEMOGRAPHICS__n_unique_clickers_28d INT COMMENT 'Количество уникальных пользователей листовой категории за 28 дней независимо от наличия demographic-атрибутов',
    CATEGORY_DEMOGRAPHICS__n_unique_known_gender_clickers_28d INT COMMENT 'Количество уникальных пользователей листовой категории с gender MALE или FEMALE за 28 дней',
    CATEGORY_DEMOGRAPHICS__n_unique_female_clickers_28d INT COMMENT 'Количество уникальных female-пользователей листовой категории за 28 дней',
    CATEGORY_DEMOGRAPHICS__n_unique_male_clickers_28d INT COMMENT 'Количество уникальных male-пользователей листовой категории за 28 дней',
    CATEGORY_DEMOGRAPHICS__n_unique_clickers_with_age_28d INT COMMENT 'Количество уникальных пользователей листовой категории с возрастом от 13 до 100 лет за 28 дней',
    CATEGORY_DEMOGRAPHICS__known_age_clicker_share_28d DOUBLE COMMENT 'Доля уникальных пользователей категории с возрастом от 13 до 100 лет',
    CATEGORY_DEMOGRAPHICS__clicker_age_p10_28d DOUBLE COMMENT 'Точный 10-й перцентиль возраста уникальных пользователей категории за 28 дней',
    CATEGORY_DEMOGRAPHICS__clicker_age_p50_28d DOUBLE COMMENT 'Точный 50-й перцентиль, или медианный возраст, уникальных пользователей категории за 28 дней',
    CATEGORY_DEMOGRAPHICS__clicker_age_p90_28d DOUBLE COMMENT 'Точный 90-й перцентиль возраста уникальных пользователей категории за 28 дней',
    CATEGORY_DEMOGRAPHICS__gender STRING COMMENT 'Gender листовой категории из S1 product metadata: M, F, U или NULL'
)
USING iceberg
COMMENT 'Gold: gender- и age-статистики product-session просмотров и уникальных пользователей на уровне листовой категории'
PARTITIONED BY (days(calculated_at))
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
