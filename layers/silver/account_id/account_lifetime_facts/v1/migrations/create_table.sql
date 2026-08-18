CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата снапшота: состояние marketing.account_properties на эту дату',
    account_id BIGINT COMMENT 'ID аккаунта покупателя (marketing.account_properties.account_id), только account_id > 0',
    first_order_date_ever DATE COMMENT 'Дата первого созданного заказа аккаунта (fo_date_created_uz, Asia/Tashkent)',
    first_order_id_ever BIGINT COMMENT 'ID первого созданного заказа аккаунта (fo_id_created)',
    first_issued_order_date DATE COMMENT 'Дата первого ВЫКУПЛЕННОГО заказа (fo_date_issued_uz); при сборке обучающего набора зануляется, если позже даты решения',
    first_issued_payment_type STRING COMMENT 'Тип оплаты первого выкупленного заказа (fo_issued_payment_type)',
    first_issued_paymart_type STRING COMMENT 'Тип рассрочки первого выкупленного заказа (fo_issued_paymart_type)',
    registration_date DATE COMMENT 'Дата регистрации аккаунта (registration_date_uz, Asia/Tashkent)',
    first_session_date DATE COMMENT 'Дата первой сессии аккаунта (first_session_started_date_uz)',
    first_city_id STRING COMMENT 'Город первого созданного заказа (first_city_id_created); в источнике строка, не числовой ID',
    first_delivery_point_type STRING COMMENT 'Тип точки выдачи первого созданного заказа (first_delivery_point_type_created)',
    acquisition_source_type STRING COMMENT 'Тип источника привлечения аккаунта (source_type)',
    acquisition_campaign_type STRING COMMENT 'Тип кампании привлечения аккаунта (campaign_type)',
    accounts_per_install_current INT COMMENT 'Сколько аккаунтов приходится на установку сейчас (cnt_accounts_per_installs); истории нет, значение текущее и приблизительное'
)
USING iceberg
COMMENT 'Дневной снимок фактов за всю жизнь аккаунта из marketing.account_properties: первый созданный и первый выкупленный заказ, регистрация, первая сессия, канал привлечения. Нужен модели невыкупов, потому что основная таблица признаков видит только 182 дня, а стаж и признак новичка считаются от первого заказа вообще. Строка за date отражает состояние источника на эту дату; собственной истории у источника нет. Поля первого выкупленного заказа могут оказаться позже исторической даты решения — это отсекается при сборке обучающего набора. Ключ date + account_id'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
