CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиции, совпадает с датой партиции silver-витрины CPI',
    city_id BIGINT COMMENT 'ID города доставки из silver.order_items.city_id',
    dimensional_group STRING COMMENT 'Габаритная группа SKU: SMALL / MEDIUM / LARGE; пустое и неизвестное значение отнесено к SMALL',
    region_id BIGINT COMMENT 'ID региона города — уровень фолбэка для тонких городов',
    cpi_forward_uzs DOUBLE COMMENT 'Эмпирический CPI прямого потока города: затраты прямой логистики (UZS) на штуку доставленного товара',
    cpi_reverse_uzs DOUBLE COMMENT 'Эмпирический CPI обратного потока города: затраты обратной логистики (UZS) на штуку возвращённого товара',
    n_items_delivered BIGINT COMMENT 'Штук доставлено за окно 90 дней (знаменатель cpi_forward_uzs) — мера надёжности городской оценки',
    n_items_reverse BIGINT COMMENT 'Штук в обратном потоке за окно 90 дней (знаменатель cpi_reverse_uzs) — мера надёжности городской оценки',
    cpi_forward_region_uzs DOUBLE COMMENT 'CPI прямого потока региона по той же габаритной группе — фолбэк при малом n_items_delivered',
    cpi_reverse_region_uzs DOUBLE COMMENT 'CPI обратного потока региона по той же габаритной группе — фолбэк при малом n_items_reverse',
    cpi_forward_country_uzs DOUBLE COMMENT 'CPI прямого потока по стране и габаритной группе — последний уровень фолбэка',
    cpi_reverse_country_uzs DOUBLE COMMENT 'CPI обратного потока по стране и габаритной группе — последний уровень фолбэка'
)
USING iceberg
COMMENT 'Gold: online-проекция CPI логистики по городу и габаритной группе для сервиса невыкупов'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
