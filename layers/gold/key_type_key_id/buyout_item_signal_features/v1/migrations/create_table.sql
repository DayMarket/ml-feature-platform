CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'analyze_date снапшота history_order_items, за который собран сигнал',
    key_type STRING COMMENT 'Уровень агрегации: sku | product | category | shop | brand',
    key_id BIGINT COMMENT 'ID сущности выбранного уровня: sku_id, product_id, category_id, shop_id или brand_name_id',
    n_rows_90d BIGINT COMMENT 'Позиций заказов за 90 дней (без ACTIVE) — знаменатель ставок отмен',
    n_delivered_90d BIGINT COMMENT 'Позиций доставлено за 90 дней: delivered_at не пуст или COURIER с issued_at',
    n_completed_90d BIGINT COMMENT 'Позиций выкуплено за 90 дней (COMPLETED среди доставленных)',
    n_no_show_90d BIGINT COMMENT 'Позиций со статусом RETURNED NO SHOW за 90 дней',
    n_nonbuyout_client_90d BIGINT COMMENT 'Клиентский невыкуп за 90 дней: RETURNED NO SHOW или RETURNED с причиной CANCELED',
    n_return_after_completed_90d BIGINT COMMENT 'Возвратов после выкупа за 90 дней (RETURNED AFTER COMPLETED)',
    n_fair_return_90d BIGINT COMMENT 'Возвратов по вине маркетплейса или контента за 90 дней (MISSING, DEFECTED, BAD_QUALITY, WRONG_ITEM, PHOTO_MISMATCH, CONTENT)',
    n_cancel_before_delivery_90d BIGINT COMMENT 'Отмен до доставки за 90 дней (RETURNED BEFORE DELIVERY)',
    gmv_delivered_90d DOUBLE COMMENT 'GMV generated доставленных позиций за 90 дней (UZS)',
    gmv_completed_90d DOUBLE COMMENT 'GMV generated выкупленных позиций за 90 дней (UZS)',
    gmv_fair_return_90d DOUBLE COMMENT 'GMV generated возвратов по вине маркетплейса за 90 дней (UZS) — вычитается из знаменателя денежной выкупаемости',
    n_rows_30d BIGINT COMMENT 'Позиций заказов за 30 дней (без ACTIVE)',
    n_delivered_30d BIGINT COMMENT 'Позиций доставлено за 30 дней',
    n_completed_30d BIGINT COMMENT 'Позиций выкуплено за 30 дней',
    n_no_show_30d BIGINT COMMENT 'Позиций со статусом RETURNED NO SHOW за 30 дней',
    n_nonbuyout_client_30d BIGINT COMMENT 'Клиентский невыкуп за 30 дней',
    n_cancel_before_delivery_30d BIGINT COMMENT 'Отмен до доставки за 30 дней',
    buyout_rate_items_90d DOUBLE COMMENT 'Сырая выкупаемость в штуках за 90 дней: n_completed_90d / n_delivered_90d; NULL при нулевом знаменателе',
    no_show_rate_90d DOUBLE COMMENT 'Сырая доля невыкупа NO SHOW за 90 дней: n_no_show_90d / n_delivered_90d',
    nonbuyout_rate_90d DOUBLE COMMENT 'Сырая доля клиентского невыкупа за 90 дней: n_nonbuyout_client_90d / n_delivered_90d',
    buyout_rate_money_90d DOUBLE COMMENT 'Денежная выкупаемость за 90 дней: gmv_completed_90d / (gmv_delivered_90d - gmv_fair_return_90d)',
    cancel_before_delivery_rate_90d DOUBLE COMMENT 'Доля отмен до доставки за 90 дней: n_cancel_before_delivery_90d / n_rows_90d',
    buyout_rate_items_30d DOUBLE COMMENT 'Сырая выкупаемость в штуках за 30 дней',
    no_show_rate_30d DOUBLE COMMENT 'Сырая доля невыкупа NO SHOW за 30 дней',
    nonbuyout_rate_30d DOUBLE COMMENT 'Сырая доля клиентского невыкупа за 30 дней',
    cancel_before_delivery_rate_30d DOUBLE COMMENT 'Доля отмен до доставки за 30 дней'
)
USING iceberg
COMMENT 'Gold: товарный сигнал выкупаемости в длинном формате (sku / product / category / shop / brand) в окнах 30 и 90 дней'
PARTITIONED BY (date)
TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')
