CREATE TABLE IF NOT EXISTS {target_table} (
    date DATE COMMENT 'Дата партиционирования, соответствует ds расчета (Asia/Tashkent)',
    sku_group_id BIGINT COMMENT 'ID sku group',
    ad_impressions BIGINT COMMENT 'Количество рекламных показов sku group за день (sum impressions из adv_funnel_daily)',
    ad_clicks BIGINT COMMENT 'Количество кликов по рекламе sku group за день (sum clicks из adv_funnel_daily)',
    ad_revenue DOUBLE COMMENT 'Рекламные расходы продавца = заработок платформы с рекламы sku group за день (sum adrev из adv_funnel_daily)'
)
USING iceberg
COMMENT 'Дневной silver pre-aggregate рекламной выручки на уровне sku_group_id из adv_funnel_daily (весь рекламный CPC-funnel, без разреза по площадке)'
PARTITIONED BY (date)
