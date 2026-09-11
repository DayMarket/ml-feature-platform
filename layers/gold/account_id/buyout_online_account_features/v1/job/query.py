"""Trino-запрос к партиции витрины признаков истории выкупа аккаунта."""

from __future__ import annotations

from datetime import date

# Порядок и состав колонок повторяют migrations/create_table.sql витрины-источника.
# Проекция переносит их без изменений: новая семантика живёт только в SERVING_COLUMNS ниже,
# и колонки добавляются сюда осознанно, вместе с миграцией онлайн-таблицы.
FEATURE_COLUMNS = (
    "date",
    "account_id",
    "n_orders_win",
    "n_delivered_orders_win",
    "n_resolved_orders_win",
    "n_orders_30d",
    "n_orders_90d",
    "n_orders_in_processing",
    "buyout_rate_money_win",
    "buyout_rate_money_90d",
    "buyout_rate_money_30d",
    "buyout_rate_items_win",
    "buyout_rate_orders_win",
    "buyout_rate_last_3",
    "buyout_rate_last_5",
    "buyout_rate_last_10",
    "buyout_rate_first_3",
    "buyout_trend",
    "prev_order_is_nonbuyout",
    "prev2_order_is_nonbuyout",
    "prev3_order_is_nonbuyout",
    "n_bad_last_3",
    "n_bad_last_5",
    "n_bad_last_10",
    "buyout_streak",
    "nonbuyout_streak",
    "n_items_no_show",
    "n_items_cancel_after_delivery",
    "n_items_return_at_handover",
    "n_items_return_post_handover",
    "n_items_fair_return",
    "n_items_cancel_before_delivery",
    "n_orders_cancelled_before_delivery",
    "no_show_share_of_delivered",
    "no_show_share_of_nonbuyout",
    "cancel_before_delivery_share",
    "no_show_gmv_share",
    "n_nonbuyout_events",
    "is_after_first_non_buyout",
    "days_since_first_nonbuyout",
    "days_since_last_nonbuyout",
    "last_nonbuyout_type",
    "last_nonbuyout_cause",
    "postpaid_share_win",
    "installment_share_win",
    "buyout_rate_postpaid",
    "buyout_rate_prepaid",
    "first_order_in_win_is_postpaid",
    "last_order_payment_type",
    "last_order_is_postpaid",
    "avg_ticket_win",
    "median_ticket_win",
    "std_ticket_win",
    "max_ticket_win",
    "avg_items_per_order_win",
    "n_distinct_dp_win",
    "n_distinct_city_win",
    "last_order_city_id",
    "last_order_region_id",
    "first_order_date_win",
    "last_order_date_win",
    "days_since_last_order_win",
    "tenure_days_win",
    "history_left_censored",
    "orders_created_prev_1d",
    "orders_created_prev_7d",
    "orders_created_prev_30d",
    "orders_created_prev_90d",
    "orders_created_prev_365d",
    "first_order_date_ever",
    "first_order_id_ever",
    "first_issued_order_date",
    "first_issued_payment_type",
    "first_issued_paymart_type",
    "registration_date",
    "first_session_date",
    "first_city_id",
    "first_delivery_point_type",
    "acquisition_source_type",
    "acquisition_campaign_type",
    "accounts_per_install_current",
    "tenure_days_true",
    "days_since_registration",
    "history_left_censored_true",
)

# Признаки, которые сервис невыкупов раньше досчитывал сам в запросе на чекауте.
# Upload публикует сырые колонки без выражений и join-ов, поэтому индикаторы и гео-доли
# считаются здесь. Выражения дословно повторяют сборщик обучающего набора: расхождение
# в одном CASE даёт train/serve skew, который в метриках модели не виден.
SERVING_COLUMNS = (
    # 0/1 по строковым колонкам витрины-источника.
    (
        "first_completed_order_is_postpaid",
        """CASE WHEN a.first_issued_payment_type = 'PostPaid' THEN 1
         WHEN COALESCE(a.first_issued_payment_type, '') = '' THEN CAST(NULL AS INTEGER)
         ELSE 0 END""",
    ),
    # В таблице только аккаунты с историей; ноль за отсутствующий аккаунт сервис
    # подставляет сам через missing-feature-value.
    ("has_asof_history", "CAST(1 AS INTEGER)"),
    ("is_first_order_ever", "CASE WHEN a.first_order_id_ever IS NULL THEN 1 ELSE 0 END"),
    (
        "last_nonbuyout_no_show",
        "CASE WHEN a.last_nonbuyout_type = 'no_show' THEN 1 ELSE 0 END",
    ),
    (
        "last_nonbuyout_cancel_after",
        "CASE WHEN a.last_nonbuyout_type = 'cancel_after_delivery' THEN 1 ELSE 0 END",
    ),
    (
        "last_nonbuyout_courier_other",
        "CASE WHEN a.last_nonbuyout_type = 'courier_or_other' THEN 1 ELSE 0 END",
    ),
    (
        "last_pay_uzumcard",
        "CASE WHEN a.last_order_payment_type = 'UzumCard' THEN 1 ELSE 0 END",
    ),
    (
        "last_pay_nasiya",
        "CASE WHEN a.last_order_payment_type = 'Nasiya' THEN 1 ELSE 0 END",
    ),
    (
        "last_pay_uzumcheckout",
        "CASE WHEN a.last_order_payment_type = 'UzumCheckout' THEN 1 ELSE 0 END",
    ),
    (
        "last_pay_bonus",
        "CASE WHEN a.last_order_payment_type = 'BONUS' THEN 1 ELSE 0 END",
    ),
    (
        "first_dp_pickup_point",
        "CASE WHEN a.first_delivery_point_type = 'DELIVERY_POINT' THEN 1 ELSE 0 END",
    ),
    (
        "first_dp_franchise",
        "CASE WHEN a.first_delivery_point_type = 'FRANCHISE' THEN 1 ELSE 0 END",
    ),
    (
        "first_dp_uzpost",
        "CASE WHEN a.first_delivery_point_type = 'UZ_POST' THEN 1 ELSE 0 END",
    ),
    # Сравнение с пустой строкой, а не COALESCE(..., '') = '': так считает сборщик
    # обучающего набора, и NULL там даёт нулевой one-hot по всем четырём типам.
    (
        "first_dp_missing",
        "CASE WHEN a.first_delivery_point_type = '' THEN 1 ELSE 0 END",
    ),
    # Гео последнего заказа: доли выкупа города и региона за ту же партицию date.
    ("prev_city_part_completed", "c.part_completed_orders"),
    ("prev_city_part_no_show", "c.part_no_show_from_total"),
    ("prev_region_part_completed", "r.part_completed_orders"),
    ("prev_region_part_no_show", "r.part_no_show_from_total"),
    ("has_prev_city", "CASE WHEN c.order_city_id IS NULL THEN 0 ELSE 1 END"),
)


def build_query(
    partition_date: date,
    source_table: str,
    city_table: str,
    region_table: str,
    shards: int,
    shard: int,
) -> str:
    """Один срез партиции по остатку account_id.

    `source_table`, `city_table`, `region_table` — Trino-имена витрины-источника и двух
    silver-витрин долей выкупа. Гео берётся за ту же партицию `date`, что и снимок аккаунта:
    обе витрины помечают партицию `analyze_date` того же дня, поэтому будущее в признак
    не подмешивается.
    """
    if shards < 1:
        raise ValueError(f"shards must be positive, got {shards}")
    if not 0 <= shard < shards:
        raise ValueError(f"shard {shard} is out of range for shards={shards}")

    partition_sql = f"DATE '{partition_date.isoformat()}'"
    source_columns_sql = ",\n        ".join(FEATURE_COLUMNS)
    projected_sql = ",\n    ".join(f"a.{column}" for column in FEATURE_COLUMNS)
    serving_sql = ",\n    ".join(
        f"{expression} AS {column}" for column, expression in SERVING_COLUMNS
    )
    return f"""
SELECT
    {projected_sql},
    {serving_sql}
FROM (
    SELECT
        {source_columns_sql}
    FROM {source_table}
    WHERE date = {partition_sql}
      AND account_id % {shards} = {shard}
) a
LEFT JOIN (
    SELECT order_city_id, part_completed_orders, part_no_show_from_total
    FROM {city_table}
    WHERE date = {partition_sql}
) c ON c.order_city_id = a.last_order_city_id
LEFT JOIN (
    SELECT order_region_id, part_completed_orders, part_no_show_from_total
    FROM {region_table}
    WHERE date = {partition_sql}
) r ON r.order_region_id = a.last_order_region_id
"""
