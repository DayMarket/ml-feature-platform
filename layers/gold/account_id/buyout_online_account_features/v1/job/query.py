"""Trino-запрос к партиции витрины признаков истории выкупа аккаунта."""

from __future__ import annotations

from datetime import date

# Порядок и состав колонок повторяют migrations/create_table.sql витрины-источника.
# Новой семантики проекция не добавляет: колонки добавляются только осознанно,
# вместе с миграцией онлайн-таблицы.
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


def build_query(partition_date: date, source_table: str, shards: int, shard: int) -> str:
    """Один срез партиции по остатку account_id; source_table — Trino-имя витрины-источника."""
    if shards < 1:
        raise ValueError(f"shards must be positive, got {shards}")
    if not 0 <= shard < shards:
        raise ValueError(f"shard {shard} is out of range for shards={shards}")

    columns_sql = ",\n    ".join(FEATURE_COLUMNS)
    return f"""
SELECT
    {columns_sql}
FROM {source_table}
WHERE date = DATE '{partition_date.isoformat()}'
  AND account_id % {shards} = {shard}
"""
