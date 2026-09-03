# Silver: `account_id_product_id`

Директория объединяет product-level сущности по `account_id,product_id`. Фактический grain
таблицы ниже остаётся `calculated_at,account_id,session_id,product_id,event_type`.

- [`account_product_session_action_counts_12h`](account_product_session_action_counts_12h/v1/README.md) — 12-часовые product action counts.
