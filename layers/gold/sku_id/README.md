# Gold: `sku_id`

Ключ группы: `sku_id`; `date` остаётся в PK дневных снимков.

- [`demand_observed_daily`](demand_observed_daily/v1/README.md) — дневной full outer join точных sales/stock snapshots; owner DAG и writer подготовлены.

- [`buyout_online_sku_features`](buyout_online_sku_features/v1/README.md) — online-таблица SKU для сервиса невыкупов: выкупаемость sku, его карточки, категории, магазина и бренда и сглаженные оценки; все активные sku в наличии.
