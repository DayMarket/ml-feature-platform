# Silver: `product_id`

Группа строится по нетемпоральной части ключа `product_id`; временные колонки ключа
не включаются в имя директории.

- [`product_metadata`](product_metadata/v1/README.md) — ежедневные категории, бренд, магазин, gender-категория и время создания товара;
- [`product_search_queries`](product_search_queries/v1/README.md) - поисковые запросы по product_id из ranking candidates за 14 дней.
- [`product_prices_daily`](product_prices_daily/v1/README.md) — дневные price-факты товара.
