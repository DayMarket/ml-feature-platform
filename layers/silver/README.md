# Silver layer

Переиспользуемые предагрегаты и промежуточные Iceberg-таблицы.

- [`account_id`](account_id/README.md) — пожизненные факты аккаунта;
- [`account_id_category_id`](account_id_category_id/README.md) — события account/category;
- [`category_level_category_id`](category_level_category_id/README.md) — агрегаты по уровням иерархии категорий;
- [`city_id_dimensional_group`](city_id_dimensional_group/README.md) — стоимость логистики по городу и габаритной группе;
- [`h3_index`](h3_index/README.md) — географические и локационные предагрегаты;
- [`order_city_id`](order_city_id/README.md) — агрегаты по городу доставки заказа;
- [`order_region_id`](order_region_id/README.md) — агрегаты по региону доставки заказа;
- [`product_id`](product_id/README.md) — метаданные, поисковые, ценовые и другие данные товара;
- [`query_platform_sku_group_id`](query_platform_sku_group_id/README.md) — дневные query/platform/SKU group conversions;
- [`query_sku_group_id`](query_sku_group_id/README.md) — заказы и другие агрегаты пары query/SKU group;
- [`sku_group_id`](sku_group_id/README.md) — предагрегаты SKU group;
- [`sku_group_id_query_category`](sku_group_id_query_category/README.md) — поисковые и категорийные взаимодействия SKU group;
- [`sku_id_promotion_id`](sku_id_promotion_id/README.md) — дневные динамические скидки SKU/promotion;
- [`sku_id`](sku_id/README.md) — предагрегаты на уровне SKU.
