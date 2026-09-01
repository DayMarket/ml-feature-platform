# Gold layer

Финальные feature tables, готовые для модельного потребления или публикации.

- [`h3_index`](h3_index/README.md) — географические признаки по H3-гексагону;
- [`product_id`](product_id/README.md) — признаки товара на уровне `product_id`;
- [`query`](query/README.md) — признаки на уровне нормализованного query;
- [`query_sku_group_id`](query_sku_group_id/README.md) — pairwise-признаки запроса и SKU group;
- [`query_text_version`](query_text_version/README.md) — справочник каноничных `query_id` по тексту запроса;
- [`calculated_at_sku_id_promotion_id`](calculated_at_sku_id_promotion_id/README.md) — timestamp snapshots SKU/promotion;
- [`calculated_at_sku_group_id_promotion_id`](calculated_at_sku_group_id_promotion_id/README.md) — timestamp snapshots SKU group/promotion;
- [`sku_group_id`](sku_group_id/README.md) — признаки на уровне SKU group;
- [`sku_group_id_query_text`](sku_group_id_query_text/README.md) — признаки SKU group и нормализованного текста запроса;
- [`key_type_key_id`](key_type_key_id/README.md) — товарный сигнал выкупаемости в длинном формате «уровень × ID»;
- [`sku_id`](sku_id/README.md) — online-признаки SKU для сервиса невыкупов;
- [`city_id_dimensional_group`](city_id_dimensional_group/README.md) — online-проекция стоимости логистики по городу и габаритной группе;
- [`account_id`](account_id/README.md) — признаки истории выкупа аккаунта для модели невыкупов.
