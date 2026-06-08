# Gold Home Recommendations Average Sales 7D по SKU Group ID

Пайплайн собирает дневной признак среднего количества продаж из рекомендаций с главной на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_home_reco_avg_sales_7d`.

Источники:

- `iceberg.silver.order_items_attribution` - атрибуция товарных позиций к виджетам;
- `iceberg.silver.order_items` - статусы, выдача, возвраты и количество товарных позиций;
- `iceberg.silver.sku` - маппинг `sku_id` в `sku_group_id`.

Признак:

- `home_reco_avg_sales_count_7d` - среднее дневное количество завершенных товарных позиций из рекомендаций с главной за окно `[ds - 7, ds - 1]`.

Для `ds = 2026-06-08` окно использует даты `[2026-06-01, 2026-06-07]`, сам `ds` в расчет не входит. Продажа учитывается, если `order_item_status = 'COMPLETED'`, `issued_at` попадает в окно, а возврата до конца окна нет. Для атрибуции и `order_items.generated_at` используется 20-дневный lookback до начала окна, чтобы не терять завершенные продажи по заказам, сгенерированным раньше. Пропущенные дни внутри 7-дневного окна заполняются нулем перед расчетом среднего.

Рекомендации с главной определяются по `widget_space_name` и `widget_section_name`: используются известные значения `HOME_RECOMMENDATIONS`, `HOME_PAGE_RECOMMENDATIONS`, `MAIN_RECOMMENDATIONS`, `MAIN_PAGE_RECOMMENDATIONS`, а также fallback по наличию `RECOMMEND`/`RECO` и `HOME`/`MAIN` в space/section.

Пайплайн пишет партицию `date = ds` в Iceberg через `overwritePartitions()`.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/sku_group_home_reco_avg_sales_7d/v1/entrypoints/get_sku_group_home_reco_avg_sales_7d.py`, поэтому отдельный Docker image для этой сущности не собирается.
