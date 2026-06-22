# Gold Price Features по SKU Group ID

Пайплайн собирает дневные ценовые признаки на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_price_features`.

Источник: `iceberg.silver.feature_platform_sku_group_id_prices`.

Основная логика:

- за дату расчета `ds` берет агрегаты цен из silver-таблицы;
- джойнится с `iceberg.silver.sku`, чтобы получить `category_id`;
- считает среднюю цену продажи внутри категории;
- записывает `sell_price_eod` как `log1p(avg_sell_price_eod)`;
- считает абсолютную скидку `median_full_price_eod - median_sell_price_eod`;
- считает долю цены продажи от полной цены `median_sell_price_eod / median_full_price_eod`;
- для вчерашнего дня относительно `ds` считает отношение текущей минимальной полной цены к средней минимальной полной цене за предыдущие 14 и 30 дней.

Партиция результата соответствует Airflow `ds`.

Перед расчетом job создает Iceberg-таблицу из `migrations/create_table.sql`, если она еще не существует.
