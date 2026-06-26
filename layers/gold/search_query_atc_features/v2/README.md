# Gold-фичи показов, ATC и заказов по Query

Пайплайн строит дневные query-only признаки количества поисковых показов, добавлений в корзину и заказов.

Целевая таблица: `iceberg.gold.feature_platform_search_query_atc_features_v2`.

Grain: одна строка на `date` и нормализованный `query`. Primary key: `date, query`.

Основная логика:

- читает поисковые события из `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- читает generated orders из `iceberg.silver.feature_platform_sku_group_query_search_orders`;
- использует только строки `space = 'SEARCH_RESULTS'`;
- нормализует исходный `query` из поля `uniqs` для поисковых событий и `query` для заказов: `lower`, замена `ё` на `е`, схлопывание пробелов, `trim` и фильтр пустых строк;
- не преобразует запрос в `base_query`: не удаляет stopwords, не удаляет повторяющиеся токены и не меняет порядок слов;
- агрегирует `sum_impressions` и `sum_atc` по `query` без разреза `sku_group_id`;
- агрегирует `orders_generated` по `query` без разреза `sku_group_id`;
- считает окна 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- окна считаются как `[ds - n, ds - 1]`, то есть дата расчёта не включается;
- `query_orders_{window}` заполняется `0.0`, если для query из поисковых событий нет заказов в соответствующем окне;
- пишет результат в Iceberg через `overwritePartitions()`.

Выходные признаки:

- `query_uniq_impressions_{1,3,7,14,21,30,60,90}`;
- `query_uniq_atcs_{1,3,7,14,21,30,60,90}`;
- `query_orders_{1,3,7,14,21,30,60,90}`.

Название `uniq` сохранено в стиле существующих search-фичей. На этом слое значения считаются как сумма `sum_impressions` и `sum_atc` из silver-источника после агрегации по query.

DAG ждет DQ DAG silver-источников:

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`.
- `dbt.source.trino.ml_feature_platform_silver.feature_platform_sku_group_query_search_orders.dq`.

Версия `v2` создается как новая Iceberg-таблица. Вся схема, включая `query_orders_*`, описана в `migrations/create_table.sql`; отдельных schema-change миграций для добавления фичей в этой версии нет.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/search_query_atc_features/v2/entrypoints/get_search_query_atc_features.py`, поэтому отдельный Docker image для этой сущности не собирается.

Ranking upload для этой таблицы не настроен.
