# Gold-фичи показов и ATC по Query

Пайплайн строит дневные query-only признаки количества поисковых показов и добавлений в корзину.

Целевая таблица: `iceberg.gold.feature_platform_search_query_atc_features`.

Grain: одна строка на `date` и нормализованный `query`. Primary key: `date, query`.

Основная логика:

- читает поисковые события из `iceberg.silver.feature_platform_search_sku_group_id_install_query`;
- использует только строки `space = 'SEARCH_RESULTS'`;
- нормализует исходный `query` из поля `uniqs`: `lower`, замена `ё` на `е`, схлопывание пробелов, `trim` и фильтр пустых строк;
- не преобразует запрос в `base_query`: не удаляет stopwords, не удаляет повторяющиеся токены и не меняет порядок слов;
- агрегирует `sum_impressions` и `sum_atc` по `query` без разреза `sku_group_id`;
- считает окна 1, 3, 7, 14, 21, 30, 60 и 90 дней;
- окна считаются как `[ds - n, ds - 1]`, то есть дата расчёта не включается;
- пишет результат в Iceberg через `overwritePartitions()`.

Выходные признаки:

- `query_uniq_impressions_{1,3,7,14,21,30,60,90}`;
- `query_uniq_atcs_{1,3,7,14,21,30,60,90}`.

Название `uniq` сохранено в стиле существующих search-фичей. На этом слое значения считаются как сумма `sum_impressions` и `sum_atc` из silver-источника после агрегации по query.

DAG ждет DQ DAG silver-источника:

- `dbt.source.trino.ml_feature_platform_silver.feature_platform_search_sku_group_id_install_query.dq`.

Пайплайн использует общий способ доставки Spark job: дефолтный Spark image и `git-sync` initContainer. Код запускается из `/git/repo/layers/gold/search_query_atc_features/v1/entrypoints/get_search_query_atc_features.py`, поэтому отдельный Docker image для этой сущности не собирается.

Ranking upload для этой таблицы не настроен.
