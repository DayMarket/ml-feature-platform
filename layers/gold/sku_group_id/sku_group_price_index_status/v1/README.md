# Gold Price Index Status по SKU Group ID

DAG id: `feature-platform.layers.gold.sku_group_id.sku_group_price_index_status`.

Пайплайн собирает дневную таблицу `price_index_status` на уровне `sku_group_id`.

Целевая таблица: `iceberg.gold.feature_platform_sku_group_price_index_status`.

Это временное решение: признак создается исключительно для обратной совместимости со старой моделью.

Основная логика:

- проверяет, что существует путь `s3a://um-prod-airflow-fs/price_index_dag/dag_runs/{ds}/price_index_features.parquet`;
- если путь отсутствует, job падает с явной ошибкой;
- читает parquet с price index признаками;
- исключает `price_index_status = 'NO_BOOST'`;
- маппит статусы в числа:
  - `CHEAPEST_AMONG_CLUSTER` -> `0`;
  - `CHEAPEST_AMONG_COMPETITORS` -> `1`;
  - `CHEAPEST_AMONG_COMPETITORS_AND_CLUSTER` -> `2`;
- пишет только три колонки: `date`, `sku_group_id`, `price_index_status`.

Партиция результата соответствует Airflow `ds`.
