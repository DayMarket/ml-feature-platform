# iceberg.gold.feature_platform_location_h3_forecast_features

Итоговая h3-витрина признаков для модели `location_forecast` — стабильный контракт фич (имена совпадают
с `features_to_use` модели, без per-prediction временных фич, которые модель добавляет на инференсе).

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_location_h3_forecast_features`.
- DAG: `feature-platform.layers.gold.location_h3_forecast_features` (`layers/gold/location_h3_forecast_features/v1/dag.py`).
- Групповой тег Airflow: `location-h3-forecast`.
- Расписание: ежедневно в 02:00 UTC, `start_date=2026-06-19T00:00:00Z`.

## Грейн / ключ
`date, h3_index`.

## Источник
Джойн пяти silver-таблиц на `(date, h3_index)`:
`iceberg.silver.feature_platform_geo_geointellect_features` (база),
`iceberg.silver.feature_platform_dp_neighbor_order_features`,
`iceberg.silver.feature_platform_geo_user_activity_features`,
`iceberg.silver.feature_platform_geo_user_location_features` и
`iceberg.silver.feature_platform_geo_yandex_poi_features`.

Gold DAG ждёт отдельный dbt DQ DAG каждой из пяти silver-таблиц. DQ DAG стартуют в
01:00 UTC, поэтому gold в 02:00 UTC ищет их logical date с `execution_delta=1 hour`. Только после
успеха всех пяти сенсоров он проверяет входные и выходную таблицы через PyIceberg и собирает gold.

## Логика
Переименования к контракту модели (`population_r0 -> population`, `views_r1_30d -> users_views_r1_30d`,
`atms_r1 -> atms_rad_1` и т.д.), производные `traffic_ring_1_2 = ptr_r2 - ptr_r1` и
`orders_per_dp_r5_h90 = orders_r5_h90 / (unique_dp_r5_h90 + 1e-5)`. Дефолты: `region` NULL -> `UNKNOWN`,
дистанции NULL -> 10000, прочие числовые NULL -> 0. Это **iceberg-source** задача (читает silver, не ClickHouse).

## Рантайм

Iceberg-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark и не ClickHouse. Чтение silver,
сборку и запись gold выполняют entity-local модули `job/runtime.py` и `job/build.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Идентификаторы всех таблиц строятся из соответствующих `config.yaml` как tuple `(schema, table)`.
Запись идемпотентна: партиция `date` перезаписывается целиком (`overwrite` по фильтру `date`).

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
