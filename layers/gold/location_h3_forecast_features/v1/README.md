# gold.feature_platform_location_h3_forecast_features

Итоговая h3-витрина признаков для модели `location_forecast` — стабильный контракт фич (имена совпадают
с `features_to_use` модели, без per-prediction временных фич, которые модель добавляет на инференсе).

## Грейн / ключ
`date, h3_index`.

## Источник
Джойн пяти silver-таблиц на `(date, h3_index)`:
`geo_geointellect_features` (база), `dp_neighbor_order_features`, `geo_user_activity_features`,
`geo_user_location_features`, `geo_yandex_poi_features`.

## Логика
Переименования к контракту модели (`population_r0 -> population`, `views_r1_30d -> users_views_r1_30d`,
`atms_r1 -> atms_rad_1` и т.д.), производные `traffic_ring_1_2 = ptr_r2 - ptr_r1` и
`orders_per_dp_r5_h90 = orders_r5_h90 / (unique_dp_r5_h90 + 1e-5)`. Дефолты: `region` NULL -> `UNKNOWN`,
дистанции NULL -> 10000, прочие числовые NULL -> 0. Это **iceberg-source** задача (читает silver, не ClickHouse).

## Рантайм

ClickHouse-source пайплайн (Airflow/Python + `pyiceberg`), **не** Spark. Чтение из ClickHouse через
connection `clickhouse_dwh_team_logistics`, запись в Iceberg через общий модуль
`layers/_common/clickhouse_iceberg.py`. Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.
Каталог Iceberg (Hive metastore, warehouse, YC S3) настроен идентично Spark-шаблону
`config/spark/layer_spark_application.yaml`; ключи S3 берутся из connection `spark_ycs_connection`.

Оркестрация — DAG `location_forecast_features_dag` (`layers/gold/location_h3_forecast_features/v1/dag.py`),
ежедневно 00:00 UTC. Запись идемпотентна: партиция `date` перезаписывается целиком (`overwrite` по фильтру `date`).

## Владелец / алерты

`table.meta.team = team:operations`, alerts `operations-analytics`, severity P3, webhook `oncall_webhook_operations`.
