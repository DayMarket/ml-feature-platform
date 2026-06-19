# silver.feature_platform_geo_user_activity_features

Активность пользователей (просмотры/заказы) по кольцам H3 (res 9) и трейлинг-окнам.

## Грейн / ключ
`date, h3_index`.

## Источник (внешний, ClickHouse)
- `silver.client_geo_activity_hex_9` — дневная активность по гексагонам (`daily_views`, `daily_orders`).

## Логика
Каждый центр расширяется до колец 0..5, дневная активность суммируется по кольцам, затем по окнам
30/60/90 дней (`day_diff BETWEEN 0 AND N-1`, отсчёт от `date`). **Расширенная сетка** rings 0..5 x 30/60/90
для `views_*` и `orders_*`.

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
