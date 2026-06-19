# silver.feature_platform_geo_user_location_features

Число пользователей по кольцам H3 (res 9) на последнем доступном снимке геолокаций.

## Грейн / ключ
`date, h3_index`. Колонка `report_date` хранит фактическую дату снимка (`max(report_date) <= date`).

## Источник (внешний, ClickHouse)
- `gold.geo_client_hist` — история геолокаций клиентов (`h3_9`, `report_date`).

## Логика
Берётся последний `report_date <= date`, число пользователей по hex суммируется по кольцам 0..5 вокруг каждого центра.

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
