# iceberg.gold.feature_platform_buyout_online_city_features

Online-проекция CPI логистики по городу и габаритной группе для сервиса невыкупов.

Это serving-контракт, а не новая семантика: состав колонок и все формулы совпадают с
`iceberg.silver.feature_platform_delivery_cpi_city_features` один в один. Gold нужен, чтобы
сервис читал стабильный контракт и не зависел от перекладок в предагрегате. Потребитель
читает последнюю дату: `WHERE date = (SELECT max(date) FROM ...)`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_online_city_features`.
- DAG: `feature-platform.layers.gold.city_id_dimensional_group.buyout_online_city_features`
  (`layers/gold/city_id_dimensional_group/buyout_online_city_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 06:00 UTC, `0 6 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.

## Грейн / ключ

`date, city_id, dimensional_group`.

`date` совпадает с партицией silver-витрины: дата конца интервала Airflow, окно затрат
закрыто строго до неё.

## Источники

- `iceberg.silver.feature_platform_delivery_cpi_city_features` — витрина платформы,
  партиция той же даты (читается через Trino, имя источника строится из его `config.yaml`).

Внешних источников нет: все они уже отработаны в silver-слое.

## Зависимости

`ExternalTaskSensor` на DQ-DAG источника:
`dbt.source.trino.ml_feature_platform_silver.feature_platform_delivery_cpi_city_features.dq`
(`mode="reschedule"`, `check_existence=True`, таймаут 3 часа).

`execution_delta = 3 часа` — разница расписаний (06:00 против 03:00) в предположении, что
logical date DQ-DAG-а совпадает с logical date DAG-производителя. DQ-DAG появится только
после мержа в `master`; дельту нужно сверить с его фактическим расписанием и при
необходимости поправить.

## Логика

`SELECT` перечисленных колонок из партиции источника с подстановкой даты партиции.
Колонки перечислены явно (`PROJECTED_COLUMNS` в `job/query.py`), чтобы новая колонка в
silver не попадала в serving-контракт молча: расширение контракта — отдельная миграция
плюс правка списка.

Фолбэк для тонких городов остаётся в строке: `*_region_uzs` и `*_country_uzs`. Уровень
выбирает потребитель по `n_items_delivered` и `n_items_reverse`.

## Caveats

- Все оговорки silver-витрины действуют и здесь: недозаполненный хвост окна, разные поля
  окон у затрат и позиций, `NULL` вместо нуля при пустом знаменателе. Подробности —
  [`delivery_cpi_city_features`](../../../../silver/city_id_dimensional_group/delivery_cpi_city_features/v1/README.md).
- Таблица не публикуется в ranking upload: потребитель — сервис невыкупов, а не сервис
  ранжирования.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_bx_analytics`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 4Gi / 2 CPU.

`trino_bx_analytics` — рабочий Trino-коннекшн buyer-команды (используется DAG-ами product-analytics-dags, например cm2_early_estimate).
пока нет отдельного Trino-connection.

Перед запросом DAG проверяет через PyIceberg обе таблицы — источник и выход. Запись
идемпотентна: партиция `date` перезаписывается целиком через `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `team:buyer`.
Вебхук `team:buyer` — рабочее прод-значение buyer-команды (например, DAG user_daily_metrics_ice в product-analytics-dags).
