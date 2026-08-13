# iceberg.silver.feature_platform_account_lifetime_facts

Дневной снапшот пожизненных фактов аккаунта: первый заказ, регистрация, канал привлечения.
Мост из ClickHouse (`marketing.account_properties`) в Iceberg: без него gold-витрины модели
невыкупов не могут читать эти поля через Trino.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_account_lifetime_facts`.
- DAG: `feature-platform.layers.silver.account_id.account_lifetime_facts`
  (`layers/silver/account_id/account_lifetime_facts/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 02:00 UTC, `0 2 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.

## Грейн / ключ

`date, account_id`.

`date` равна дате конца интервала Airflow: строка описывает состояние источника на эту дату.
Сами значения — исторические факты, они не меняются; снапшот нужен, чтобы популяция и
поздние правки источника были воспроизводимы.

## Источники

- `marketing.account_properties` (ClickHouse, внешняя таблица DE) — строка на `account_id`.

## Зависимости

Сенсоров нет: источник не принадлежит feature-platform, DQ-DAG-а для него нет. Контракт
свежести DE по `marketing.account_properties` не зафиксирован — вопрос вынесен в PR.
Пустой результат источника не пишется: `require_non_empty` останавливает задачу с ошибкой, чтобы не затереть
хорошую партицию.

## Логика

Берутся только поля «первого» и регистрационного ряда: они уже произошли и не меняются.
Поля текущего состояния (`segment`, RFM, `last_*`, `is_banned`) намеренно не берутся — их
сегодняшнее значение было бы будущим относительно исторического решения. Прецедент утечки
в MAD-13227: шесть таких колонок давали ROC-AUC 0.7702 против честных 0.7540.

Популяция: аккаунты с хотя бы одним созданным заказом (`fo_date_created_uz > 1970-01-01`,
`account_id > 0`) и активностью за последние 200 дней (`last_order_date_created_uz >= date - 200`).
Фильтр по `last_order_date_created_uz` — только отбор популяции, признаком он не становится:
окно покрывает всех, кто может попасть в дневную популяцию gold-витрины (история 182 дня
плюс запас). Замер 12.08.2026: 4.36 млн строк из 8.86 млн аккаунтов с заказами.

## Caveats

- `first_issued_order_date` — про первый ВЫКУПЛЕННЫЙ заказ. При сборке обучающего набора
  это поле обязано зануляться, если дата позже даты решения, иначе получается утечка.
- `accounts_per_install_current` — текущее значение без истории, признак приблизительный
  и для исторических решений не точен.
- `first_city_id` в источнике — строка (`LowCardinality(String)`), а не числовой ID города;
  тип сохранён как `STRING`, приведение к `city_id` из `order_items` не делается.
- Даты источника — в Asia/Tashkent (суффикс `_uz` в исходных именах). Сентинел
  `1970-01-01` в `fo_date_issued_uz` означает «выкупленного заказа не было».
- Строк порядка 4.4 млн на партицию: полный снапшот, а не дельта.

## Рантайм

ClickHouse-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через
ClickHouse-connection, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 16Gi / 4 CPU.

`source.clickhouse_conn_id` сейчас `clickhouse_account_lifetime_facts_dag` — это заглушка. Доступ к
`marketing.account_properties` зависит от RBAC: коннекшн clickhouse_account_lifetime_facts_dag заводится DE по заявке (конвенция clickhouse_<dag>_dag в product-analytics-dags)
до включения DAG.

Запись идемпотентна: партиция `date` перезаписывается целиком через PyIceberg `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `team:buyer`.
Вебхук `team:buyer` — рабочее прод-значение buyer-команды (например, DAG user_daily_metrics_ice в product-analytics-dags).
