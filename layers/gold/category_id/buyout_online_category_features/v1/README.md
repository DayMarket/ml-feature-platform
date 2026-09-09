# iceberg.gold.feature_platform_buyout_online_category_features

Online-таблица категории для сервиса невыкупов: одна строка на `category_id` — сглаженная
выкупаемость и доля NO SHOW категории плюс общая выкупаемость и доля NO SHOW маркетплейса.

Зачем: в `feature_platform_buyout_online_sku_features` есть только sku с заказами за
90 дней. Для sku без строки модель невыкупов при обучении (MAD-13227, MAD-13695) видела
выкупаемость категории, а для категории без заказов — выкупаемость маркетплейса. Эта
таблица даёт сервису те же подстановки по `category_id` позиции корзины. Потребитель
читает последнюю дату: `WHERE date = (SELECT max(date) FROM ...)`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_online_category_features`.
- DAG: `feature-platform.layers.gold.category_id.buyout_online_category_features`
  (`layers/gold/category_id/buyout_online_category_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 06:00 UTC, `0 6 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.

## Ключ

`date, category_id`.

`date` совпадает с партицией источника: `analyze_date` снимка `history_order_items`,
то есть `data_interval_end - 1 day` в UTC.

## Источники

- `iceberg.gold.feature_platform_buyout_item_signal_features` — строки `key_type = 'category'`
  партиции той же даты (читается через Trino, имя источника строится из его `config.yaml`).

Внешних источников нет.

## Зависимости

`ExternalTaskSensor` на DQ-DAG источника:
`dbt.source.trino.ml_feature_platform_gold.feature_platform_buyout_item_signal_features.dq`
(`mode="reschedule"`, `check_existence=True`, таймаут 3 часа).

`execution_delta = 3 часа` — разница расписаний (06:00 против 03:00) в предположении, что
logical date DQ-DAG-а совпадает с logical date DAG-производителя; дельту нужно сверить
с фактическим расписанием DQ-DAG-а после его появления.

## Логика

Те же формулы, что в `buyout_online_sku_features`, k = 30:

- общая выкупаемость маркетплейса `marketplace_buyout_rate_90d =
  SUM(n_completed_90d) / SUM(n_delivered_90d)` по всем категориям сигнала
  (`marketplace_no_show_rate_90d` — так же по `n_no_show_90d`);
- категория стягивается к ней: `category_buyout_rate_90d =
  (marketplace_rate * 30 + raw_rate * n_delivered) / (30 + n_delivered)`;
- сырые выкупаемость и доля NO SHOW (`*_raw_90d`) и `cat_n_delivered_90d` остаются
  в строке рядом со сглаженными.

Как сервису подставлять (см. также README `buyout_online_sku_features`):

- sku без строки в таблице sku → выкупаемость sku и карточки = `category_buyout_rate_90d`,
  число доставок sku и карточки = 0, разрыв sku−карточка = 0;
- категории нет и здесь → `marketplace_buyout_rate_90d` / `marketplace_no_show_rate_90d`
  (одинаковы во всех строках партиции).

## Caveats

- В таблице только категории с заказами за 90 дней; строк на дату — по их числу.
- Значения на дату равны `category_*_90d` / `marketplace_*_90d` в
  `buyout_online_sku_features` той же партиции: два DAG-а читают одну партицию сигнала.
- Таблица не публикуется в ranking upload: потребитель — сервис невыкупов.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_bx_analytics`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 4Gi / 2 CPU.

Перед запросом DAG проверяет через PyIceberg обе таблицы — источник и выход. Запись
идемпотентна: партиция `date` перезаписывается целиком через `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `team:buyer`.
