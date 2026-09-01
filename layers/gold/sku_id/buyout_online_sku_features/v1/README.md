# iceberg.gold.feature_platform_buyout_online_sku_features

Online-таблица SKU для сервиса невыкупов: одна строка на `sku_id` — собственный сигнал,
родительские ставки (карточка, категория, магазин, бренд) и сглаженные оценки.

Это serving-контракт, а не новая семантика: все числа приходят из
`feature_platform_buyout_item_signal_features`, здесь они разворачиваются в широкую строку
и сглаживаются к родителю. Потребитель читает последнюю дату:
`WHERE date = (SELECT max(date) FROM ...)`.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_online_sku_features`.
- DAG: `feature-platform.layers.gold.sku_id.buyout_online_sku_features`
  (`layers/gold/sku_id/buyout_online_sku_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 06:00 UTC, `0 6 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.

## Грейн / ключ

`date, sku_id`.

`date` совпадает с партицией источника: `analyze_date` снапшота `history_order_items`,
то есть `data_interval_end - 1 day` в UTC.

## Источники

- `iceberg.gold.feature_platform_buyout_item_signal_features` — витрина платформы, партиция
  той же даты (читается через Trino, имя источника строится из его `config.yaml`);
- `"dwh-iceberg".silver.sku` — внешняя DE-таблица, маппинг `sku_id` на карточку, категорию,
  магазин и бренд.

## Зависимости

`ExternalTaskSensor` на DQ-DAG источника:
`dbt.source.trino.ml_feature_platform_gold.feature_platform_buyout_item_signal_features.dq`
(`mode="reschedule"`, `check_existence=True`, таймаут 3 часа).

`execution_delta = 3 часа` — разница расписаний (06:00 против 03:00) в предположении, что
logical date DQ-DAG-а совпадает с logical date DAG-производителя. DQ-DAG появится только
после мержа в `master`; дельту нужно сверить с его фактическим расписанием и при
необходимости поправить.

## Логика

Сглаживание — канон `cart_item_signal.sql` (MAD-13227), k = 30:

- общая выкупаемость маркетплейса считается по строкам `key_type = 'category'`;
- категория стягивается к глобальной ставке;
- `sku` и `product` стягиваются к сглаженной ставке своей категории:
  `shrunk = (parent_rate * 30 + raw_rate * n) / (30 + n)`.

Признак гипотезы MAD-13413 «размеры внутри карточки выкупаются по-разному»:
`sku_vs_product_gap_90d = sku_buyout_rate_shrunk_90d - product_buyout_rate_shrunk_90d`.

Сырые родительские ставки и объёмы (`*_n_delivered_90d`) остаются в строке рядом со
сглаженными: потребитель может пересобрать сглаживание с другим k.

## Caveats

- Популяция — только sku с историей за 90 дней (порядка 1.44 млн строк). Холодных sku в
  таблице нет: сервис для них падает на категорию или бренд из своих данных. Вопрос
  «весь каталог против sku с историей» вынесен в PR.
- `LEFT JOIN` к родителям: если у sku нет `product_id`, `shop_id` или `brand_name_id`
  в `silver.sku`, соответствующие колонки остаются `NULL`, а сглаженные ставки
  считаются от `n = 0`, то есть равны ставке категории.
- Атрибуты `silver.sku` берутся текущим снимком: смена категории у sku задним числом
  меняет и родителя, и сглаженную оценку.
- Таблица не публикуется в ranking upload: потребитель — сервис невыкупов, а не сервис
  ранжирования.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_bx_analytics`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 16Gi / 4 CPU.

`trino_bx_analytics` — рабочий Trino-коннекшн buyer-команды (используется DAG-ами product-analytics-dags, например cm2_early_estimate).
пока нет отдельного Trino-connection.

Перед запросом DAG проверяет через PyIceberg обе таблицы — источник и выход. Запись
идемпотентна: партиция `date` перезаписывается целиком через `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `team:buyer`.
Вебхук `team:buyer` — рабочее прод-значение buyer-команды (например, DAG user_daily_metrics_ice в product-analytics-dags).
