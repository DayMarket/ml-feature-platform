# iceberg.gold.feature_platform_buyout_online_sku_features

Online-таблица SKU для сервиса невыкупов: одна строка на `sku_id` — собственный сигнал,
родительские ставки (карточка, категория, магазин, бренд) и сглаженные оценки.

Это копия сигнала для сервиса, а не новая семантика: все числа приходят из
`feature_platform_buyout_item_signal_features`, здесь они разворачиваются в широкую строку
и сглаживаются к родителю. В таблице все активные sku в наличии плюс все sku с доставками
за 90 дней, поэтому сервису достаточно одного поиска по `sku_id` за последнюю дату:
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
  `shrunk = (parent_rate * 30 + raw_rate * n) / (30 + n)`;
- магазин стягивается к общей выкупаемости маркетплейса той же формулой
  (`shop_buyout_rate_shrunk_90d`); сама общая выкупаемость маркетплейса отдаётся
  колонками `marketplace_buyout_rate_90d` / `marketplace_no_show_rate_90d`.

Признак гипотезы MAD-13413 «размеры внутри карточки выкупаются по-разному»:
`sku_vs_product_gap_90d = sku_buyout_rate_shrunk_90d - product_buyout_rate_shrunk_90d`.

Сырые родительские ставки и объёмы (`*_n_delivered_90d`) остаются в строке рядом со
сглаженными: потребитель может пересобрать сглаживание с другим k.

## Caveats

- В таблице все активные sku в наличии (`status = 'ACTIVE'`, `quantity_active +
  quantity_additional + quantity_fbs > 0` в `silver.sku`, ≈2.3 млн) плюс все sku с доставками
  за 90 дней (≈1.5 млн, большей частью те же). Sku, которого не было в наличии на снимке
  D−1 и который появился позже, в партиции нет — сервис берёт для него общую выкупаемость
  маркетплейса из любой строки (`marketplace_*_90d` одинаковы во всей партиции).
- `LEFT JOIN` к родителям: если у sku нет `product_id`, `shop_id` или `brand_name_id`
  в `silver.sku`, соответствующие колонки остаются `NULL`, а сглаженные ставки
  считаются от `n = 0`, то есть равны ставке категории.
- Атрибуты `silver.sku` берутся текущим снимком: смена категории у sku задним числом
  меняет и родителя, и сглаженную оценку.
- Таблица не публикуется в ranking upload: потребитель — сервис невыкупов, а не сервис
  ранжирования.

## Колонки для модели невыкупов и подстановки

Модель (MAD-13695) берёт по позициям корзины ровно эти колонки и сводит их по корзине:
выкупаемость sku, карточки, магазина и категории — средним с весом GMV позиции,
перцентили, минимум и среднее числа доставок — без весов.

| Признак модели | Колонка витрины |
|---|---|
| выкупаемость sku, перцентили по корзине, худший sku | `sku_buyout_rate_shrunk_90d` |
| выкупаемость карточки товара | `product_buyout_rate_shrunk_90d` |
| выкупаемость магазина | `shop_buyout_rate_shrunk_90d` (не сырая `shop_buyout_rate_90d`) |
| выкупаемость и доля NO SHOW категории | `category_buyout_rate_90d`, `category_no_show_rate_90d` |
| число доставок sku и карточки | `sku_n_delivered_90d`, `product_n_delivered_90d` |
| разрыв sku против карточки | `sku_vs_product_gap_90d` |

Подстановки уже посчитаны в строке, как при обучении (`cart_item_signal.sql`):

- sku без доставок за 90 дней: `sku_n_delivered_90d = 0`, сырые доли sku `NULL`,
  `sku_buyout_rate_shrunk_90d = category_buyout_rate_90d`; карточка без доставок — так же;
  разрыв считается из этих сглаженных значений;
- категория без доставок: `category_*_90d = marketplace_*_90d`;
- сервису подставлять нечего — только для sku без строки (появился после снимка) взять
  `marketplace_buyout_rate_90d` / `marketplace_no_show_rate_90d`, число доставок 0, разрыв 0.

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
