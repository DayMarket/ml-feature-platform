# iceberg.silver.feature_platform_order_completion_category_features

Дневной снапшот долей выкупа (`COMPLETED`) и невыкупа (`RETURNED NO SHOW`) заказов по уровням
иерархии категорий: `leaf`, `l1`, `l2`, `l3`, `l4`.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_order_completion_category_features`.
- DAG: `feature-platform.layers.silver.category_level_category_id.order_completion_category_features`
  (`layers/silver/category_level_category_id/order_completion_category_features/v1/dag.py`).
- Групповой тег Airflow: `order-completion-rates`.
- Расписание: ежедневно в 03:00 UTC, `0 3 * * *`.
- `start_date=2026-04-01T00:00:00Z`, `catchup=False`, `max_active_runs=1`.
- Сенсоров нет. `history_order_items` — внешний DE-источник; его снапшот за `analyze_date = D`
  публикуется в `D 19:00 UTC`, то есть за 8 часов до запуска DAG за `D`.

## Грейн / ключ

`date, category_level, category_id`.

Все пять срезов лежат в одной таблице и различаются колонкой `category_level`:

| `category_level` | Источник `category_id` |
| --- | --- |
| `leaf` | `history_order_items.category_id` без подъема по дереву |
| `l1` … `l4` | `dict.category.l1_category` … `l4_category` |

`date` равна `analyze_date` источника и вычисляется как `data_interval_end - 1 day` в UTC.
Признак считается обновляемым один раз в сутки, поэтому потребитель мерджит строку к `date + 1`.
Сдвиг в таблицу не зашит.

## Источники

- `"dwh-iceberg".silver.history_order_items` — снапшот истории заказов на `analyze_date`.
- `"dwh-clickhouse".dict.category` — иерархия категорий `l1_category` … `l4_category`.
  Читается тем же Trino-запросом через ClickHouse-каталог, отдельного ClickHouse-коннекта нет.

## Логика

Внутри снапшота `analyze_date = date` берутся позиции с `generated_at > date - INTERVAL '120' DAY`
(окно задано `source.lookback_days` в `config.yaml`).

Исключаются возвраты по вине маркетплейса или контента:
`return_cause NOT IN ('MISSING', 'DEFECTED', 'BAD_QUALITY', 'WRONG_ITEM', 'PHOTO_MISMATCH', 'CONTENT')`.

Каждая позиция раскладывается на пять строк уровня через `CROSS JOIN UNNEST`, затем строки с
`category_id IS NULL OR category_id <= 0` отбрасываются. Метрики на уровне
`category_level, category_id`:

- `total_orders = COUNT(DISTINCT order_id) FILTER (real_order_item_status != 'ACTIVE')`;
- `part_completed_orders = COUNT(DISTINCT order_id) FILTER (status = 'COMPLETED') / total_orders`;
- `part_no_show_from_total = COUNT(DISTINCT order_id) FILTER (status = 'RETURNED NO SHOW') / total_orders`.

Строки с `total_orders = 0` не пишутся; нулевой знаменатель не заполняется нулём.

## Caveats

- **Знаменатели по уровням не совпадают.** Фильтр строгий (`lN_category > 0`), fallback на
  ближайшего предка не делается. Из 6853 категорий `dict.category` уровень `l2` заполнен у 6830,
  `l3` — у 6598, `l4` — только у 4758 (69%). Заказы в категориях с деревом мельче уровня N
  не входят в срез `lN` вообще. Это отличается от подхода
  `layers/silver/account_id_category_id/account_l*_event_w_imps_counts`, где применяется
  `COALESCE` вниз по иерархии.
- `return_cause NOT IN (...)` в Trino отбрасывает строки с `return_cause IS NULL`. По замеру на
  `analyze_date = 2026-08-01` за 120 дней таких строк 12 995 из 32 251 724 (0.04%), и все они
  внутри статусов `RETURNED` и `RETURNED AFTER COMPLETED`. Ни одна позиция `COMPLETED`,
  `RETURNED NO SHOW` или `ACTIVE` не теряется.
- Один `order_id` может содержать позиции с разными статусами, поэтому
  `part_completed_orders + part_no_show_from_total` не обязано давать долю до единицы. По той же
  причине один заказ учитывается в нескольких `category_id` одного уровня, если в нем позиции
  из разных категорий.
- Join `history_order_items.category_id = dict.category.id` на замере покрывает 100% позиций.
  `dict.category` — внешний словарь без версионирования: перестройка дерева меняет исторические
  срезы задним числом только для новых партиций, старые партиции не пересчитываются.
- Объем: на `2026-08-01` — 9 423 строки (`leaf` 4474, `l1` 23, `l2` 219, `l3` 1663, `l4` 3044).
- Ретеншн источника — 126 значений `analyze_date`, поэтому окно 120 дней помещается в один
  снапшот, но запас всего 6 дней. При сокращении ретеншна окно надо пересматривать.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_search`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 4Gi / 2 CPU.

Запись идемпотентна: партиция `date` перезаписывается целиком через PyIceberg `overwrite`.
Пустой результат источника не пишется — задача падает, чтобы не затереть хорошую партицию.

## Владелец / алерты

`table.meta.team = team:search`, alerts `search`, severity P3, webhook `oncall_webhook_search`.
