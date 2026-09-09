# iceberg.gold.feature_platform_buyout_online_account_features

Онлайн-проекция витрины признаков истории выкупа аккаунта: 84 колонки источника переносятся
как есть, плюс 19 serving-колонок, которые сервис невыкупов раньше досчитывал сам в запросе
на чекауте. Отдельная таблица нужна как serving-контракт сервиса — её состав меняется осознанно,
независимо от офлайн-витрины, из которой обучается модель.

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_online_account_features`.
- DAG: `feature-platform.layers.gold.account_id.buyout_online_account_features`
  (`layers/gold/account_id/buyout_online_account_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 06:00 UTC, `0 6 * * *` (11:00 по Ташкенту).
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`,
  `is_paused_upon_creation=True`.
- Партиция `date = D` вычисляется как `data_interval_end − 1 день` в UTC и совпадает с партицией
  витрины-источника.

## Грейн / ключ

`date, account_id` — одна строка на аккаунт в сутки, ровно как в источнике.

## Источники

- `iceberg.gold.feature_platform_buyout_account_history_features` (в Trino —
  `"dwh-iceberg".gold.feature_platform_buyout_account_history_features`), партиция `date = D`.
- `iceberg.silver.feature_platform_order_completion_city_features`, партиция `date = D` —
  доли выкупа города последнего заказа.
- `iceberg.silver.feature_platform_order_completion_region_features`, партиция `date = D` —
  то же по региону.

Обе гео-витрины помечают партицию `analyze_date` того же дня, что и витрина-источник, поэтому
проекция читает партицию `D` из всех трёх: будущее в признак не подмешивается.

## Зависимости

`ExternalTaskSensor` на таску `dq` DAG'а витрины-источника:
`feature-platform.layers.gold.account_id.buyout_account_history_features`,
`external_task_id="dq"`, `execution_delta = 2 часа` — разница расписаний (06:00 против 04:00):
`D 06:00 - 2 часа = D 04:00`, логическая дата запуска, который пишет партицию `D`.

Сенсор специально смотрит не на `dbt.source.trino.ml_feature_platform_gold.
feature_platform_buyout_account_history_features.dq`. Тот идёт в `0 1 * * *`, то есть всегда
раньше производителя (04:00), и партицию `D` проверяет только в `D+2 01:00` — на 19 часов позже
запуска проекции, за пределами её `timeout` и `dagrun_timeout`. Ни одна дельта этого не чинит:
ждать надо таску `dq` самого производителя (AGENTS.md, «Downstream-DAG'и»).

Ещё два `ExternalTaskSensor` — на таски `dq` DAG'ов гео-витрин
`feature-platform.layers.silver.order_city_id.order_completion_city_features` и
`feature-platform.layers.silver.order_region_id.order_completion_region_features`,
`execution_delta = 3 часа`: `D 06:00 - 3ч = D 03:00`, логическая дата запуска silver,
который пишет партицию `D`.

## Логика

`job/query.py` собирает `SELECT` фиксированного списка из 84 колонок (`FEATURE_COLUMNS`) из
партиции витрины-источника. Порядок колонок совпадает с `migrations/create_table.sql` источника;
новая колонка появляется здесь только вместе с миграцией онлайн-таблицы. Имя таблицы-источника
не зашито: DAG читает `config.yaml` витрины-источника и собирает Trino-имя из
`table.catalog/schema/name`. Перед выгрузкой все три таблицы проходят preflight в
Iceberg-каталоге.

Поверх проекции считаются 19 serving-колонок (`SERVING_COLUMNS`):

- индикаторы 0/1 по строковым колонкам источника — `last_nonbuyout_no_show`,
  `last_nonbuyout_cancel_after`, `last_nonbuyout_courier_other`, `last_pay_uzumcard`,
  `last_pay_nasiya`, `last_pay_uzumcheckout`, `last_pay_bonus`, `first_dp_pickup_point`,
  `first_dp_franchise`, `first_dp_uzpost`, `first_dp_missing`,
  `first_completed_order_is_postpaid`, `is_first_order_ever`;
- `has_asof_history` — константа `1`: в таблице только аккаунты с историей, ноль за
  отсутствующий аккаунт подставляет сам сервис;
- гео последнего заказа — `prev_city_part_completed`, `prev_city_part_no_show`,
  `prev_region_part_completed`, `prev_region_part_no_show`, `has_prev_city`: два `LEFT JOIN`
  по `last_order_city_id` и `last_order_region_id` за ту же партицию `date`.

Выражения дословно повторяют сборщик обучающего набора. В частности `first_dp_missing`
сравнивает с пустой строкой, а не с `COALESCE(..., '')`: при `NULL` в
`first_delivery_point_type` все четыре `first_dp_*` дают ноль — так же, как на обучении.
Расхождение в одном `CASE` даёт train/serve skew, который в метриках модели не виден.

Партиция читается срезами по остатку `account_id % shards` (`source.shards`, по умолчанию 16):
одна выгрузка на десятки миллионов аккаунтов не помещается в память задачи. Первый срез
перезаписывает партицию (`overwrite` с фильтром `date = D`), остальные дописываются (`append`).

## Caveats

- **Семантика 84 перенесённых колонок живёт не здесь.** Все определения признаков, окна и
  оговорки — в README витрины-источника:
  `../../buyout_account_history_features/v1/README.md`. Дублировать их здесь нельзя — разъедутся.
  Своя семантика у таблицы только в 19 `SERVING_COLUMNS`.
- **Гео-признаки пустые для аккаунта без заказов в окне.** `last_order_city_id` тогда `NULL`,
  `LEFT JOIN` не находит строку, `has_prev_city = 0`, доли — `NULL`.
- **Падение в середине цикла оставляет партицию неполной.** Повтор задачи начинается с первого
  среза и перезаписывает партицию целиком, поэтому состояние восстанавливается следующим успешным
  запуском. Потребителю до этого момента видна часть аккаунтов.
- **Пустой первый срез — ошибка**: задача падает, не трогая партицию, чтобы не затереть хорошие
  данные при неопубликованном источнике.
- **Число срезов подбирается под память.** При росте популяции увеличивать `source.shards`,
  а не память задачи: пиковая память определяется размером одного среза.

## Serving-контракт

Из этой таблицы 82 признака публикуются в Kafka сервиса ранжирования —
`upload/buyout_features_upload/v1` (feature group `fs_buyout_account_features_v1`,
DAG `feature-platform.upload.buyout_features_upload`, `07:00 UTC`). Порядок признаков в
том конфиге задаёт позиции в protobuf-массиве.

Сервис невыкупов (владелец — Широков) забирает таблицу в PostgreSQL. Правила чтения:

- брать строки последней доступной даты: `WHERE date = (SELECT max(date) FROM ...)`;
- пропуск в признаке — законное значение (нет истории, нулевой знаменатель, нет пожизненных
  фактов), нулём его заменять нельзя;
- поля `first_issued_*` и атрибуция описывают состояние на дату партиции; гейт «первый выкуп в
  будущем» нужен только при сборке обучающего набора по историческим датам, для онлайна он не
  применяется;
- признак «заказы того же дня до текущего» в витрине отсутствует — его считает сам сервис на
  момент решения;
- добавление и удаление колонок — изменение serving-контракта: миграция плюс согласование
  с владельцем сервиса.

У двух потребителей разное обращение с пропусками: PostgreSQL-выгрузка сохраняет `NULL`,
а Kafka-аплоад заполняет их нулями (`na.fill(0.0)` в job'е плюс `missing-feature-value: 0.0`
на стороне сервиса). Признак, где ноль и «нет данных» — разные вещи, требует отдельного
индикатора наличия, как `has_prev_city` для гео.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_bx_analytics`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 8Gi / 2 CPU.

## Владелец / алерты

`table.meta.team = team:buyer`, `dag.team = buyer`, `dag.owner = team:buyer`,
alerts `buyer`, severity `P2`, webhook conn id `team:buyer`.

## Открытые вопросы

- Airflow connection к Trino: домена buyer в списке (`trino_bx_analytics`, `trino_recsys`) нет,
  временно взят `trino_bx_analytics` — подтвердить у владельца.
- Conn id `team:buyer` собран по конвенции `oncall_webhook_<team>` — подтвердить у владельца.
- Число срезов 16 выбрано по оценке «десятки миллионов аккаунтов × 82 колонки»; после первого
  прогона сверить фактическую память задачи и при необходимости изменить `source.shards`.
- Connection `trino_bx_analytics` теперь читает ещё и две silver-витрины схемы `silver`
  (владелец — search). Права проверялись только `EXPLAIN`-ом; на первом прогоне убедиться,
  что чтение проходит и под сервисной учёткой задачи.
- На 2026-09-09 обе витрины аккаунта (`buyout_account_history_features` и эта) физически
  пусты: `max(date)` равен `NULL`. Гео-витрины при этом заполнены по `2026-09-08`. Пока
  цепочка не отработает хотя бы раз, аплоад публиковать нечего.
