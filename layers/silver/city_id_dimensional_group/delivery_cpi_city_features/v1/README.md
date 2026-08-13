# iceberg.silver.feature_platform_delivery_cpi_city_features

Дневной снапшот эмпирического CPI логистики (стоимость доставки одной штуки) по городу и
габаритной группе за скользящее окно 90 дней. Витрина нужна модели невыкупов, чтобы
переводить вероятность невыкупа в деньги: прямое плечо оплачивается всегда, обратное —
только при невыкупе или возврате.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_delivery_cpi_city_features`.
- DAG: `feature-platform.layers.silver.city_id_dimensional_group.delivery_cpi_city_features`
  (`layers/silver/city_id_dimensional_group/delivery_cpi_city_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 03:00 UTC, `0 3 * * *`.
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`.
- Исторические партиции заполняет отдельный DAG
  `feature-platform.layers.silver.city_id_dimensional_group.delivery_cpi_city_features.backfill`
  (`dag_backfill.py`): `catchup=True`, `max_active_runs=3`, интервалы 2026-02-20 .. 2026-08-20
  (партиции 2026-02-21 .. 2026-08-19), создаётся на паузе, без on-call callback. Диапазон
  сверяется с фактически недостающими датами перед снятием с паузы; после заполнения DAG
  ставится на паузу и удаляется.

## Грейн / ключ

`date, city_id, dimensional_group`.

`date` равна дате конца интервала Airflow: окно затрат закрыто строго до неё,
`[date - 90 дней, date)`. Запуск в 03:00 UTC за интервал, который закончился в этот момент,
видит полные сутки `date - 1`.

## Источники

Все три — внешние DE-таблицы, платформа их не производит:

- `"dwh-iceberg".silver.preliminary_cm2_by_order_item` — фактические аллокации затрат
  прямого и обратного потока (UZS, IFRS);
- `"dwh-iceberg".silver.order_items` — город, регион, количество, даты доставки и возврата;
- `"dwh-iceberg".silver.sku` — габаритная группа товара.

## Зависимости

Сенсоров нет: ни один источник не принадлежит feature-platform, DQ-DAG-ов для них нет.
Контракт свежести DE по этим таблицам не зафиксирован — вопрос вынесен в PR. Защита от
пустого источника — проверка `require_non_empty` перед записью: пустой результат не
затирает хорошую партицию.

## Логика

1. Затраты: дедупликация `preliminary_cm2_by_order_item` по `order_item_id`
   (`ROW_NUMBER() ... ORDER BY order_issued_at DESC`, берём `rn = 1`), `NaN` и `NULL`
   заменяются нулём, служебные аккаунты (`account_id IS NULL OR = 0`) отброшены.
2. Позиции: в `order_items` нет `real_order_item_status`, поэтому потоки реконструируются
   из дат:
   - доставлен = `delivered_at IS NOT NULL OR (delivery_type = 'COURIER' AND issued_at IS NOT NULL)`;
   - обратный поток = `returned_at IS NOT NULL AND (delivered_at IS NOT NULL OR issued_at IS NOT NULL)`
     (отмена до отгрузки обратной логистики не создаёт).

   Сверка с каноническими счётчиками `items_*` из `order_item_ue_buyer` на неделе
   01–07.07.2026 (2.0 млн позиций): доставка — 98.8% точных совпадений, обратный поток —
   98.3% (недолов 0.12%, ложных 1.45%).
3. Габаритная группа: `SMALL / MEDIUM / LARGE`, всё остальное (включая пустую строку и
   `UNKNOWN`) считается `SMALL` — как в `business_parameters_v2.sql`.
4. Агрегация по городу и габаритной группе:
   `cpi_forward_uzs = SUM(forward_cost) / SUM(n_delivered)`,
   `cpi_reverse_uzs = SUM(reverse_cost) / SUM(n_reverse)`.
   Обратное плечо не взвешивается по типам возврата: суммируются фактические затраты всех
   трёх типов (невыкуп, возврат на выдаче, возврат после выкупа) и делятся на суммарные
   штуки обратного потока.
5. Фолбэк для тонких городов лежит в той же строке: `*_region_uzs` и `*_country_uzs`
   считаются оконными суммами по региону и по стране в той же габаритной группе.
   Уровень выбирает потребитель — по `n_items_delivered` / `n_items_reverse`.

## Caveats

- Хвост окна недозаполнен: заказ попадает в `preliminary_cm2_by_order_item` по мере
  завершения. Замер 12.08.2026: за `D-1` в витрине 26 тыс. позиций против ~250 тыс. на
  зрелых сутках (10%), `D-2` — 35%, `D-3` — 59%, `D-5` — 79%, к `D-7` около 90%. На окне
  90 дней это около 3% позиций, но хвост смещён к быстро завершившимся заказам.
- Окна источников заданы по разным полям: у затрат — `order_created_at`, у позиций —
  `generated_at`. На краях окна часть позиций не находит пары в `JOIN` и выпадает.
- `NULLIF` в знаменателе: если у пары «город × габаритная группа» нет обратного потока,
  `cpi_reverse_uzs` = `NULL`, а не ноль. Нулём не заполняем — потребитель обязан спуститься
  на региональный или страновой уровень.
- `region_id` берётся `ARBITRARY` по позициям города: привязка города к региону
  считается стабильной внутри окна.
- Позиции с `city_id IS NULL` отбрасываются.
- Дубли `order_item_id` в `preliminary_cm2_by_order_item` плавают между прогонами
  (подвох №11 справочника `01_cm2`), поэтому дедупликация обязательна и зашита в запрос.

## Рантайм

Trino-source пайплайн (Airflow/Python + `pyiceberg`), не Spark. Чтение через connection
`trino_search`, запись — через entity-local модуль `job/runtime.py`.
Образ задачи: `ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`, 8Gi / 2 CPU.

Выбор `trino_search` — вопрос PR: витрина не относится к поисковому домену, но у buyer-команды
пока нет отдельного Trino-connection.

Запись идемпотентна: партиция `date` перезаписывается целиком через PyIceberg `overwrite`.

## Владелец / алерты

`table.meta.team = team:buyer`, alerts `buyer`, severity P2, webhook `oncall_webhook_buyer`.
Идентификатор on-call webhook подтверждается в PR.
