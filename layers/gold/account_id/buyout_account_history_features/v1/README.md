# iceberg.gold.feature_platform_buyout_account_history_features

Ежедневный as-of срез признаков истории выкупа по аккаунту: объём истории, выкупаемость в
деньгах, позициях и заказах, состав невыкупа, серии исходов, способы оплаты, чек, счётчики
создания заказов и пожизненные факты аккаунта. Витрина — офлайн-источник признаков для модели
невыкупов (MAD-13413, переиспользует прототипы MAD-13227).

## Выход и оркестрация

- Таблица: `iceberg.gold.feature_platform_buyout_account_history_features`.
- DAG: `feature-platform.layers.gold.account_id.buyout_account_history_features`
  (`layers/gold/account_id/buyout_account_history_features/v1/dag.py`).
- Групповой тег Airflow: `buyout-features`.
- Расписание: ежедневно в 04:00 UTC, `0 4 * * *` (09:00 по Ташкенту).
- `start_date=2026-08-20T00:00:00Z`, `catchup=False`, `max_active_runs=1`,
  `is_paused_upon_creation=True`.
- Партиция `date = D` берётся из `data_interval_start` в UTC: запуск в `D+1 04:00 UTC`
  считает партицию `D`. К этому моменту срез `analyze_date = D` уже опубликован
  (публикация в `D 19:00 UTC` = полночь Ташкента дня `D+1`).
- Запись идемпотентна: `writeTo(...).overwritePartitions()` перезаписывает только партицию `date`.

## Грейн / ключ

`date, account_id` — одна строка на аккаунт в сутки.

Популяция: все аккаунты `account_id > 0` с хотя бы одним заказом в окне среза (182 дня до `D`).
Ограничения прототипа сняты: ни фильтра «есть заказ на `D+1`», ни хэш-сэмпла.

Точка отсчёта as-of — конец суток `D` по Ташкенту. В терминах прототипа:
`snapshot_date = date`, `decision_date = date + 1`. Отдельной колонки `decision_date` в витрине
нет: потребитель, принимающий решение в сутки `D+1`, читает партицию `date = D`.

## Источники

- `iceberg.silver.history_order_items` — as-of состояние позиций заказа, партиция `analyze_date = D`.
  Внешний DE-источник (dbt-модель `models/core/history_order_items.sql`).
- `iceberg.silver.order_items` — сырые позиции заказов, только факты создания
  (`generated_at`, `purchased_at`, `order_item_status`): горизонт счётчиков 365 дней не помещается
  в окно среза 182 дня.
- `iceberg.silver.feature_platform_account_lifetime_facts` — пожизненные факты аккаунта,
  партиция `date = D` (repository-managed, `layers/silver/account_id/account_lifetime_facts/v1`).

## Зависимости

`ExternalTaskSensor` на DQ-DAG silver-витрины пожизненных фактов:
`dbt.source.trino.ml_feature_platform_silver.feature_platform_account_lifetime_facts.dq`,
`execution_delta = 1 сутки 2 часа`.

Сутки в дельте не опечатка. Silver-витрина помечает партицию датой конца интервала Airflow:
партицию `D` пишет запуск, стартовавший в `D 02:00 UTC` (логическая дата `D-1 02:00`), а запуск
в `D+1 02:00 UTC` пишет уже партицию `D+1`. Витрина читает партицию `D` — снимок источника,
сделанный внутри суток `D`, до конца окна; более свежая партиция `D+1` снята уже после конца
суток `D` и в as-of витрину не берётся. Дельта указывает на DQ-запуск именно партиции `D`
в предположении, что DQ-DAG разделяет логическую дату производящего silver-DAG.

**Дельта предварительная.** После появления DQ-DAG сверить его расписание и поправить
`ACCOUNT_LIFETIME_FACTS_DQ_EXECUTION_DELTA` в `dag.py`.

`history_order_items` и `order_items` — внешние источники DE, сенсоров на них нет: срез
`analyze_date = D` публикуется в `D 19:00 UTC`, за 9 часов до запуска.

## Логика

Порт Trino-прототипов `ml-buyout-service/model/features/sql/user_features_asof.sql` и
`user_creation_facts.sql` на Spark SQL. Три блока в `job/getting_buyout_account_history_features.py`:

1. `asof_history_sql` — признаки по срезу `analyze_date = D`:
   позиции → флаги исхода (`is_delivered`, четыре составляющие невыкупа, уважительный возврат,
   клиентский невыкуп) → свёртка до заказа → агрегаты по аккаунту, окна последних 3/5/10
   завершённых заказов и текущая серия одинаковых исходов.
2. `orders_created_sql` — счётчики созданных заказов за 1/7/30/90/365 суток, окно оканчивается
   в полночь Ташкента дня `D+1`, то есть на конце суток `D`.
3. `lifetime_facts_sql` + производные — пожизненные поля аккаунта и `tenure_days_true`,
   `days_since_registration`, `history_left_censored_true`, которые в MAD-13227 считались
   на сборке обучающего набора.

Ключевые определения (сохранены из прототипа):

- доставка фиксируется по `delivered_at`, а у курьерки — по `issued_at`: у курьерских заказов
  `delivered_at` всегда пуст;
- клиентский невыкуп = `RETURNED NO SHOW` ∪ (`RETURNED` и `return_cause = 'CANCELED'`);
- четыре составляющие невыкупа (как в MAD-11218): no-show (возврат `CANCELED` через 6+ дней
  после доставки), отмена после доставки (менее 6 дней), возврат на вручении (в течение часа
  после выдачи) и возврат после вручения (позже часа);
- уважительный возврат — `return_cause IN ('MISSING', 'DEFECTED', 'BAD_QUALITY', 'WRONG_ITEM',
  'PHOTO_MISMATCH', 'CONTENT')` с непустым `returned_at`;
- выкупаемость заказа в деньгах = `gmv_completed / (gmv_delivered − gmv уважительных возвратов)`;
- `buyout_trend` = последние 3 минус первые 3 заказа, но только при 4+ завершённых заказах:
  иначе оба окна — одни и те же заказы и «мало истории» выглядело бы как «динамики нет».

## Caveats

Осознанные решения прототипа, перенесённые в витрину:

- **Денежные ставки считаются через `gmv_completed`, а не `gmv_net`.** Объёмы возвратов в срезе
  берутся из текущего состояния `silver.order_items` и в старых срезах не as-of; `gmv_net`
  и `gmv_returned` годятся только для таргетов.
- **Стаж по окну среза занижен.** Окно 182 дня короче медианного стажа покупателя, поэтому
  `tenure_days_win` и `first_order_date_win` — про окно, а не про жизнь. Настоящие значения —
  `tenure_days_true` и `first_order_date_ever` из пожизненных фактов. Признак
  `history_left_censored` помечает аккаунты, у которых самый ранний заказ лежит у левого края
  окна (окно приближается как «минус полгода» плюс 7 дней буфера);
  `history_left_censored_true` — то же по настоящей дате первого заказа и порогу 182 дня.
- **Заказы того же дня в витрину не входят.** Срез обрывается в полночь, поэтому заказы,
  созданные утром дня решения до момента решения, ему не видны — это около 30% целевых заказов
  в наборе MAD-13227. Признак «заказы того же дня до текущего» остаётся обязанностью онлайн-сервиса:
  на конец суток `D` его считать не от чего.
- **Поля `first_issued_*` и атрибуция не занулены по будущему.** В витрине «сегодня» всегда позже
  прошлых выкупов, поэтому значения отдаются как есть. Зануление относительно исторической даты
  решения (маска «дата первого выкупа ≥ даты решения ИЛИ сентинел 1970») делает сборщик обучающего
  набора: в MAD-13227 эта группа несла 65% невыкупа против 16% фона.
- **`accounts_per_install_current` — текущее значение без истории**, признак приблизительный.
- **Пожизненные факты берутся из снимка внутри суток `D` (02:00 UTC).** У аккаунта, чей первый
  заказ в жизни случился в сутки `D` после 07:00 по Ташкенту, снимок его ещё не видит: поля
  `first_order_date_ever`, `registration_date` и производные останутся пустыми, хотя сам аккаунт
  в популяции есть. Обратная развилка (читать более свежую партицию `D+1`) добавила бы в as-of
  витрину сведения, снятые уже после конца суток `D`.
- **Счётчики создания могут быть `NULL`.** Если у аккаунта нет строк в `silver.order_items` внутри
  окна сканирования (например, все позиции в статусах `CREATED`/`NOT_CREATED`), счётчики остаются
  пустыми: нулём они не заполняются. Сборщик обучающего набора в MAD-13227 заполнял счётчики нулями.
- **Окно сканирования `order_items` шире окна счётчиков**: `purchased_at` может отстоять от
  `generated_at`, поэтому слева берётся запас 7 дней, справа — 2 дня (как в прототипе).
- **Популяция ограничена только `account_id > 0`.** Фильтр `order_item_status NOT IN ('CREATED',
  'NOT_CREATED')` из прототипа относился к выборке целевых заказов из `silver.order_items`;
  в срезе `real_order_item_status` принимает 7 других значений, и такого фильтра там нет.
- **`return_cause` в срезе — пустая строка, а не `NULL`**: приводится через `NULLIF`.

Диалектные замены Trino → Spark SQL (детали — в таблице соответствия в описании задачи):
`REDUCE` → `AGGREGATE`, `ARRAY_AGG(... ORDER BY ...)` → `TRANSFORM(SORT_ARRAY(COLLECT_LIST(STRUCT(...))))`,
`APPROX_PERCENTILE` → `PERCENTILE_APPROX`, `with_timezone` → `TO_UTC_TIMESTAMP`,
`ts AT TIME ZONE` → `FROM_UTC_TIMESTAMP`, `date_add('day', n, d)` → `DATE_ADD(d, n)`,
`date_diff('day', a, b)` по timestamp → `TIMESTAMPDIFF(DAY, a, b)` (не `DATEDIFF`: тот считает
календарные сутки, а Trino отбрасывает дробную часть), индексация массива `arr[1]` → `ELEMENT_AT(arr, 1)`.

## Рантайм

Spark on k8s: общий SparkApplication template `config/spark/layer_spark_application.yaml`,
общий образ Spark и доставка кода через `git-sync`; отдельный образ не собирается.
Resource profile: `large` (`config/spark/resources.yaml`).

Пояс сессии Spark в entrypoint принудительно `UTC`: границы суток режутся
`FROM_UTC_TIMESTAMP`/`TO_UTC_TIMESTAMP` от Ташкента и зависят от базового пояса.

## Владелец / алерты

`table.meta.team = team:buyer`, `dag.team = buyer`, `dag.owner = team:buyer`,
alerts `buyer`, severity `P2`, webhook conn id `oncall_webhook_buyer`.

## Открытые вопросы

- Имена и типы колонок сверены с `layers/silver/account_id/account_lifetime_facts/v1/migrations/create_table.sql`
  (`first_city_id` — строка, `accounts_per_install_current` — `INT`). При изменении silver-контракта
  править `lifetime_facts_sql` и миграции обеих gold-таблиц.
- `execution_delta` сенсора уточняется после появления DQ-DAG (см. «Зависимости»).
- Развилка по свежести пожизненных фактов: витрина читает партицию `D` (снимок внутри суток `D`).
  Если владелец предпочтёт самый свежий снимок, надо читать партицию `D+1` и одновременно
  поменять дельту сенсора на 2 часа. Расхождение затрагивает только аккаунты с первым в жизни
  заказом в сутки `D` после 07:00 по Ташкенту.
- Conn id `oncall_webhook_buyer` собран по конвенции `oncall_webhook_<team>` — подтвердить у владельца.
- `registration_date` отдаётся как есть: сентинел `1970-01-01`, если он встречается в источнике,
  даст большое `days_since_registration`. В MAD-13227 поле тоже не гейтилось.
- Из `user_creation_facts.sql` в витрину не перенесены `gmv_created_prev_365d`,
  `order_seq_in_window`, `hours_since_prev_order` и `days_since_prev_order`: первые два относятся
  к конкретному заказу, последние два дублируют `days_since_last_order_win`. Добавить — по запросу владельца.
