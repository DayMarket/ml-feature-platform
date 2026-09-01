# Ежедневные атрибуты пользователя

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_account_demographics`.
- DAG: `feature-platform.layers.silver.account_id.account_demographics`.
- Путь: `layers/silver/account_id/account_demographics/v1`.
- Airflow group tag: `recsys-main-page-features`.
- Расписание: ежедневно в `19:00 UTC`, то есть в `00:00 Asia/Tashkent`.
- `start_date=2026-08-08T19:00:00Z`, `catchup=true`: при первом включении DAG выполняет
  начальный backfill примерно за две недели.

`dt` рассчитывается как локальная дата `data_interval_end` в `Asia/Tashkent`. Повторный запуск
одной даты идемпотентно перезаписывает её через PyIceberg `overwrite`.

## Grain и схема

Grain и уникальный ключ: `dt,account_id`.

- `dt` — дата расчёта в `Asia/Tashkent`;
- `account_id` — положительный идентификатор пользователя;
- `gender` — `MALE`, `FEMALE` или `NULL`;
- `age` — полное количество лет пользователя на `dt` либо `NULL`;
- `city_name` — самый частый ближайший город по `GEO_INFO` за настроенное окно
  `source.lookback_days` либо `NULL`;
- `platform` — самая частая платформа по уникальным сессиям за настроенное окно
  `source.lookback_days`: `IOS`, `ANDROID`, `WEB` или `NULL`.

Дата рождения, координаты и технические счётчики используются только внутри SQL и не
публикуются.

## Источники

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-iceberg".silver.customer`: `account_id`, `sex`, `birth_date`;
- `"ch-ecosystem".ecosystem.ecosystem_users`: `last_user_id_m`, `last_gender_ub`;
- `"dwh-clickhouse".clickstream_b2c.events`: `account_id`, `event_type`, `received_at`,
  `event_id`, `event_properties` для `GEO_INFO`;
- `"dwh-clickhouse".dict.city`: `city_ru_name`, `city_latitude`, `city_longitude`;
- `"dwh-iceberg".silver.sessions_with_attribution`: `date_uz`, `account_id`, `session_id`,
  `platform`, `started_at`.

UM и Ecosystem объединяются через `FULL OUTER JOIN` по
`customer.account_id = ecosystem_users.last_user_id_m`. Город и платформа присоединяются через
`LEFT JOIN`, поэтому не расширяют исходную population S5.

Подтверждённые upstream DQ DAG id внешних источников отсутствуют, поэтому отдельные sensors не
добавлены.

## Gender и возраст

Gender из UM имеет приоритет над Ecosystem после нормализации:

```text
UM и Ecosystem: MAN/M -> MALE, WOMAN/F -> FEMALE, остальные значения -> NULL
gender = COALESCE(um_gender, ecosystem_gender)
```

Полная дата рождения берётся из UM и нормализуется в `Asia/Tashkent`. Технический placeholder
преобразуется в `NULL`:

```sql
NULLIF(
    CAST(AT_TIMEZONE(birth_date, 'Asia/Tashkent') AS DATE),
    DATE '1970-01-01'
)
```

Возраст считается в полных годах:

```sql
CAST(DATE_DIFF('year', birth_date, dt) AS INTEGER)
```

В Trino `DATE_DIFF('year', ...)` учитывает месяц и день: до дня рождения текущий неполный год
не засчитывается. `birth_year_ub` из Ecosystem содержит только год и не используется для age,
поскольку из него невозможно восстановить месяц и день. Если полная дата рождения отсутствует
или позже `dt`, `age = NULL`. Использование `dt`, а не `CURRENT_DATE`, сохраняет повторяемость
пересчёта одной даты.

## Город за настраиваемое окно

Длина окна задаётся в `config.yaml` как `source.lookback_days` и сейчас равна 28 дням. Окно:
`[dt - lookback_days, dt)` в `Asia/Tashkent`.

Для каждого валидного `GEO_INFO` извлекаются `latitude` и `longitude`. Каждое событие по его
уникальному `event_id` относится к ближайшему городу через `GREAT_CIRCLE_DISTANCE`, после чего
события суммируются по `account_id,city_name`. Координаты с плавающей точкой не входят в
`GROUP BY`. Выбирается город с максимальным числом событий. При равенстве используется самое
свежее событие, затем `city_name`.

В отличие от `any(event_properties)`, эта семантика действительно выбирает самый частый, а не
произвольный город.

## Платформа за настраиваемое окно

Длина окна берётся из того же `source.lookback_days`. Источник уже имеет session-grain и
партиционирован по `date_uz` в `Asia/Tashkent`, поэтому S5 не сканирует все clickstream-события.
Дополнительный точный фильтр по `started_at`, переведённому в `Asia/Tashkent`, сохраняет окно
`[dt - lookback_days, dt)`. Платформа нормализуется через `UPPER(TRIM(platform))`, после чего
остаются только `IOS`, `ANDROID`, `WEB`.

Частота считается по уникальным `session_id`, чтобы количество технических событий внутри
одной сессии не влияло на результат. `ARRAY_AGG` сортирует платформы по числу сессий по
убыванию; при равенстве выбирается платформа с самой свежей сессией, затем значение по
алфавиту.

## Проверки качества

Перед записью producer проверяет:

- непустой результат;
- `dt` заполнена и равна целевой дате;
- уникальность `dt,account_id`;
- `account_id > 0`;
- `gender IN ('MALE', 'FEMALE')` либо `NULL`;
- целочисленный `age >= 0` либо `NULL`;
- непустой `city_name` либо `NULL`;
- `platform IN ('IOS', 'ANDROID', 'WEB')` либо `NULL`.

Логируются доли `NULL` в age, gender, city и platform, а также распределение возраста.

После merge в master стандартная синхронизация создаст dbt DQ на уникальность ключа и
`not_null` ключевых колонок.

## Потребители и runtime

G5 выбирает последнюю запись, удовлетворяющую условию:

```sql
dt <= DATE(calculated_at AT TIME ZONE 'Asia/Tashkent')
```

Age buckets определяются в Gold и не являются частью Silver-контракта. Прямой ranking-service
upload не настраивается.

Пайплайн использует Airflow/Python + `pyiceberg`, образ
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` и pod размера `small`: 1 CPU, 4 GiB memory.

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
