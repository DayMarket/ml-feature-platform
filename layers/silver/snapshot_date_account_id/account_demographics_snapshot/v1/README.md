# account_demographics_snapshot

Канонический дневной snapshot gender и возраста пользователя в полных годах.

## Выход и оркестрация

- Таблица: `iceberg.silver.feature_platform_account_demographics_snapshot`.
- DAG: `feature-platform.layers.silver.snapshot_date_account_id.account_demographics_snapshot`.
- Путь: `layers/silver/snapshot_date_account_id/account_demographics_snapshot/v1`.
- Airflow group tag: `recsys-main-page-features`.
- Расписание: ежедневно в `19:00 UTC`, то есть в `00:00 Asia/Tashkent`.
- `start_date=2026-06-01T00:00:00Z`, `catchup=false`.

`snapshot_date` рассчитывается как локальная дата
`data_interval_end AT TIME ZONE 'Asia/Tashkent'`. Повторный запуск одной партиции использует ту
же дату и идемпотентно перезаписывает её через PyIceberg `overwrite`.

Подтверждённые upstream DQ DAG id внешних источников отсутствуют, поэтому отдельные sensors не
добавлены.

## Grain и схема

Grain и уникальный ключ: `snapshot_date,account_id`.

- `snapshot_date` — дата снимка в `Asia/Tashkent`;
- `account_id` — положительный идентификатор пользователя;
- `gender` — `M`, `F` или `NULL`;
- `age` — полное количество лет на `snapshot_date` либо `NULL`.

Дата рождения используется только внутри SQL и не публикуется.

## Источники и population

Чтение выполняется через Trino connection `trino_recsys`:

- `"dwh-iceberg".silver.customer`: `account_id`, `sex`;
- `"ch-ecosystem".ecosystem.ecosystem_users`: `last_user_id_m`,
  `last_gender_ub`, `birth_year_UB`.

Источники объединяются через `FULL OUTER JOIN` по
`customer.account_id = ecosystem_users.last_user_id_m`. Контракт предполагает уникальность
`account_id` в обоих источниках. В snapshot входят пользователи, присутствующие хотя бы в одном
источнике.

## Gender

Gender из UM имеет приоритет над Ecosystem после нормализации:

```text
UM: MAN -> M, WOMAN -> F, остальные значения -> NULL
Ecosystem: M -> M, F -> F, остальные и пустые значения -> NULL
gender = COALESCE(um_gender, ecosystem_gender)
```

Приоритет применяется только к gender.

## Возраст

Возраст рассчитывается исключительно из `birth_year_UB`:

```sql
NULLIF(
    TRY_CAST(birth_year_UB AS DATE),
    DATE '1970-01-01'
)
```

Неприводимые значения и технический placeholder `1970-01-01` становятся `NULL`. Если дата
рождения отсутствует или позже `snapshot_date`, `age = NULL`. Иначе из разницы календарных лет
вычитается единица, если день рождения в году snapshot ещё не наступил. `NULL` не заменяется
нулём.

Возраст не рассчитывается из UM и не зависит от даты фактического запуска.

## Проверки качества

Перед записью producer проверяет:

- непустой результат;
- `snapshot_date` заполнена и равна целевой партиции;
- уникальность `snapshot_date,account_id`;
- `account_id > 0`;
- `gender IN ('M', 'F')` либо `NULL`;
- `age` является целым неотрицательным числом либо `NULL`.

Доля `NULL` в age/gender, а также `min`, median, p95, p99 и `max` возраста логируются.

После merge в master стандартная синхронизация создаст dbt DQ на уникальность ключа и
`not_null` ключевых колонок.

## Потребители

G5 выбирает последний snapshot, удовлетворяющий условию:

```sql
snapshot_date <= DATE(calculated_at AT TIME ZONE 'Asia/Tashkent')
```

S5 поставляет числовой `age` и `gender`. Age buckets определяются в Gold и не являются частью
Silver-контракта. Потребители: account profile и gender-фичи, category gender click shares,
account-category gender compatibility, weighted female/male category clickers и product gender
popularity-фичи. Прямой ranking-service upload для Silver-таблицы не настраивается.

## Рантайм, владелец и алерты

Airflow/Python + `pyiceberg`, образ
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

`table.meta.team = team::recsys`; DAG/alerts team `recsys`; severity `P3`; webhook
`oncall_webhook_recsys`.
