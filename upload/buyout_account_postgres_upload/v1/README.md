# buyout_account_postgres_upload

Публикует последнюю партицию витрины
`gold.feature_platform_buyout_online_account_features` (Iceberg,
`layers/gold/account_id/buyout_online_account_features/v1`) в PostgreSQL
сервиса невыкупов. Вторая PostgreSQL-выгрузка в репозитории; своего
Iceberg-марта не заводит и не нуждается в нём — из витрины публикуется
`account_id` плюс одна фича, одна дата и четыре константы.

- **DAG id**: `feature-platform.upload.buyout_account_postgres_upload`
- **Расписание**: `0 7 * * *` UTC
- **Ожидание источника**: `ExternalTaskSensor` на `external_task_id="dq"`
  DAG'а `feature-platform.layers.gold.account_id.buyout_online_account_features`
  (расписание `0 6 * * *`), `execution_delta=60` минут — витрина и выгрузка
  идут по одному и тому же `data_interval`, но с часовым сдвигом расписаний.
- **Владелец/алерты**: `team:buyer`, `severity=P3`, `oncall_webhook_conn_id=team:buyer`.
- **Connection**: `postgres_non_buyout_service_connect`.
- **База / целевая таблица**: `mlgrowth` / `public.account_buyout_features`.

## Код публикации — общий со sku-выгрузкой

У этой выгрузки нет своего `job/`: `dag.py` импортирует
`upload/buyout_sku_postgres_upload/v1/job/upload_postgres.py` напрямую по
пути (тем же `importlib`-приёмом, каким Kafka-выгрузки переиспользуют
`features_service_upload`), а не копирует код. `publish()` там же обзавёлся
двумя опциональными параметрами — `column_map` и `constants` — специально
ради этой выгрузки; у sku-выгрузки они не заданы, и её поведение не
изменилось.

## Режим записи

Публикация идёт через промежуточный стейдж, а не пишет в целевую таблицу
напрямую — тот же режим, что и у buyout_sku_postgres_upload:

1. `CREATE TEMP TABLE stage_account_buyout_features (...)`. Не `LIKE
   public.account_buyout_features` — колонки источника называются иначе,
   чем в целевой таблице (`orders_created_prev_365d` vs `orders_count`,
   `last_order_date_win` vs `last_order_date`), поэтому стейдж создаётся с
   исходными именами и типами колонок, взятыми из Arrow-схемы Iceberg-скана.
2. `COPY ... FROM STDIN` льёт батчи скана (`account_id`,
   `orders_created_prev_365d`, `last_order_date_win`, `updated_at`) в стейдж.
3. Проверка объёма — по числу строк, ушедших в стейдж (см. ниже).
4. `TRUNCATE TABLE public.account_buyout_features`.
5. Явный `INSERT INTO ... (colonки) SELECT ... FROM stage` — не `SELECT *`
   (см. ниже).
6. Один `commit()` на всё.

Скан Iceberg и кодирование CSV — самая долгая часть публикации, и она пишет
только во временную таблицу, не в target. ACCESS EXCLUSIVE на
`public.account_buyout_features` берётся лишь на шагах 4–5 — именно ради
этого партиция сначала стадируется: читатели сервиса невыкупов не
блокируются на время скана и кодирования, а не только на время самой
записи.

## Отображение колонок

| Целевая (`public.account_buyout_features`) | Источник (стейдж)                          |
|-----------------------------------------------|---------------------------------------------|
| `account_id`                                  | `account_id`                                 |
| `orders_count`                                | `orders_created_prev_365d`                   |
| `last_order_date`                             | `CAST(last_order_date_win AS timestamptz)`   |
| `updated_at`                                  | `updated_at` (момент запуска задачи, UTC)    |
| `no_block`                                    | константа `true`                             |
| `segment_description`                         | константа `''`                               |
| `text_description_ru`                         | константа `''`                               |
| `text_description_uz`                         | константа `''`                               |

Это `sink.column_map` и `sink.constants` конфига — единственный источник
правды об отображении, здесь оно продублировано только для чтения. Явный
список колонок в `INSERT` строится из этих двух блоков в `job.build_insert_
select()`; `SELECT *` для этой выгрузки не годится ровно потому, что имена
колонок разошлись.

## Почему константы — SQL-литералы, а не CSV

`no_block`, `segment_description`, `text_description_ru`, `text_description_
uz` не читаются из витрины и не идут через `COPY`: они прописаны прямо в
`SELECT` как SQL-литералы (`true`, `''`). Если бы то же самое шло через CSV
(будто это ещё одна колонка стейджа), `csv.writer` записал бы пустую строку
как неквотированное пустое поле, а `COPY ... FORMAT csv` прочитал бы такое
поле как SQL `NULL`, а не как `''` — пустая строка незаметно превратилась бы
в NULL. Поэтому три текстовых константы и `no_block` формируются в SQL, а
не участвуют в стейдже вовсе.

## `last_order_date` — из 182-дневного окна

`last_order_date_win` — не абсолютная дата последнего заказа аккаунта, а
дата последнего заказа **в пределах 182-дневного окна** витрины-источника
(`layers/gold/account_id/buyout_online_account_features/v1`). На партиции
2026-09-09 у обеих участвующих в выгрузке колонок было измерено **ноль**
NULL (`last_order_date_win` и `orders_created_prev_365d`), то есть на
практике граница окна ни разу не обрезала значение ни для одного аккаунта в
таблице (в таблице вообще только аккаунты с хотя бы одним заказом за 182
дня до даты партиции — см. `create_table.sql` источника). Строгой гарантии
на будущее это не даёт: если у витрины появится NULL в `last_order_date_win`,
`last_order_date` в PostgreSQL станет NULL вместе с ним — приведение
`CAST(... AS timestamptz)` NULL пропускает, не подменяет.

## Порог объёма (`min_rows`)

Как и у sku-выгрузки, `min_rows` — не отдельная константа в этом DAG'е, а
значение `min_rows` теста `row_count_min` из блока `dq` конфига
витрины-источника
(`layers/gold/account_id/buyout_online_account_features/v1/config.yaml`).
На момент написания — `4000000`: измеренный объём партиции 2026-09-09 —
4 316 684 строки, днём раньше — 4 312 654; порог оставляет запас ниже обоих
значений, оставаясь тестом на severity `warn` (порог только читается
выгрузкой, её собственная severity не меняется). Партиция короче порога или
пустая — `RuntimeError` до `TRUNCATE`: транзакция откатывается, стейдж
уходит вместе с ней (`ON COMMIT DROP`), target вообще не тронут.

## Схема целевой таблицы

DDL таблицы `public.account_buyout_features` в БД `mlgrowth` живёт **вне этого
репозитория** — таблицу заводит пользователь вручную в PostgreSQL сервиса
невыкупов, репозиторий её не создаёт и не мигрирует. Подтверждённые 8
колонок (порядок как в задании на создание таблицы):

```sql
account_id bigint PRIMARY KEY, orders_count int, no_block boolean,
segment_description text, text_description_ru text, text_description_uz text,
last_order_date timestamptz, updated_at timestamptz
```

Изменение колонок на стороне PostgreSQL не отражается в репозитории
автоматически, и наоборот — рассинхронизация `sink.column_map`/`constants`
со схемой таблицы уронит `INSERT` с ошибкой числа или типа колонок.
