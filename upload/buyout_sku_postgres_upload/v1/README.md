# buyout_sku_postgres_upload

Публикует последнюю партицию витрины `gold.feature_platform_sku_buyout_features`
(Iceberg, `layers/gold/sku_id/sku_buyout_features/v1`) в PostgreSQL сервиса
невыкупов — первая в репозитории выгрузка не в Kafka.

- **DAG id**: `feature-platform.upload.buyout_sku_postgres_upload`
- **Расписание**: `0 7 * * *` UTC
- **Ожидание источника**: `ExternalTaskSensor` на `external_task_id="dq"` DAG'а
  `feature-platform.layers.gold.sku_id.sku_buyout_features`, `execution_delta=0`
  минут — та же партиция, что уже прошла DQ витрины.
- **Владелец/алерты**: `team:buyer`, `severity=P3`, `oncall_webhook_conn_id=team:buyer`.
- **Connection**: `postgres_non_buyout_service_connect`.
- **База / целевая таблица**: `mlgrowth` / `public.sku_buyout_features`.

## Режим записи

Публикация идёт через промежуточный стейдж, а не пишет в целевую таблицу
напрямую. Порядок внутри одной транзакции (`connection.autocommit = False`,
единый `commit()` в конце):

1. `CREATE TEMP TABLE stage_sku_buyout_features (LIKE public.sku_buyout_features)`.
2. `COPY ... FROM STDIN` льёт все батчи Iceberg-скана в стейдж.
3. Проверка объёма (см. ниже) — по числу строк, ушедших в стейдж.
4. `TRUNCATE TABLE public.sku_buyout_features`.
5. `INSERT INTO public.sku_buyout_features SELECT * FROM stage_sku_buyout_features`.
6. Один `commit()` на всё.

Скан Iceberg и кодирование CSV (шаг 2) — самая долгая часть публикации
(~10.5М строк), и она пишет только во временную таблицу, не в target.
ACCESS EXCLUSIVE на `public.sku_buyout_features` берётся лишь на шагах
4–5 (`TRUNCATE` + `INSERT ... SELECT`) — именно ради этого партиция сначала
стадируется: читатели сервиса невыкупов не блокируются на время скана и
кодирования, а не только на время самой записи. Упрощение обратно до прямого
`COPY` в target вернёт блокировку читателей на всё время скана.

Проверка объёма (см. `publish` в `job/upload_postgres.py`) выполняется
**до** `TRUNCATE` — по строкам, уже попавшим в стейдж. Если партиция пуста
(`written == 0`) или короче порога `min_rows`, задача поднимает `RuntimeError`
до `TRUNCATE`: транзакция откатывается (`rollback()`), стейдж уходит вместе с
ней (`ON COMMIT DROP`), а target вообще не тронут — короткая или пустая
партиция не доходит до целевой таблицы, а не откатывается постфактум. Порог
`min_rows` — не отдельная константа в этом DAG'е, а значение `min_rows` теста
`row_count_min` из блока `dq` конфига витрины-источника
(`layers/gold/sku_id/sku_buyout_features/v1/config.yaml`), на момент
написания — `9000000`. Задача в любом из этих случаев падает и уходит в
алерт дежурному.

## Схема целевой таблицы

DDL таблицы `public.sku_buyout_features` в БД `mlgrowth` живёт **вне этого репозитория** —
таблицу завели вручную в PostgreSQL сервиса невыкупов до начала этой работы.
Список колонок ниже подтверждён через Trino на момент ввода в эксплуатацию
DAG'а `feature-platform.upload.buyout_sku_postgres_upload` и должен
поддерживаться **вручную синхронно** со схемой: изменение колонок на стороне
PostgreSQL не отражается в репозитории автоматически, и наоборот — добавление
новой колонки в `features` конфига без соответствующей колонки в PostgreSQL
уронит `COPY` с ошибкой числа колонок.

Подтверждённые 21 колонка в PostgreSQL (порядок как в `CREATE TABLE`):

```
sku_id bigint, product_id bigint, seller_id bigint,
l1_category integer, l2_category integer, l3_category integer,
l4_category integer, l5_category integer,
type varchar, commission numeric, cost_price numeric,
predicted_dimensional_group varchar, is_not_block boolean,
sku_buyout double, product_buyout double, category_buyout double,
shop_buyout double, category_no_show double,
sku_n_delivered integer, product_n_delivered integer,
updated_at timestamptz
```

Важные расхождения с исходной Iceberg-витриной:

- **Нет `category_id`.** Iceberg-мart несёт `category_id` как ключ джойна с
  `dict.category`, но сервис невыкупов эту колонку не потребляет — она
  сознательно исключена из `features` конфига и не публикуется.
- **Нет `date`.** PostgreSQL-таблица не партиционирована по дате: `updated_at`
  (момент запуска задачи, UTC) — единственный маркер версии данных. Выгрузка
  всегда полностью заменяет содержимое таблицы одной партицией витрины —
  предыдущая дата не хранится и не нужна.
- **`sku_n_delivered` и `product_n_delivered` — `integer` в PostgreSQL, но
  `BIGINT` в Iceberg.** Замеренный максимум на партиции 2026-09-09 — 234 462,
  то есть запас до предела `int4` (2 147 483 647) — четыре порядка величины.
  Явного приведения/проверки диапазона в коде выгрузки нет: при переполнении
  `COPY` обязан упасть с ошибкой PostgreSQL (`integer out of range`), а не
  молча усечь значение — так `psycopg2`/PostgreSQL и ведут себя на COPY с
  текстовым представлением числа, выходящим за диапазon целевого типа.
  Наблюдать за ростом этих значений и пересматривать типы в PostgreSQL нужно
  заранее, до фактического переполнения.
- **`commission` и `cost_price` в Trino отображаются как `varchar`.** Это
  особенность рендеринга Trino для PostgreSQL `numeric` без явных
  precision/scale, а не текстовая колонка. `COPY` пишет десятичные литералы
  строкой, PostgreSQL сам приводит их к `numeric` при вставке.

## NULL-значения

Пропуск в признаке выкупаемости (`sku_buyout`, `product_buyout`,
`category_buyout`, `shop_buyout`, `category_no_show` и др.) — законное
значение, а не отсутствие данных, которое можно заменить нулём. CSV,
построенный `batch_to_csv`, пишет для `None` пустое поле, что `COPY ... WITH
(FORMAT csv)` интерпретирует как SQL `NULL`. Нулевое значение выкупаемости и
пропуск выкупаемости — разные вещи, и код нигде их не подменяет друг другом.

## Рантайм-зависимости

Задача использует `PostgresHook` (`apache-airflow-providers-postgres`) и
`pyiceberg`'s `DataScan.to_arrow_batch_reader` — ни то, ни другое больше
нигде в репозитории не используется, и их наличие в образе
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2` не подтверждено ни одним
существующим DAG'ом. Проверка через `docker run` (см. `AGENTS.md`, раздел
«Custom Image Workflow») на момент написания **не выполнена**: локальный
Docker daemon недоступен в среде разработки. Это открытый вопрос —
до подтверждения наличия обеих зависимостей в образе DAG нельзя считать
готовым к раскатке.
