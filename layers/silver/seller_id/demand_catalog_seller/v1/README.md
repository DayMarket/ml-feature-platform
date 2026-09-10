# Полный текущий каталог продавцов

## Выход и источники

Таблица `iceberg.silver.feature_platform_demand_catalog_seller`.
Путь `layers/silver/seller_id/demand_catalog_seller/v1`, ключ `(date,seller_id)`.
12 полей: ID/master, статусы связи, is_1p, регистрация и capture metadata.
Источник — весь доступный CH `marts.sellers_info`, без фильтра по текущим SKU,
активности, заказам или наличию. ФИО, контакты, реквизиты и готовые окна источника
не читаются. Регистрация нормализуется из Asia/Tashkent в UTC, NULL сохраняется.

Исходный master сохраняется отдельно. Непустой после strip master даёт matched;
пустой у существующей строки — unmatched и seller_id строкой. NULL — unavailable,
не доказанное отсутствие связи. Повтор seller_id и коллизия fallback с настоящим
master блокируют подготовку, без max/first. Несколько seller одного master допустимы.
Источник не содержит seller факта — consumer не теряет факт через INNER JOIN и
не объявляет его unmatched. Нулевая/unknown принадлежность EOD здесь не исправляется.

Дата — фактический захват Asia/Tashkent, не историческая отсечка модели. Полная
атомарная замена текущего каталога, а не накопление ежедневных копий. Обычные Iceberg
snapshots, без protected tags; immutable training package сохраняется моделью отдельно.
SKU/tree должны наследовать exact проверенный seller snapshot/catalog_version.

## Проверки и статус

Подготовлены migration/config, source SQL и чистый Arrow adapter. Он требует независимый
expected_source_rows, проверяет полный состав, ключи, signed BIGINT, исходные типы,
версии и master-коллизии; сохраняет raw master/NULL/UTC. Count не заменяет контроль
содержимого и готовности источника. Unavailable может присутствовать в подготовленной
порции, но запрещён DQ. Adapter не пишет, не подтверждает readiness и не создаёт runs.
Подготовлен writer.py: preflight миграции/схемы/identity partition, повторная проверка
master-полей из raw, единого capture/version и source count. Полная замена через одну
Iceberg transaction, обязательный source callback после подготовки файлов перед commit.
Отказ callback или конфликт конкурентной записи не заменяет прежний срез. Все строки
записанного snapshot сравниваются с исходным batch; смена current во время read-back
не возвращает receipt. Штатный DQ фильтрует единый capture по точному `ingested_at`;
written не означает passed. Feature statistics считаются по тому же capture.
Повтор после потери acknowledgement перезаписывает полный срез без дублей; старые
даты и исчезнувшие seller удаляются из current. Tags не создаются.
Подготовлен runtime.py: сначала target и CH LIMIT 0, затем независимый source
count/unique/ID audit, порционное чтение четырёх полей и подготовка полного каталога.
Callback повторно читает те же поля и сравнивает все значения, не только count.
Два полных source чтения и три aggregate audit scans на захват — цена проверки
согласованности; это не snapshot isolation или блокировка CH. Потоки закрываются при
обрыве, подмене/смене типов, повторе ключа и несовпадении второго чтения.
Смена target metadata за время extraction блокирует writer до новой транзакции.
Лимиты: 100000 строк/64 MiB на порцию, raw и prepared Arrow-каталог каждый не более
512 MiB, без усечения. Это лимиты буферов, не общий предел Python heap; pod остаётся
2 CPU/8 GiB. Receipt содержит source schema/content hash/audit и время проверки.
Историческая дата не является аргументом capture; runtime сохраняет текущее время.
Подготовлен orchestration.py: CH clickhouse_dwh_team_logistics с use_numpy=False,
Trino Connections из DQ/stats config и общий Hive/S3 loader штатных DQ results.
До extraction сверяются полные схемы target/DQ/stats с DDL, типы/nullable Iceberg,
Trino metadata через LIMIT 0 и native source types. Неизвестная service DDL/схема,
нечитаемый input или лишние exclude_columns блокируют чтение источника.
Собственные CH/Trino клиенты и курсоры закрываются при ошибке/успехе; переданные
извне клиенты закрывает caller. Owner DAG подключён локально (ниже).

Источник — ReplicatedMergeTree с seller_id в sorting key, произвольный FINAL
или dedup агрегат не добавлен. Эквивалентного полного seller-master каталога в новой
FP-ветке не найдено; product_metadata не содержит эту связь.

## Оркестрация

Один owner DAG `feature-platform.layers.silver.seller_id.demand_catalog_seller` с `max_active_runs=1` заменяет полный текущий каталог. Scheduled run идёт ежедневно в `04:00 UTC`; ручной полный refresh запускается в том же DAG с `{"mode":"manual"}`.

Отдельного history/full-history DAG нет. Запись, DQ и feature statistics образуют один сериализованный интервал; ручной запуск не содержит исторических дат, потому что каталог является текущим reference snapshot.
