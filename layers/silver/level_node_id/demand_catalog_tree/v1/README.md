# Дерево текущего SKU-каталога

## Выход и вход

Таблица `iceberg.silver.feature_platform_demand_catalog_tree`.
Путь `layers/silver/level_node_id/demand_catalog_tree/v1`, ключ `(date,level,node_id)`.
11 полей: уровень, узел, родитель, passthrough и capture metadata. Единственный
вход — exact проверенный snapshot `iceberg.silver.feature_platform_demand_catalog_sku`,
владелец `layers/silver/sku_id/demand_catalog_sku/v1`. Второго чтения DWH/MDM нет.

market → l1 → l2 → l3 → l4 → l5 → leaf. Node ID market или level:category_id.
У корня parent=NULL; каждый другой parent — предыдущий уровень того же пути.
Passthrough означает равенство category ID с непосредственным родителем, не только
сырой ноль уровня. У market/L1 false. Одинаковые пути сворачиваются, несколько
родителей одного узла блокируют построение. Нового unknown-узла нет.

Date/catalog_version наследуются из SKU capture; ingested_at — текущее время
материализации UTC. Весь tree заменяется атомарно, без накопления ежедневных копий.
Обычные snapshots; table UUID/schema и exact SKU snapshot связываются manifest.
Immutable training package отдельно; protected tags нет.

## Реализация и ограничения

Подготовлены DDL/config и чистый preparation.py. Функция принимает порции проекции
SKU-каталога с нормализованными путями, проверяет ключи/типы/полный count/общую версию,
canonical node IDs и однозначность родителей. Missing-category строки входят в source
count, но не дерево. Audit содержит valid/missing SKU counts, узлы по уровням и
edges_sha1 отсортированных node>parent. Сами missing SKU доступны в исходном exact
каталоге; покрытие продаж считается модельным потребителем, не этой витриной.
Функция не читает DWH/Iceberg, не заменяет проверку upstream DQ и не пишет данные.
Подготовлен writer.py: проверка схемы/metadata выбранного source, canonical nodes,
уровней/родителей/passthrough и наличия полного пути до leaf у каждого узла.
Атомарно заменяет весь срез, включая старые даты/исчезнувшие узлы. Требует callback
проверки exact input непосредственно перед commit; отказ сохраняет прежний snapshot.
Guard metadata target защищает захват от чужой записи во время extraction. Полный
read-back сравнивает все поля, не только count; конфликт current не возвращает written.
Повтор после сбоя read-back снова заменяет полный срез без дублей и protected tags.
Receipt передаётся штатному DQ точного `ingested_at`; feature statistics используют
тот же capture. Сам writer не доказывает соответствие
узлов исходному SKU: это обязанность полного reader/preparation и exact DQ binding.
Теперь inputs.py связывает passed DQ точного SKU dag/run с UUID/snapshot и его
schema_id, сверяет все 38 полей с миграцией владельца. Более новый current не мешает
читать доступный выбранный snapshot; схема current не подставляется к старому input.
reader.py читает FOR VERSION AS OF с ORDER BY sku_id без фильтра даты/лимита каталога:
проекцию 14 полей порциями до 100000 строк/64 MiB. Проверяет native Trino metadata,
полный count, ключи и date/version/manifest/capture каждой строки, закрывает cursor
при ошибке/раннем выходе. Строки missing category не теряются до source count.
runtime.py проверяет схемы input/output/DQ/stats до скана и повторно получает DQ
payload того же run/доступность snapshot перед commit. Receipt содержит source
receipt/reference/schema ID/query hash и tree audit. Нет default True source callback.
orchestration.py использует общий Hive/S3 loader и согласованный trino_search для
source/DQ/stats; точный XCom task=dq, include_prior_dates=False. Переданные соединения
остаются caller, созданные bridge закрываются при ошибках. Owner DAG подключён локально.
Эквивалента в FP не найдено: product_metadata — product-grain, category features —
показатели заказов, не ребра единственного согласованного дерева.

## Оркестрация и DQ

Один owner DAG `feature-platform.layers.silver.level_node_id.demand_catalog_tree` с `max_active_runs=1` заменяет полный текущий справочник дерева. Scheduled run идёт ежедневно в `04:00 UTC` и ждёт точный DQ SKU catalog. Ручной полный refresh запускается в этом же DAG с `mode=manual` и точной `reference` на SKU catalog.

Отдельного history/full-history DAG нет. После атомарной записи выполняются DQ и feature statistics по точному writer receipt.
