# Текущий SKU-каталог

## Выход и источники

Таблица `iceberg.silver.feature_platform_demand_catalog_sku`, ключ `(date,sku_id)`.
Путь `layers/silver/sku_id/demand_catalog_sku/v1`. 38 полей ранее согласованного
контракта: SKU/card/category/current seller/shop/status/создание, raw и нормализованный
путь, готовые golden/master-связи и capture metadata. Дневных окон, активности,
канала по истории, эпизодов OOS, X/y и скорингов здесь нет.

Внешние источники: dict.sku LEFT JOIN dict.category по category_id; MDM-связи
matching.meta_sku_id_dict → matching.mdm_golden_meta_links → matching.mdm_golden_sku.
Master/is_1p/регистрация — только exact проверенный snapshot полного catalog_seller
из `layers/silver/seller_id/demand_catalog_seller/v1`, не повторное marts.sellers_info.
Catalog_version общий с seller/tree; table UUID/schema и captures закрепляются manifest.

Raw L1–L6 сохраняют нули и NULL отдельно. Нормализованный путь — market, L1–L5 с
протяжкой нулей от предыдущего уровня, leaf из category_id. Missing-category SKU
не удаляются, путь NULL; конфликт родителя блокирует ready. Golden/master только
готовые связи. Unmatched — доказанное отсутствие; conflict не выбирает одну golden и
остаётся самостоятельным `unit_id=s:<sku_id>`. Unavailable блокирует запись.
NULL is_1p не FALSE, created_at не first_observed_alive_date.

Дата capture — фактическая Asia/Tashkent, timestamps UTC. Полная атомарная замена
текущего среза, включая исчезнувшие SKU; manual не выдумывает прошлый каталог.
Обычные snapshots без protected tags, обучение сохраняет immutable package отдельно.

## Проверка источников и статус

Read-only 2026-09-09: dict.sku — 10470760 уникальных SKU, недопустимых ID и missing
seller_id в этом срезе нет. dict.category — 6906 уникальных строк, missing L1=0.
Оба источника — Dictionary; DateTime created_at использует Asia/Tashkent сервера.
MDM golden — ReplicatedReplacingMergeTree с ORDER BY (category_id,golden_sku_id).
После FINAL — 356787 уникальных golden-ID, 15097 merged, нулевых UUID/merge targets
и неизвестных merge flags нет. Это не проверка всей цепочки соответствий SKU:
нужна проверка source readiness полного production-прогона; локальный runtime ниже
проверяет consistency, кардинальности links и merge-цепочки.
Нельзя перенести legacy drop_duplicates(sku_id) или молча выбрать первый golden.

Эквивалента не найдено: product_metadata имеет product-grain и другую категорийную
семантику, не полный SKU/golden/master каталог. Старые схемы сверены с архивом
отменённой поставки; поля сохранены, protected tags не перенесены.
Подготовлены DDL/config/preparation/writer/exact seller binding, полный source loader/runtime/Connections и owner DAG.
Добавлено чистое golden_graph.py: итеративный проход полного захваченного графа
до terminal golden для каждого ID, с проверкой независимого count/UUID/merge flags,
дублей, отсутствующих целей и циклов во всём графе. Path compression исключает
повторный обход общей части цепочек; лимита одним переходом/рекурсии нет.
Audit содержит число terminal/merged/multi-hop узлов и максимальную глубину.
Добавлены category_paths.py/golden_links.py: полный category count/raw-типы,
нормализация L2–L5 с сохранением raw L1–L6, leaf=category_id. Неоднозначные parent
помечают все затронутые категории conflict, без выбора первого; normalized paths
у них NULL. Отсутствующий L1 остаётся missing. SKU preparation блокирует использованные
conflict до commit, DQ контракт также запрещает их готовому срезу.

query.py даёт отдельные SKU/category/golden/active_links captures и LIMIT 0 для
проверки native metadata. ID SKU/category не кастуются в signed до проверки диапазона.
У golden используется FINAL. Активные links читаются без INNER JOIN: dictHas сохраняет
флаг meta_present и NULL identity orphan-связи. Links.source — provenance (manual,
review, dedup_merge и др.), marketplace='uzum' отбирается по meta.source. Семантика
сверена с dbt commerce/golden_sku_mapping.sql CTE verified/nasz_paired_skus.sql;
их ML extended matching/ранжирование не переносится. Все четыре LIMIT 0 прошли CH.

prepare_active_links проверяет весь capture/count до отбора Uzum. Orphan без meta
невозможно приписать marketplace или SKU: он пропускается, но явно учитывается как
`golden_links.orphan_meta_link_rows` в source audit. resolve_sku_links считает
повторную идентичную связь в audit, но не
выбирает одну из разных: после merge resolution один terminal даёт matched, разные —
conflict с `golden_sku_id=NULL` и `unit_id=s:<sku_id>`, отсутствующий golden —
блокирующий unavailable. Отсутствие SKU в этом индексе само не доказывает unmatched.

Orphan не становится `unmatched` и не получает fallback; полный raw capture остаётся
проверяемым до и после подготовки.

## Подготовка и атомарная запись SKU

job/preparation.py принимает полный raw SKU Arrow capture из семи полей, raw category,
golden и active links captures с независимыми counts и полный seller snapshot.
Все 38 полей собираются через колоночные index/take: нет Python-списка миллионов SKU
строк. Нули/NULL исходных полей сохраняются; missing category оставляет NULL-путь,
но не удаляет SKU. Unmatched MDM появляется только после проверки полного capture.
Использованные category/seller conflict и golden unavailable блокируют запись;
golden conflict остаётся самостоятельным SKU. Missing seller не становится unmatched.
Master/raw/status seller повторно сверяются, unknown is_1p сохраняется.

job/inputs.py проверяет passed DQ точного seller owner run, writer receipt, UUID,
snapshot ID и схему выбранной версии по DDL владельца. Новый head/schema не является
fallback. Подготовка сверяет полный seller payload/count/capture с закреплённым receipt;
получение exact XCom и чтение источников теперь подключены в runtime/Connections bridge.

job/writer.py выполняет preflight существующей таблицы/identity date, подготовку,
атомарную полную замену и полный read-back. Исчезнувшие SKU и старые даты удаляются
из текущего среза. verify_source обязателен и непосредственно перед commit повторно
проверяет source captures/exact seller DQ; callback теперь реализован в runtime.
Проверка смены target metadata и optimistic commit запрещают потерю чужой записи.
Receipt written передаётся DQ точного `ingested_at`, но не означает passed.
Feature statistics считаются по тому же capture. Read-back ошибка
после commit не откатывает snapshot и не выдаёт успешный receipt.

Конфигурация owner выделяет pod 2 CPU/32 GiB. Arrow output_bytes в audit — размер
буферов результата, не peak RSS. Source SQL может
выполняться в CH без новых промежуточных таблиц; seller берётся из exact FP snapshot,
не из повторного marts.sellers_info.

Дополнительный audit merge-графа: missing target/self-loop=0, но у 2861 merged
строки target сам merged. Утверждён проход до конечного golden в одном захваченном
графе; циклы, missing targets и повтор golden-ID блокируют запись. Один переход
не гарантирует конечный golden. Dict meta для source=uzum содержит
10954174 уникальных source_sku_id с допустимым числовым ID; это не вся проверка JOIN.

Схемы SKU/tree сохраняют прежние имена, типы и nullable.

## Полный загрузчик

source_reader.py читает четыре полных CH captures порциями 100000 строк/64 MiB.
Native metadata проверяется через LIMIT 0, counts — независимыми агрегатами.
У dict.sku подтверждены id UInt64, product/category Int64, nullable seller/shop Int64;
UUID не заменяется неявной строковой конверсией, даты приходят явно UTC. Orphan meta
блокирует загрузку до больших captures. Весь raw результат собирается в Arrow;
порог порции не является ограничением общей памяти процесса.

seller_reader.py читает все 12 полей exact snapshot через Trino, без фильтра SKU,
со строгими native типами, count/порядком/лимитами и закрытием cursor при ошибках.
runtime.py проверяет полные схемы input/output/DQ/stats до большого скана и связывает
writer с повторной проверкой: все raw captures читаются дважды и сравниваются целиком,
counts проверяются до/между/после. Неизменный count не скрывает изменённый статус,
название категории или MDM provenance. Это не snapshot isolation ClickHouse.
Seller DQ/UUID/snapshot/schema перепроверяются до и после повторного CH чтения.

orchestration.execute_capture использует clickhouse_dwh_team_logistics/use_numpy=False,
trino_search и общий Hive/S3 catalog из штатного DQ loader. Exact XCom читает task=dq
только указанного seller dag/run, include_prior_dates=False. Собственные clients
закрываются при ошибке, переданные caller не закрываются. Owner DAG подключён локально.

## Оркестрация и DQ

Полная замена не накапливает историю захватов в текущем содержимом таблицы.
Поэтому `dq.warmup_days` должен быть `0`; ненулевое значение отклоняется до открытия подключений.

Один owner DAG `feature-platform.layers.silver.sku_id.demand_catalog_sku` с `max_active_runs=1` заменяет полный текущий SKU-каталог. Scheduled run идёт ежедневно в `04:00 UTC` и ждёт точный DQ seller catalog. Ручной полный refresh запускается в этом же DAG:

```json
{
  "mode": "manual",
  "reference": {
    "dag_id": "feature-platform.layers.silver.seller_id.demand_catalog_seller",
    "run_id": "<exact-run-id>",
    "logical_date": "<exact-logical-date>"
  }
}
```

Отдельного history/full-history DAG нет; upstream не запускается скрыто и `latest` не используется. После атомарной записи выполняются DQ и feature statistics по точному receipt.
