"""ClickHouse SQL источников текущего SKU-каталога."""

LEVELS = ("market", "l1", "l2", "l3", "l4", "l5", "leaf")
RAW_LEVELS = tuple(f"raw_l{level}_category_id" for level in range(1, 7))


def sku_query(config: dict) -> str:
    return f"""SELECT
    toInt64(id) AS sku_id,
    toInt64(product_id) AS product_id,
    toInt64(category_id) AS category_id,
    toInt64(seller_id) AS seller_id,
    toInt64(shop_id) AS shop_id,
    toDateTime64(toTimeZone(created_at, 'UTC'), 6, 'UTC') AS sku_created_at,
    status AS sku_status
FROM {config['source']['sku']}
SETTINGS max_threads = 1, max_execution_time = 1200"""


def category_query(config: dict) -> str:
    """Путь market → L1..L5 → leaf: нулевой уровень наследует предыдущий, leaf — category_id.

    Категория без L1 получает статус missing и пустой путь.
    """
    raw = ",\n    ".join(f"toInt64(l{n}_category) AS raw_l{n}_category_id" for n in range(1, 7))
    return f"""SELECT
    category_id,
    {', '.join(RAW_LEVELS)},
    l1_title,
    leaf_title,
    if(valid, 'valid', 'missing') AS category_path_status,
    if(valid, 'market', NULL) AS market,
    if(valid, concat('l1:', toString(p1)), NULL) AS l1,
    if(valid, concat('l2:', toString(p2)), NULL) AS l2,
    if(valid, concat('l3:', toString(p3)), NULL) AS l3,
    if(valid, concat('l4:', toString(p4)), NULL) AS l4,
    if(valid, concat('l5:', toString(p5)), NULL) AS l5,
    if(valid, concat('leaf:', toString(category_id)), NULL) AS leaf
FROM (
    SELECT
        toInt64(id) AS category_id,
        {raw},
        l1_category_title AS l1_title,
        title_ru AS leaf_title,
        raw_l1_category_id > 0 AS valid,
        raw_l1_category_id AS p1,
        if(raw_l2_category_id = 0, p1, raw_l2_category_id) AS p2,
        if(raw_l3_category_id = 0, p2, raw_l3_category_id) AS p3,
        if(raw_l4_category_id = 0, p3, raw_l4_category_id) AS p4,
        if(raw_l5_category_id = 0, p4, raw_l5_category_id) AS p5
    FROM {config['source']['category']}
)
SETTINGS max_threads = 1, max_execution_time = 600"""


def golden_query(config: dict) -> str:
    """Последнее состояние merge на golden UUID (sorting key источника не задаёт версию)."""
    return f"""SELECT
    toString(golden_sku_id) AS golden_sku_id,
    toUInt8(latest.1) AS is_merged,
    toString(latest.2) AS merged_into
FROM (
    SELECT golden_sku_id, argMax(tuple(is_merged, merged_into), updated_at) AS latest
    FROM {config['source']['golden']}
    GROUP BY golden_sku_id
)
SETTINGS max_threads = 1, max_execution_time = 600"""


def links_query(config: dict) -> str:
    """Активные связи Uzum SKU → golden через словарь meta SKU; orphan-связи отбрасываются."""
    meta = config["source"]["meta_sku"]
    return f"""SELECT sku_id, golden_sku_id
FROM (
    SELECT
        toInt64(toUInt64OrZero(dictGetString('{meta}', 'source_sku_id', meta_sku_id))) AS sku_id,
        toString(golden_sku_id) AS golden_sku_id
    FROM {config['source']['golden_links']}
    WHERE deleted_at IS NULL
      AND dictHas('{meta}', meta_sku_id)
      AND dictGetString('{meta}', 'source', meta_sku_id) = 'uzum'
)
WHERE sku_id > 0
SETTINGS max_threads = 1, max_execution_time = 1200"""
