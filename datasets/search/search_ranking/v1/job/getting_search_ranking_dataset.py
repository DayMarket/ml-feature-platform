from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _parse_airflow_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for candidate in (normalized, normalized.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(
        "Unsupported partition timestamp format. "
        f"Expected Airflow ISO timestamp or YYYY-MM-DD HH:MM:SS, got {value!r}"
    )


def _collection_date(partition_start: str) -> date:
    return _parse_airflow_datetime(partition_start).date()


def build_search_ranking_dataset(
    spark: SparkSession,
    partition_start: str,
):
    collection_date = _collection_date(partition_start)
    event_date = collection_date - timedelta(days=14)

    return spark.sql(
        f"""
WITH params AS (
    SELECT
        DATE '{collection_date.isoformat()}' AS collection_date,
        DATE '{event_date.isoformat()}' AS event_date
),
attributed_orders AS (
    SELECT DISTINCT
        last_search_session_id,
        install_id,
        order_id,
        order_item_id,
        trim(lower(query)) AS query
    FROM iceberg.silver.order_items_attribution
    CROSS JOIN params p
    WHERE
        query != ''
        AND widget_space_name IN (
            'SHOP_SEARCH_RESULTS',
            'COLLECTION_SEARCH_RESULTS',
            'SEARCH',
            'SEARCH_RESULTS'
        )
        AND event_received_at >= p.event_date
        AND event_received_at < DATE_ADD(p.event_date, 1)
),
search_attributed_orders AS (
    SELECT DISTINCT
        last_search_session_id,
        install_id,
        order_id,
        order_item_id,
        trim(lower(query)) AS query
    FROM iceberg.silver.order_items_attribution
    CROSS JOIN params p
    WHERE
        -- Метка новой атрибуции берется как есть: ни widget_space_name, ни
        -- is_full_catpred, ни query != '' здесь не фильтруют - has_search_attr
        -- сам по себе отвечает на вопрос "заказ пришел из поиска". Из фильтров
        -- остается только окно партиции.
        --
        -- Цена решения, замеренная на event_received_at = 2026-09-15 по ключу
        -- агрегата: 40 230 ключей с прежними фильтрами против 138 665 без них.
        -- Весь рост дает снятый фильтр is_full_catpred, и это не catpred, а
        -- платформа: в атрибуции колонка varchar с тремя состояниями - ''
        -- (64 221 строка поиска), 'false' (41 305) и 'true' (5 619). Пустая
        -- строка - это Android, который флаг не логирует (63 211 из 64 221
        -- на ANDROID, iOS присылает 'false'), и предикат
        -- COALESCE(is_full_catpred, 'false') = 'false' ее отбрасывает.
        -- Настоящий catpred - только 'true': это запросы-категории вроде
        -- "косметика" или "зубная паста", около 5% строк поиска.
        -- Пустой query дает еще 671 строку, но они не матчатся с показами:
        -- join идет по query.
        --
        -- В sessions_raw ниже перекос обратный: там колонка boolean, Android
        -- присылает NULL (57.9M показов поиска из 70.5M за тот же день), и
        -- COALESCE(is_full_catpred, false) = false их сохраняет. То есть
        -- Android-показы в витрине есть, а Android-заказы ветка
        -- is_generated_order почти целиком теряет; has_search_attr без
        -- фильтров этого перекоса не наследует.
        has_search_attr
        AND query != ''
        AND event_received_at >= p.event_date
        AND event_received_at < DATE_ADD(p.event_date, 1)
),
order_items_enhanced AS (
    SELECT
        oi.order_item_id,
        oi.order_item_status,
        s.sku_group_id
    FROM iceberg.silver.order_items oi
    INNER JOIN iceberg.silver.sku s ON s.id = oi.sku_id
    CROSS JOIN params p
    WHERE
        -- Окно generated_at - граница скана order_items, общая для обеих
        -- веток. order_item_status фильтруется не здесь, а в ветке
        -- is_generated_order: has_search_attr не должен наследовать ее
        -- фильтры. На метку это не влияет - среди строк с has_search_attr =
        -- true статусов CREATED/NOT_CREATED за 2026-09-15 нет ни одной.
        oi.generated_at >= DATE_SUB(p.event_date, 15)
        AND oi.generated_at < DATE_ADD(p.event_date, 15)
),
orders AS (
    SELECT
        ao.last_search_session_id,
        ao.install_id,
        ao.query,
        oie.sku_group_id,
        1 AS is_generated_order
    FROM attributed_orders ao
    INNER JOIN order_items_enhanced oie
        ON oie.order_item_id = ao.order_item_id
    WHERE oie.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
    GROUP BY
        ao.last_search_session_id,
        ao.install_id,
        ao.query,
        oie.sku_group_id
),
search_attr_orders AS (
    SELECT
        sao.last_search_session_id,
        sao.install_id,
        sao.query,
        oie.sku_group_id,
        1 AS has_search_attr
    FROM search_attributed_orders sao
    INNER JOIN order_items_enhanced oie
        ON oie.order_item_id = sao.order_item_id
    GROUP BY
        sao.last_search_session_id,
        sao.install_id,
        sao.query,
        oie.sku_group_id
),
sessions_raw AS (
    SELECT
        p.collection_date,
        p.event_date,
        logged_at,
        received_at,
        install_id,
        session_id,
        CAST(sku_group_id AS BIGINT) AS sku_group_id,
        trim(lower(query)) AS query,
        CAST(`position` AS INT) AS `position`,
        widget_section_name,
        widget_space_name,
        -- bid_id есть в events плоской колонкой и совпадает с
        -- event_properties.event_parameters.bid_id на 100% показов
        -- (проверено на received_at = 2026-07-01, 89 163 741 строк),
        -- поэтому JSON здесь не парсится.
        CAST(bid_id AS BIGINT) AS bid_id,
        -- sell_price тоже есть плоской колонкой и совпадает со своим двойником
        -- в event_parameters на всех 85 980 262 непустых показах (проверено на
        -- received_at = 2026-09-15, 85 980 264 строки после фильтров ниже),
        -- включая совпадение NULL, поэтому JSON здесь не парсится.
        -- Не путать с seller_price ниже: это разные поля одного события, они
        -- расходятся на 8.4% показов, и подменять одно другим нельзя.
        CAST(
            get_json_object(event_properties, '$.event_parameters.sell_price') AS BIGINT
        ) AS sell_price,
        CAST(
            get_json_object(event_properties, '$.event_parameters.cpo_adv_version') AS BIGINT
        ) AS cpo_adv_version,
        -- seller_price не равен плоской колонке sell_price: значения расходятся
        -- на 12.3% показов, поэтому берется именно из event_parameters.
        CAST(
            get_json_object(event_properties, '$.event_parameters.seller_price') AS BIGINT
        ) AS seller_price,
        -- final_price логируется только когда он строго меньше seller_price
        -- (случай final_price = seller_price в источнике не встречается),
        -- поэтому COALESCE восстанавливает цену показа без потери информации.
        COALESCE(
            CAST(
                get_json_object(event_properties, '$.event_parameters.final_price') AS BIGINT
            ),
            CAST(
                get_json_object(event_properties, '$.event_parameters.seller_price') AS BIGINT
            ),
            CAST(
                get_json_object(event_properties, '$.event_parameters.full_price') AS BIGINT
            )
        ) AS final_price
    FROM iceberg.silver_b2c_clickstream.events
    CROSS JOIN params p
    WHERE
        logged_at >= DATE_SUB(p.event_date, 3)
        AND logged_at < DATE_ADD(p.event_date, 4)
        AND received_at >= p.event_date
        AND received_at < DATE_ADD(p.event_date, 1)
        AND event_type = 'PRODUCT_IMPRESSION'
        AND widget_space_name = 'SEARCH_RESULTS'
        AND widget_section_name = 'SEARCH_RESULTS'
        AND query IS NOT NULL
        AND trim(query) != ''
        AND COALESCE(is_full_catpred, false) = false
        -- Клиент, у которого не проинициализировался SDK, шлёт install_id = NULL
        -- вместе с session_id = нулевым sentinel'ом. Совпадение 100%: за
        -- received_at = 2026-08-11 таких показов 166, все с одного устройства, и
        -- event_properties.event_parameters.event_validation_info.validationStatus
        -- у них FAILED. Такая строка не атрибутируется к заказу (LEFT JOIN orders
        -- идёт по install_id и session_id) и ломает первичный ключ витрины.
        AND install_id IS NOT NULL
        AND session_id != '00000000-0000-0000-0000-000000000000'
),
sessions_with_duplicate_stats AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY event_date, install_id, session_id, query, `position`
        ) AS position_duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, install_id, session_id, query, `position`
            ORDER BY received_at ASC, logged_at ASC, sku_group_id ASC
        ) AS position_duplicate_rank
    FROM sessions_raw
),
sessions_deduplicated AS (
    SELECT
        collection_date,
        event_date,
        logged_at,
        received_at,
        install_id,
        session_id,
        sku_group_id,
        query,
        `position`,
        widget_section_name,
        widget_space_name,
        bid_id,
        sell_price,
        cpo_adv_version,
        seller_price,
        final_price,
        position_duplicate_count
    FROM sessions_with_duplicate_stats
    WHERE position_duplicate_rank = 1
),
sessions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, install_id, session_id, query
            ORDER BY `position` ASC, received_at ASC, logged_at ASC, sku_group_id ASC
        ) AS deduplicate_rank
    FROM sessions_deduplicated
),
ranking_score_events AS (
    SELECT
        trim(lower(search_query)) AS query,
        ranking_candidates,
        COALESCE(
            from_json(
                get_json_object(external_features, '$.normalized_linear_score'),
                'ARRAY<DOUBLE>'
            ),
            array_repeat(CAST(NULL AS DOUBLE), size(ranking_candidates))
        ) AS normalized_linear_scores,
        COALESCE(
            from_json(
                get_json_object(external_features, '$.linear_score'),
                'ARRAY<DOUBLE>'
            ),
            array_repeat(CAST(NULL AS DOUBLE), size(ranking_candidates))
        ) AS linear_scores,
        COALESCE(
            from_json(
                get_json_object(external_features, '$.dssm_score'),
                'ARRAY<DOUBLE>'
            ),
            array_repeat(CAST(NULL AS DOUBLE), size(ranking_candidates))
        ) AS dssm_scores
    FROM iceberg.silver.ranking_analytics_events
    CROSS JOIN params p
    WHERE
        fired_at >= p.event_date
        AND fired_at < DATE_ADD(p.event_date, 1)
        AND model_name LIKE '%search_uni%'
        AND ranking_candidates IS NOT NULL
        AND size(ranking_candidates) > 0
        AND search_query IS NOT NULL
        AND trim(search_query) != ''
        AND external_features IS NOT NULL
),
ranking_scores_exploded AS (
    SELECT
        query,
        CAST(score.ranking_candidates AS BIGINT) AS sku_group_id,
        CAST(score.normalized_linear_scores AS DOUBLE) AS normalized_linear_score,
        CAST(score.linear_scores AS DOUBLE) AS linear_score,
        CAST(score.dssm_scores AS DOUBLE) AS dssm_score
    FROM ranking_score_events
    LATERAL VIEW explode(
        arrays_zip(
            ranking_candidates,
            normalized_linear_scores,
            linear_scores,
            dssm_scores
        )
    ) exploded_scores AS score
),
ranking_scores AS (
    SELECT
        query,
        sku_group_id,
        AVG(normalized_linear_score) AS normalized_linear_score,
        AVG(linear_score) AS linear_score,
        AVG(dssm_score) AS dssm_score
    FROM ranking_scores_exploded
    WHERE sku_group_id IS NOT NULL
    GROUP BY
        query,
        sku_group_id
)
SELECT
    s.collection_date,
    s.event_date,
    s.logged_at,
    s.received_at,
    s.install_id,
    s.session_id,
    s.sku_group_id,
    s.query,
    s.`position`,
    CAST(s.deduplicate_rank AS BIGINT) AS deduplicate_rank,
    CAST(s.position_duplicate_count AS BIGINT) AS position_duplicate_count,
    s.widget_section_name,
    s.widget_space_name,
    s.cpo_adv_version,
    s.bid_id,
    s.sell_price,
    s.seller_price,
    s.final_price,
    rs.normalized_linear_score,
    rs.linear_score,
    rs.dssm_score,
    COALESCE(o.is_generated_order, 0) AS is_generated_order,
    COALESCE(sa.has_search_attr, 0) AS has_search_attr
FROM sessions s
LEFT JOIN orders o
    ON o.install_id = s.install_id
    AND o.last_search_session_id = s.session_id
    AND o.sku_group_id = s.sku_group_id
    AND o.query = s.query
LEFT JOIN search_attr_orders sa
    ON sa.install_id = s.install_id
    AND sa.last_search_session_id = s.session_id
    AND sa.sku_group_id = s.sku_group_id
    AND sa.query = s.query
LEFT JOIN ranking_scores rs
    ON rs.query = s.query
    AND rs.sku_group_id = s.sku_group_id
"""
    )


def save_search_ranking_dataset(
    spark: SparkSession,
    partition_start: str,
    target_table: str,
) -> None:
    dataset = build_search_ranking_dataset(
        spark,
        partition_start,
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    dataset.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_ranking_dataset(
        spark,
        arguments.partition_start,
        arguments.table_name,
    )
