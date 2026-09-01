from datetime import date

from job.entities import DatasetSettings

HASH_BUCKETS = 10000


def sample_threshold(sample_percent: int) -> int:
    """Верхняя граница хэш-бакета для заданного процента запросов."""
    return int(sample_percent) * HASH_BUCKETS // 100


def build_dataset_query(
    collection_date: date,
    event_date_start: date,
    event_date_end: date,
    settings: DatasetSettings,
) -> str:
    threshold = sample_threshold(settings.sample_percent)

    return f"""
WITH params AS (
    SELECT
        DATE '{collection_date.isoformat()}' AS collection_date,
        DATE '{event_date_start.isoformat()}' AS event_date_start,
        DATE '{event_date_end.isoformat()}' AS event_date_end
),
event_dates AS (
    SELECT explode(sequence(p.event_date_start, date_sub(p.event_date_end, 1))) AS event_date
    FROM params p
),
sampled_events AS (
    SELECT
        CAST(e.fired_at AS DATE) AS event_date,
        e.fired_at AS fired_at,
        e.model_name AS model_name,
        e.request_id AS request_id,
        e.install_id AS install_id,
        e.search_query AS search_query,
        e.category_id AS category_id,
        e.promo_id AS promo_id,
        e.ranking_candidates AS ranking_candidates,
        e.final_scores AS final_scores,
        e.model_output AS model_output,
        e.model_input['cm2_features'] AS cm2_features,
        e.common_external_features AS common_external_features,
        -- Длина JSON-массивов не проверена на живых данных для dssm_score,
        -- normalized_linear_score, cpo_adv_percents и bid_amounts, поэтому при
        -- рассогласовании колонка обнуляется, а строка лога сохраняется.
        CASE
            WHEN size(from_json(get_json_object(e.external_features, '$.dssm_score'), 'ARRAY<DOUBLE>'))
                 = size(e.ranking_candidates)
            THEN from_json(get_json_object(e.external_features, '$.dssm_score'), 'ARRAY<DOUBLE>')
            ELSE array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        END AS dssm_scores,
        CASE
            WHEN size(from_json(get_json_object(e.external_features, '$.linear_score'), 'ARRAY<DOUBLE>'))
                 = size(e.ranking_candidates)
            THEN from_json(get_json_object(e.external_features, '$.linear_score'), 'ARRAY<DOUBLE>')
            ELSE array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        END AS linear_scores,
        CASE
            WHEN size(from_json(get_json_object(e.external_features, '$.normalized_linear_score'), 'ARRAY<DOUBLE>'))
                 = size(e.ranking_candidates)
            THEN from_json(get_json_object(e.external_features, '$.normalized_linear_score'), 'ARRAY<DOUBLE>')
            ELSE array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        END AS normalized_linear_scores,
        CASE
            WHEN size(from_json(get_json_object(e.external_features, '$.cpo_adv_percents'), 'ARRAY<DOUBLE>'))
                 = size(e.ranking_candidates)
            THEN from_json(get_json_object(e.external_features, '$.cpo_adv_percents'), 'ARRAY<DOUBLE>')
            ELSE array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        END AS cpo_adv_percents,
        CASE
            WHEN size(from_json(get_json_object(e.external_features, '$.bid_amounts'), 'ARRAY<DOUBLE>'))
                 = size(e.ranking_candidates)
            THEN from_json(get_json_object(e.external_features, '$.bid_amounts'), 'ARRAY<DOUBLE>')
            ELSE array_repeat(CAST(NULL AS DOUBLE), size(e.ranking_candidates))
        END AS bid_amounts
    FROM iceberg.silver.ranking_analytics_events e
    WHERE
        -- Статические границы, не значения из params: без них джойн/фильтр по
        -- значению из CTE может не включить partition pruning на самом дорогом
        -- скане джоба (см. тот же приём и комментарий у feedback/frequency ниже).
        e.fired_at >= DATE '{event_date_start.isoformat()}'
        AND e.fired_at < DATE '{event_date_end.isoformat()}'
        AND e.model_name = '{settings.model_name}'
        AND e.ranking_candidates IS NOT NULL
        AND size(e.ranking_candidates) > 0
        AND e.model_input IS NOT NULL
        -- arrays_zip дополняет короткие массивы NULL'ами до самого длинного, из-за
        -- чего кандидат и его скоры разъезжаются по индексу. Событие с рассогласованной
        -- длиной родных массивов — сломанный контракт лога, его дешевле выбросить.
        AND size(e.final_scores) = size(e.ranking_candidates)
        AND size(e.model_output) = size(e.ranking_candidates)
        AND size(e.model_input['cm2_features']) = size(e.ranking_candidates)
        -- Отбор по запросу, не по строке: попавший запрос берётся со всеми
        -- кандидатами. pmod, а не abs: xxhash64 может вернуть Long.MIN_VALUE,
        -- у которого abs отрицателен и условие молча отсечёт всё.
        AND pmod(xxhash64(e.request_id), {HASH_BUCKETS}) < {threshold}
        -- xxhash64(NULL) — константа-сид 42, а pmod(42, 10000) = 42 всегда
        -- меньше любого положительного порога: без этого фильтра каждая строка
        -- лога с NULL request_id проходила бы сэмплирование со 100% вероятностью.
        AND e.request_id IS NOT NULL
),
candidates AS (
    SELECT
        s.event_date AS event_date,
        s.fired_at AS fired_at,
        s.model_name AS model_name,
        s.request_id AS request_id,
        s.install_id AS install_id,
        s.search_query AS search_query,
        s.category_id AS category_id,
        s.promo_id AS promo_id,
        s.common_external_features AS common_external_features,
        candidate_index + 1 AS candidate_position,
        candidate AS candidate,
        CAST(candidate.ranking_candidates AS BIGINT) AS sku_group_id
    FROM sampled_events s
    LATERAL VIEW posexplode(
        arrays_zip(
            s.ranking_candidates,
            s.final_scores,
            s.model_output,
            s.cm2_features,
            s.dssm_scores,
            s.linear_scores,
            s.normalized_linear_scores,
            s.cpo_adv_percents,
            s.bid_amounts
        )
    ) exploded AS candidate_index, candidate
),
-- Полный скан iceberg.silver.sku ради MIN(created_at) на группу: silver.sku —
-- снапшот без истории, партиции/даты для сужения скана у него нет, поэтому
-- цена этой агрегации неотделима от самого приёма (см. 5.1 дизайн-документа).
sku_group_created AS (
    SELECT
        sku_group_id,
        MIN(created_at) AS created_at
    FROM iceberg.silver.sku
    WHERE sku_group_id IS NOT NULL
    GROUP BY sku_group_id
),
feedback_snapshot_dates AS (
    SELECT DISTINCT f.date AS snapshot_date
    FROM iceberg.gold.feature_platform_sku_group_feedback_base_stats f
    WHERE
        f.date < DATE '{event_date_end.isoformat()}'
        AND f.date >= date_sub(DATE '{event_date_start.isoformat()}', 30)
),
feedback_date_map AS (
    SELECT
        d.event_date AS event_date,
        MAX(s.snapshot_date) AS snapshot_date
    FROM event_dates d
    JOIN feedback_snapshot_dates s ON s.snapshot_date <= d.event_date
    GROUP BY d.event_date
),
feedback AS (
    SELECT
        m.event_date AS event_date,
        f.sku_group_id AS sku_group_id,
        MAX(f.product_rating) AS product_rating,
        MAX(f.total_reviews_count) AS total_reviews_count
    FROM feedback_date_map m
    JOIN iceberg.gold.feature_platform_sku_group_feedback_base_stats f
        ON f.date = m.snapshot_date
    -- Статические границы повторяют окно feedback_snapshot_dates: без них
    -- джойн по значению из CTE может не включить partition pruning и утащить
    -- всю историю витрины.
    WHERE
        f.date >= date_sub(DATE '{event_date_start.isoformat()}', 30)
        AND f.date < DATE '{event_date_end.isoformat()}'
    GROUP BY m.event_date, f.sku_group_id
),
frequency_snapshot_dates AS (
    SELECT DISTINCT g.analyze_date AS snapshot_date
    FROM iceberg.silver.search_queries_frequency_groups_30d g
    WHERE
        g.analyze_date < DATE '{event_date_end.isoformat()}'
        AND g.analyze_date >= date_sub(DATE '{event_date_start.isoformat()}', 30)
),
frequency_date_map AS (
    SELECT
        d.event_date AS event_date,
        MAX(s.snapshot_date) AS snapshot_date
    FROM event_dates d
    JOIN frequency_snapshot_dates s ON s.snapshot_date <= d.event_date
    GROUP BY d.event_date
),
frequency AS (
    SELECT
        m.event_date AS event_date,
        trim(lower(g.query_text)) AS query,
        -- MAX() трёх колонок независимо друг от друга: на схлопнутом дубле
        -- (тот же snapshot_date + нормализованный query_text) значения могут
        -- прийти из разных исходных строк. MAX(frequency_group) вдобавок
        -- лексикографический по значениям HF/MF/LF, а не по частотности — не
        -- читать как авторитетное значение на дубле, только как способ не
        -- размножить строки лога.
        MAX(g.frequency_group) AS frequency_group,
        MAX(g.users_total) AS users_total,
        MAX(g.query_rank) AS query_rank
    FROM frequency_date_map m
    JOIN iceberg.silver.search_queries_frequency_groups_30d g
        ON g.analyze_date = m.snapshot_date
    WHERE
        g.analyze_date >= date_sub(DATE '{event_date_start.isoformat()}', 30)
        AND g.analyze_date < DATE '{event_date_end.isoformat()}'
    -- Схлопывание обязательно: trim(lower(...)) склеивает запросы, различающиеся
    -- только регистром или пробелами, и без GROUP BY такой дубль размножил бы
    -- каждую строку лога.
    GROUP BY m.event_date, trim(lower(g.query_text))
)
SELECT
    DATE '{collection_date.isoformat()}' AS collection_date,
    c.event_date AS event_date,
    c.fired_at AS fired_at,
    c.model_name AS model_name,
    c.request_id AS request_id,
    c.install_id AS install_id,
    c.search_query AS search_query,
    CAST(c.category_id AS INT) AS category_id,
    c.promo_id AS promo_id,
    CAST(c.candidate_position AS INT) AS `position`,
    c.sku_group_id AS sku_group_id,
    CAST(c.candidate.final_scores AS DOUBLE) AS final_score,
    CAST(c.candidate.model_output[1] AS DOUBLE) AS model_probability,
    CAST(c.candidate.model_output[2] AS DOUBLE) AS alpha_component,
    CAST(c.candidate.model_output[3] AS DOUBLE) AS beta_component,
    CAST(c.candidate.model_output[4] AS DOUBLE) AS gamma_component,
    CAST(c.candidate.model_output[5] AS DOUBLE) AS delta_component,
    CAST(c.candidate.dssm_scores AS DOUBLE) AS dssm_score,
    CAST(c.candidate.linear_scores AS DOUBLE) AS linear_score,
    CAST(c.candidate.normalized_linear_scores AS DOUBLE) AS normalized_linear_score,
    CAST(c.candidate.cpo_adv_percents AS DOUBLE) AS cpo_adv_percent,
    CAST(c.candidate.bid_amounts AS DOUBLE) AS bid_amount,
    CAST(c.candidate.cm2_features[0] AS DOUBLE) AS commission_percent,
    CAST(c.candidate.cm2_features[1] AS DOUBLE) AS seller_price,
    CAST(c.candidate.cm2_features[2] AS DOUBLE) AS logistics_fee,
    CAST(c.candidate.cm2_features[3] AS DOUBLE) AS cpi_cost,
    CAST(c.candidate.cm2_features[4] AS DOUBLE) AS cpm_bid,
    CAST(c.candidate.cm2_features[5] AS DOUBLE) AS cpo_percent,
    CAST(c.candidate.cm2_features[6] AS DOUBLE) AS vat_rate,
    CAST(c.candidate.cm2_features[7] AS DOUBLE) AS items_quantity,
    CAST(c.common_external_features['alpha'] AS DOUBLE) AS alpha,
    CAST(c.common_external_features['beta'] AS DOUBLE) AS beta,
    CAST(c.common_external_features['gamma'] AS DOUBLE) AS gamma,
    CAST(c.common_external_features['delta'] AS DOUBLE) AS delta,
    CAST(datediff(c.event_date, CAST(sg.created_at AS DATE)) AS INT) AS sku_group_age_days,
    CAST(fb.product_rating AS DOUBLE) AS product_rating,
    CAST(fb.total_reviews_count AS BIGINT) AS total_reviews_count,
    COALESCE(fq.frequency_group, 'LF') AS frequency_group,
    CAST(fq.users_total AS BIGINT) AS users_total,
    CAST(fq.query_rank AS BIGINT) AS query_rank
FROM candidates c
LEFT JOIN sku_group_created sg
    ON sg.sku_group_id = c.sku_group_id
LEFT JOIN feedback fb
    ON fb.event_date = c.event_date
    AND fb.sku_group_id = c.sku_group_id
LEFT JOIN frequency fq
    ON fq.event_date = c.event_date
    AND fq.query = trim(lower(c.search_query))
"""
