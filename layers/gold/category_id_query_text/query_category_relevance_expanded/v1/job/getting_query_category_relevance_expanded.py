from datetime import date, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


SOURCE_TABLE = "iceberg.gold.feature_platform_query_category_relevance"
QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"

SELECTED_COLUMNS = ("date", "category_id", "query_text", "relevance")


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def parse_partition_date(partition_start: str) -> str:
    supported_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )
    normalized_value = partition_start
    if normalized_value.endswith("Z"):
        normalized_value = f"{normalized_value[:-1]}+0000"
    else:
        normalized_value = normalized_value.replace("+00:00", "+0000")

    for date_format in supported_formats:
        try:
            return datetime.strptime(normalized_value, date_format).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(partition_start).date().isoformat()
    except ValueError as error:
        raise ValueError(
            "Unsupported partition_start value for "
            f"query_category_relevance_expanded: {partition_start}"
        ) from error


def render_query(run_date: str) -> str:
    # run_date подставляется в SQL литералом: принимаем только чистую дату.
    if date.fromisoformat(run_date).isoformat() != run_date:
        raise ValueError(f"run_date must be YYYY-MM-DD, got {run_date!r}")

    # 1. Вся витрина, без фильтра по date: даты источника не важны.
    # 2. К строкам добавляются копии со всеми query_text того же query_id из справочника
    #    (связь только по query_id).
    # 3. Текст запроса только приводится к нижнему регистру.
    # 4. На пару category_id, query_text остаётся одна строка с максимальным relevance.
    # run_date задаёт только партицию, в которую пишется снимок.
    return f"""
WITH source AS (
    SELECT query_id, query_text, category_id, relevance
    FROM {SOURCE_TABLE}
),
merged AS (
    SELECT category_id, query_text, relevance
    FROM source
    UNION ALL
    SELECT source.category_id, dictionary.query_text, source.relevance
    FROM source
    JOIN {QUERY_ID_TABLE} AS dictionary ON dictionary.query_id = source.query_id
),
ranked AS (
    SELECT
        category_id,
        lower(query_text) AS query_text,
        relevance,
        row_number() OVER (
            PARTITION BY category_id, lower(query_text)
            ORDER BY relevance DESC NULLS LAST
        ) AS relevance_rank
    FROM merged
)
SELECT DATE '{run_date}' AS date, category_id, query_text, relevance
FROM ranked
WHERE relevance_rank = 1
"""


def save_query_category_relevance_expanded(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    frame = spark.sql(render_query(run_date)).select(*SELECTED_COLUMNS)
    frame.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_query_category_relevance_expanded(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
