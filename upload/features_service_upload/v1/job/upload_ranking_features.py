import json
import math
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType
from ranking_python_client import (
    AccountFeatureSet,
    AccountToCategoryFeatureSet,
    FeaturesUpdate,
    QueryFeatureSet,
    SkuGroupFeatureSet,
    SkuGroupToCategoryFeatureSet,
    SkuGroupToQueryFeatureSet,
)

from job.entities import Arguments

try:
    from ranking_python_client import SkuGroupToPromoFeatureSet
except ImportError:
    SkuGroupToPromoFeatureSet = None


ENTITY_TYPES = {
    ("account_id",): ("accountFeatureSet", AccountFeatureSet),
    ("query",): ("queryFeatureSet", QueryFeatureSet),
    ("sku_group_id",): ("skuGroupFeatureSet", SkuGroupFeatureSet),
    ("account_id", "category_id"): (
        "accountToCategoryFeatureSet",
        AccountToCategoryFeatureSet,
    ),
    ("category_id", "sku_group_id"): (
        "skuGroupToCategoryFeatureSet",
        SkuGroupToCategoryFeatureSet,
    ),
    ("query", "sku_group_id"): (
        "skuGroupToQueryFeatureSet",
        SkuGroupToQueryFeatureSet,
    ),
    ("promotion_id", "sku_group_id"): (
        "skuGroupToPromoFeatureSet",
        SkuGroupToPromoFeatureSet,
    ),
}

PROTO_KEY_ARGUMENTS = {
    "account_id": "accountId",
    "category_id": "categoryId",
    "promotion_id": "promoId",
    "query": "query",
    "sku_group_id": "skuGroupId",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _load_config(config_path: str) -> dict[str, Any]:
    return json.loads(Path(config_path).read_text(encoding="utf-8"))


def _feature_group_catalog(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(feature_group["name"]): feature_group
        for feature_group in config["feature_groups"]
    }


def _requested_feature_groups(
    config: dict[str, Any],
    feature_groups_argument: str | None,
) -> list[dict[str, Any]]:
    if not feature_groups_argument:
        return config["feature_groups"]

    requested_groups = json.loads(feature_groups_argument)
    catalog = _feature_group_catalog(config)
    selected_groups = []
    for requested_group in requested_groups:
        group_name = str(requested_group["name"])
        if group_name not in catalog:
            raise ValueError(f"Unknown requested feature group: {group_name}")

        features = [str(feature) for feature in requested_group["features"]]
        selected_group = dict(catalog[group_name])
        selected_group["source"] = dict(catalog[group_name]["source"])
        selected_group["features"] = features
        if "log1p_features" in selected_group:
            selected_group["log1p_features"] = [
                feature
                for feature in selected_group["log1p_features"]
                if feature in features
            ]
        selected_groups.append(selected_group)
    return selected_groups


def _read_simple_nested_config(config_path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, config)]
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, separator, value = raw_line.strip().partition(":")
        if not separator or not key:
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value.strip():
            parent[key.strip()] = value.strip()
        else:
            nested: dict[str, Any] = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


def _source_metadata(
    repo_root: str,
    feature_group: dict[str, Any],
) -> dict[str, Any]:
    source = feature_group["source"]
    for config_path in sorted(Path(repo_root).glob("layers/**/config.yaml")):
        table = _read_simple_nested_config(config_path).get("table", {})
        if (
            table.get("schema") == source["schema"]
            and table.get("name") == source["table"]
        ):
            primary_key = [
                column.strip()
                for column in str(table["primary_key"]).split(",")
                if column.strip()
            ]
            if table["schema"] != "gold":
                raise ValueError(
                    f"Ranking upload source must be gold, got "
                    f"{table['schema']}.{table['name']}"
                )
            timestamp_column = source.get("timestamp_column")
            if "date" in primary_key:
                date_column = "date"
                entity_keys = [column for column in primary_key if column != "date"]
            elif timestamp_column in primary_key:
                if source.get("read_mode") != "latest_timestamp":
                    raise ValueError(
                        f"Ranking upload source {table['schema']}.{table['name']} "
                        "with timestamp primary key must use read_mode=latest_timestamp"
                    )
                date_column = None
                entity_keys = [
                    column for column in primary_key if column != timestamp_column
                ]
            else:
                raise ValueError(
                    f"Ranking upload source {table['schema']}.{table['name']} "
                    "primary_key must contain date or configured timestamp_column"
                )
            return {
                "catalog": table["catalog"],
                "primary_key": primary_key,
                "date_column": date_column,
                "timestamp_column": timestamp_column,
                "entity_keys": entity_keys,
            }
    raise ValueError(
        f"Source table {source['schema']}.{source['table']} is not declared "
        "under layers/**/config.yaml"
    )


def _source_table(feature_group: dict[str, Any], metadata: dict[str, Any]) -> str:
    source = feature_group["source"]
    return f"{metadata['catalog']}.{source['schema']}.{source['table']}"


def _entity_keys(metadata: dict[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(metadata["entity_keys"]))


def _prepare_source_frame(
    spark: SparkSession,
    feature_group: dict[str, Any],
    metadata: dict[str, Any],
    run_date: str,
) -> DataFrame:
    frame = spark.table(_source_table(feature_group, metadata))
    if metadata["date_column"]:
        frame = frame.filter(
            F.col(metadata["date_column"]) == F.lit(run_date).cast("date")
        )
    elif feature_group["source"].get("read_mode") == "latest_timestamp":
        timestamp_column = metadata["timestamp_column"]
        latest_row = frame.agg(
            F.max(F.col(timestamp_column)).alias("_latest")
        ).collect()[0]
        latest_timestamp = latest_row["_latest"]
        if latest_timestamp is None:
            print(
                f"Source {_source_table(feature_group, metadata)} has no "
                f"{timestamp_column} values"
            )
            frame = frame.filter(F.lit(False))
        else:
            print(
                f"Use latest {timestamp_column}={latest_timestamp} "
                f"from {_source_table(feature_group, metadata)}"
            )
            frame = frame.filter(F.col(timestamp_column) == F.lit(latest_timestamp))
    if feature_group["source"].get("limit") is not None:
        frame = frame.limit(int(feature_group["source"]["limit"]))

    selections = [F.col(key) for key in metadata["entity_keys"]]
    selections.extend(
        F.col(feature_name)
        for feature_name in feature_group["features"]
    )
    return frame.select(*selections).na.fill(0.0, feature_group["features"])


def _feature_value(row: Any, feature_name: str, log1p_features: set[str]) -> float:
    value = row[feature_name]
    if value is None:
        value = 0.0
    if feature_name in log1p_features:
        return math.log1p(float(value))
    return float(value)


def _feature_values(
    row: Any,
    feature_group: dict[str, Any],
) -> list[float]:
    log1p_features = set(feature_group.get("log1p_features", []))
    return [
        _feature_value(row, feature_name, log1p_features)
        for feature_name in feature_group["features"]
    ]


def _row_to_proto(
    row: Any,
    feature_group: dict[str, Any],
    metadata: dict[str, Any],
) -> bytes:
    entity_keys = _entity_keys(metadata)
    update_field, message_class = ENTITY_TYPES[entity_keys]
    if message_class is None:
        raise RuntimeError(
            f"ranking_python_client does not support entity keys {entity_keys}. "
            "Update the ranking upload image/client before enabling this feature group."
        )
    key_arguments = {
        PROTO_KEY_ARGUMENTS[key]: row[key]
        for key in metadata["entity_keys"]
    }
    message = message_class(
        **key_arguments,
        fsName=feature_group["name"],
        features=_feature_values(row, feature_group),
    )
    return FeaturesUpdate(**{update_field: message}).SerializeToString()


def _row_to_debug_payload(
    row: Any,
    feature_group: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    entity_keys = _entity_keys(metadata)
    update_field, _ = ENTITY_TYPES[entity_keys]
    feature_values = _feature_values(row, feature_group)
    return {
        "update_field": update_field,
        "fsName": feature_group["name"],
        "keys": {key: row[key] for key in metadata["entity_keys"]},
        "features_count": len(feature_values),
        "features": feature_values,
    }


def _to_kafka_frame(
    frame: DataFrame,
    feature_group: dict[str, Any],
    metadata: dict[str, Any],
) -> DataFrame:
    serializer = F.udf(
        partial(_row_to_proto, feature_group=feature_group, metadata=metadata),
        BinaryType(),
    )
    key_columns = [
        F.coalesce(F.col(key).cast("string"), F.lit(""))
        for key in metadata["entity_keys"]
    ]
    return (
        frame.withColumn("_serialized", serializer(F.struct("*")))
        .select(
            F.concat_ws("|", F.lit(feature_group["name"]), *key_columns).alias("key"),
            F.col("_serialized").alias("value"),
        )
    )


def _write_to_kafka(frame: DataFrame, arguments: Arguments) -> None:
    (
        frame.write.format("kafka")
        .option("kafka.bootstrap.servers", arguments.kafka_brokers)
        .option("topic", arguments.kafka_topic)
        .option("kafka.ssl.truststore.location", "/opt/spark/work-dir/client.truststore")
        .option("kafka.ssl.truststore.password", "changeit")
        .option("kafka.security.protocol", "SASL_SSL")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{arguments.kafka_login}" password="{arguments.kafka_password}";',
        )
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.max.request.size", "12000000")
        .option("kafka.compression.type", "snappy")
        .option("kafka.linger.ms", "10")
        .option("kafka.batch.size", str(120 * 1024))
        .option("kafka.queue.buffering.max.messages", "170000")
        .option("kafka.acks", "all")
        .save()
    )


def run(spark: SparkSession, arguments: Arguments) -> None:
    config = _load_config(arguments.config_path)
    feature_groups = _requested_feature_groups(config, arguments.feature_groups)
    print(
        "Upload component feature groups: "
        f"{[feature_group['name'] for feature_group in feature_groups]}"
    )
    for feature_group in feature_groups:
        metadata = _source_metadata(arguments.repo_root, feature_group)
        source_frame = _prepare_source_frame(
            spark,
            feature_group,
            metadata,
            arguments.run_date,
        )
        source_count = source_frame.count()
        print(
            f"Prepared feature group {feature_group['name']} "
            f"from {_source_table(feature_group, metadata)} "
            f"date={arguments.run_date} rows={source_count} "
            f"limit={feature_group['source'].get('limit')}"
        )
        if source_count == 0:
            print(
                f"Skip Kafka upload for {feature_group['name']}: "
                f"no rows for date={arguments.run_date}"
            )
            continue
        for sample_index, sample_row in enumerate(source_frame.take(3), start=1):
            print(
                "Kafka payload sample "
                f"feature_group={feature_group['name']} "
                f"sample={sample_index} "
                f"{json.dumps(_row_to_debug_payload(sample_row, feature_group, metadata), ensure_ascii=False, default=_json_default)}"
            )
        kafka_frame = _to_kafka_frame(source_frame, feature_group, metadata)
        kafka_count = kafka_frame.count()
        for sample_index, sample_row in enumerate(kafka_frame.select("key").take(3), start=1):
            print(
                "Kafka key sample "
                f"feature_group={feature_group['name']} "
                f"sample={sample_index} key={sample_row['key']}"
            )
        print(
            f"Upload feature group {feature_group['name']} "
            f"to Kafka topic={arguments.kafka_topic} records={kafka_count}"
        )
        _write_to_kafka(kafka_frame, arguments)
        print(
            f"Uploaded feature group {feature_group['name']} "
            f"to Kafka topic={arguments.kafka_topic} records={kafka_count}"
        )
