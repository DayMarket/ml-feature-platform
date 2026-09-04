import json
import os
import random
import string
from typing import Any

from airflow.sdk import BaseHook


def _unquote_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _read_simple_config(path: str) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack = [(-1, config)]

    with open(path, "r", encoding="utf-8") as config_file:
        for raw_line in config_file:
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue

            indent = len(raw_line) - len(raw_line.lstrip(" "))
            key, separator, value = raw_line.strip().partition(":")
            if not separator or not key:
                continue

            while stack and indent <= stack[-1][0]:
                stack.pop()

            parent = stack[-1][1]
            value = value.strip()
            if value:
                parent[key.strip()] = _unquote_scalar(value)
            else:
                nested: dict[str, Any] = {}
                parent[key.strip()] = nested
                stack.append((indent, nested))

    return config


def _get_config() -> dict[str, Any]:
    entity_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return _read_simple_config(os.path.join(entity_root, "config.yaml"))


def _normalize_team_name(value: Any) -> str:
    team_name = str(value or "search")
    if team_name.startswith("team::"):
        return team_name.split("team::", 1)[1]
    if team_name.startswith("team:"):
        return team_name.split("team:", 1)[1]
    return team_name


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().strip('"').strip("'").lower()
    if normalized in {"true", "yes", "1"}:
        return True
    if normalized in {"false", "no", "0"}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _get_table_name(config: dict[str, Any]) -> str:
    table = config["table"]
    return ".".join((table["catalog"], table["schema"], table["name"]))


def _get_resources(config: dict[str, Any]) -> dict[str, Any]:
    entity_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    resources_path = os.path.abspath(
        os.path.join(entity_root, config["resources"]["path"])
    )
    with open(resources_path, "r", encoding="utf-8") as resources_file:
        return json.load(resources_file)


def get_dag_settings() -> dict[str, Any]:
    config = _get_config()
    table_meta = config["table"]["meta"]
    dag_config = config["dag"]
    alerts_config = config["alerts"]

    dag_team = _normalize_team_name(dag_config.get("team", table_meta["team"]))
    alert_team = _normalize_team_name(alerts_config.get("team", dag_team))

    return {
        "dag_id": str(dag_config["id"]),
        "owner": str(dag_config.get("owner", f"team:{dag_team}")),
        "team_tag": str(dag_config.get("team_tag", f"team::{dag_team}")),
        "group_tag": str(dag_config["group_tag"]),
        "schedule": str(dag_config["schedule"]),
        "start_date": str(dag_config["start_date"]),
        "catchup": _parse_bool(dag_config["catchup"]),
        "alert_severity": str(alerts_config["severity"]),
        "alert_team": alert_team,
        "alert_oncall_webhook_conn_id": str(
            alerts_config["oncall_webhook_conn_id"]
        ),
    }


def get_deployment() -> str:
    config = _get_config()
    spark_config = config["spark"]
    resources = _get_resources(config)
    resource_profile = resources["profiles"][spark_config["resource_profile"]]

    entity_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    template_path = os.path.abspath(
        os.path.join(entity_root, spark_config["template_path"])
    )
    with open(template_path, "r", encoding="utf-8") as template_file:
        deployment = template_file.read()

    s3_connection = json.loads(
        BaseHook.get_connection("spark_ycs_connection").extra
    )
    research_connection = json.loads(
        BaseHook.get_connection("spark_search_research_connection").extra
    )
    executor_core_request = resource_profile.get("executor_core_request")
    if executor_core_request is None:
        executor_core_request = resource_profile["executor_cores"]

    replacements = {
        "<partition_start>": (
            '{{ data_interval_start.in_timezone("UTC").strftime('
            '"%Y-%m-%d %H:%M:%S") }}'
        ),
        "<partition_end>": (
            '{{ data_interval_end.in_timezone("UTC").strftime('
            '"%Y-%m-%d %H:%M:%S") }}'
        ),
        "<random_string>": "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        ).lower(),
        "<application_name>": str(spark_config["application_name"]),
        "<main_application_file>": str(spark_config["main_application_file"]),
        "<app_type>": str(resources["app_type"]),
        "<spark_event_log_bucket_name>": str(resources["spark_event_log_bucket"]),
        "<hive_metastore_uris>": str(resources["hive_metastore_uris"]),
        "<table_name>": _get_table_name(config),
        "<driver_cores>": str(resource_profile["driver_cores"]),
        "<driver_memory>": str(resource_profile["driver_memory"]),
        "<driver_memory_overhead>": str(
            resource_profile["driver_memory_overhead"]
        ),
        "<executor_cores>": str(resource_profile["executor_cores"]),
        "<executor_core_request>": str(executor_core_request),
        "<executor_instances>": str(resource_profile["executor_instances"]),
        "<executor_memory>": str(resource_profile["executor_memory"]),
        "<executor_memory_overhead>": str(
            resource_profile["executor_memory_overhead"]
        ),
        "<s3_secret_key>": s3_connection["aws_secret_access_key"],
        "<s3_access_key>": s3_connection["aws_access_key_id"],
        "<s3_search_research_secret_key>": research_connection[
            "aws_secret_access_key"
        ],
        "<s3_search_research_access_key>": research_connection[
            "aws_access_key_id"
        ],
    }

    for placeholder, value in replacements.items():
        deployment = deployment.replace(placeholder, value)

    return deployment
