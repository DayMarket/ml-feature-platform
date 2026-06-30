import json
import os
import random
import string
from typing import Any, Dict, Optional

from airflow.sdk import BaseHook


def _dag_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def _read_simple_config(path: str) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
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
            if value.strip():
                parent[key.strip()] = value.strip()
            else:
                nested: Dict[str, Any] = {}
                parent[key.strip()] = nested
                stack.append((indent, nested))
    return config


def _get_dag_config() -> Dict[str, Any]:
    return _read_simple_config(os.path.join(_dag_root(), "config.yaml"))


def _get_table_name(config: Dict[str, Any]) -> str:
    table = config["table"]
    return ".".join((table["catalog"], table["schema"], table["name"]))


def _get_spark_config() -> Dict[str, Any]:
    return _get_dag_config()["spark"]


def _get_resources_config() -> Dict[str, Any]:
    dag_config = _get_dag_config()
    resources_path = os.path.abspath(
        os.path.join(_dag_root(), dag_config["resources"]["path"])
    )
    return _read_json(resources_path)


def _get_task_resources(resources_config: Dict[str, Any]) -> Dict[str, Any]:
    spark_config = _get_spark_config()
    return resources_config["profiles"][spark_config["resource_profile"]]


def _normalize_team_name(team_value: Any) -> str:
    team_name = str(team_value or "search")
    if team_name.startswith("team:"):
        return team_name.split(":", 1)[1]
    if team_name.startswith("team::"):
        return team_name.split("::", 1)[1]
    return team_name


def get_dag_settings() -> Dict[str, str]:
    config = _get_dag_config()
    table_meta = config.get("table", {}).get("meta", {})
    dag_config = config.get("dag", {})
    alerts_config = config.get("alerts", {})

    dag_team = _normalize_team_name(
        dag_config.get("team", table_meta.get("team", "search"))
    )
    alert_team = _normalize_team_name(alerts_config.get("team", dag_team))

    return {
        "dag_id": str(dag_config["id"]),
        "schedule": str(dag_config["schedule"]),
        "start_date": str(dag_config["start_date"]),
        "owner": str(dag_config.get("owner", f"team:{dag_team}")),
        "team_tag": str(dag_config.get("team_tag", f"team::{dag_team}")),
        "alert_severity": str(alerts_config.get("severity", "P3")),
        "alert_team": alert_team,
        "alert_oncall_webhook_conn_id": str(
            alerts_config.get("oncall_webhook_conn_id", f"oncall_webhook_{alert_team}")
        ),
    }


def _shared_deployment_path(deployment_name: Optional[str]) -> str:
    spark_config = _get_spark_config()
    template_path = spark_config.get("template_path")
    if not template_path:
        raise FileNotFoundError(
            f"Local deployment {deployment_name} is missing and spark.template_path is not configured"
        )
    return os.path.abspath(os.path.join(_dag_root(), template_path))


def _read_deployment(folder_name: str, deployment_name: str) -> str:
    deployment_path = os.path.join(os.path.dirname(__file__), folder_name, deployment_name)
    if not os.path.exists(deployment_path):
        deployment_path = _shared_deployment_path(deployment_name)
    with open(deployment_path, "r", encoding="utf-8") as deployment_file:
        return deployment_file.read()


def get_deployment(folder_name: str, deployment_name: str) -> str:
    deployment_content = _read_deployment(folder_name, deployment_name)
    config = _get_dag_config()
    spark_config = _get_spark_config()
    resources_config = _get_resources_config()
    task_resources = _get_task_resources(resources_config)
    s3_connection = json.loads(BaseHook.get_connection("spark_ycs_connection").extra)
    random_string = "".join(
        random.choices(string.ascii_letters + string.digits, k=10)
    ).lower()

    replacements = {
        "<partition_start>": '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}',
        "<partition_end>": '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}',
        "<random_string>": random_string,
        "<application_name>": str(spark_config["application_name"]),
        "<main_application_file>": str(spark_config["main_application_file"]),
        "<app_type>": str(resources_config["app_type"]),
        "<spark_event_log_bucket_name>": str(resources_config["spark_event_log_bucket"]),
        "<hive_metastore_uris>": str(resources_config["hive_metastore_uris"]),
        "<table_name>": _get_table_name(config),
        "<s3_secret_key>": s3_connection["aws_secret_access_key"],
        "<s3_access_key>": s3_connection["aws_access_key_id"],
        "<driver_cores>": str(task_resources["driver_cores"]),
        "<driver_memory>": str(task_resources["driver_memory"]),
        "<executor_cores>": str(task_resources["executor_cores"]),
        "<executor_instances>": str(task_resources["executor_instances"]),
        "<executor_memory>": str(task_resources["executor_memory"]),
    }
    for source, target in replacements.items():
        deployment_content = deployment_content.replace(source, target)
    return deployment_content
