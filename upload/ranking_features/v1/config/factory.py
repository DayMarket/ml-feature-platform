import json
import os
import random
import string
from typing import Any, Dict

from airflow.hooks.base import BaseHook


def _dag_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def get_config() -> Dict[str, Any]:
    return _load_json(os.path.join(_dag_root(), "config.yaml"))


def _normalize_team_name(team_value: Any) -> str:
    team_name = str(team_value or "search")
    if team_name.startswith("team:"):
        return team_name.split(":", 1)[1]
    if team_name.startswith("team::"):
        return team_name.split("::", 1)[1]
    return team_name


def get_dag_settings() -> Dict[str, str]:
    config = get_config()
    dag_config = config.get("dag", {})
    alerts_config = config.get("alerts", {})
    dag_team = _normalize_team_name(dag_config.get("team", "search"))
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


def get_source_dependencies() -> list[dict[str, str]]:
    dependencies = []
    seen_tables = set()
    for feature_group in get_config()["feature_groups"]:
        source = feature_group["source"]
        source_key = (source["schema"], source["table"])
        if source_key in seen_tables:
            continue
        seen_tables.add(source_key)
        dependencies.append(
            {
                "task_id": f"wait_for_{source['table']}",
                "external_dag_id": (
                    f"dbt.source.trino.ml_feature_platform_{source['schema']}."
                    f"{source['table']}.dq"
                ),
            }
        )
    return dependencies


def _kafka_brokers(connection: Any) -> str:
    extra = connection.extra_dejson
    brokers = extra.get("bootstrap_servers") or extra.get("brokers")
    if brokers:
        return str(brokers)
    if connection.port:
        return ",".join(
            f"{host.strip()}:{connection.port}"
            for host in str(connection.host).split(",")
            if host.strip()
        )
    return str(connection.host)


def get_deployment(folder_name: str, deployment_name: str) -> str:
    deployment_path = os.path.join(os.path.dirname(__file__), folder_name, deployment_name)
    with open(deployment_path, "r", encoding="utf-8") as deployment_file:
        deployment_content = deployment_file.read()

    config = get_config()
    resources_path = os.path.join(_dag_root(), config["resources"]["path"])
    resources = _load_json(resources_path)
    s3_connection = json.loads(
        BaseHook.get_connection("spark_ycs_connection").get_extra()
    )
    kafka_connection = BaseHook.get_connection(config["kafka"]["connection_id"])
    random_string = "".join(
        random.choices(string.ascii_letters + string.digits, k=10)
    ).lower()

    replacements = {
        "<random_string>": random_string,
        "<app_type>": str(resources["app_type"]),
        "<spark_event_log_bucket_name>": str(resources["spark_event_log_bucket"]),
        "<hive_metastore_uris>": str(resources["hive_metastore_uris"]),
        "<driver_cores>": str(resources["driver_cores"]),
        "<driver_memory>": str(resources["driver_memory"]),
        "<executor_cores>": str(resources["executor_cores"]),
        "<executor_instances>": str(resources["executor_instances"]),
        "<executor_memory>": str(resources["executor_memory"]),
        "<s3_secret_key>": str(s3_connection["aws_secret_access_key"]),
        "<s3_access_key>": str(s3_connection["aws_access_key_id"]),
        "<run_date>": "{{ ds }}",
        "<config_path>": "/git/repo/upload/ranking_features/v1/config.yaml",
        "<kafka_topic>": str(config["kafka"]["topic"]),
        "<kafka_brokers>": _kafka_brokers(kafka_connection),
        "<kafka_login>": str(kafka_connection.login or ""),
        "<kafka_password>": str(kafka_connection.password or ""),
    }
    for source, target in replacements.items():
        deployment_content = deployment_content.replace(source, target)
    return deployment_content
