import json
import os
import random
import string
from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook


def get_deployment(folder_name: str, deployment_name: str) -> str:
    deployment_content = _read_deployment(folder_name, deployment_name)
    return _fill_arguments(deployment_content, deployment_name)


def _read_deployment(folder_name: str, deployment_name: str) -> str:
    deployment_folder = os.path.join(os.path.dirname(__file__), folder_name)
    deployment_path = os.path.join(deployment_folder, deployment_name)
    with open(deployment_path, "r", encoding="utf-8") as deployment_file:
        return deployment_file.read()


def _read_json_like_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def _read_simple_config(path: str) -> Dict[str, str]:
    config: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as config_file:
        for raw_line in config_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition(":")
            if key and value:
                config[key.strip()] = value.strip()
    return config


def _get_dag_config() -> Dict[str, str]:
    dag_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return _read_simple_config(os.path.join(dag_root, "config.yaml"))


def _get_resources_config() -> Dict[str, Any]:
    dag_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    config = _get_dag_config()
    resources_path = config.get("resources", "config/resources.yaml")
    absolute_resources_path = os.path.abspath(os.path.join(dag_root, resources_path))
    return _read_json_like_yaml(absolute_resources_path)


def _get_task_resources(
    resources_config: Dict[str, Any],
    deployment_name: Optional[str],
) -> Dict[str, Any]:
    if deployment_name and isinstance(resources_config.get(deployment_name), dict):
        return resources_config[deployment_name]
    return resources_config


def _fill_arguments(deployment_content: str, deployment_name: Optional[str] = None) -> str:
    dag_config = _get_dag_config()
    resources_config = _get_resources_config()
    task_resources = _get_task_resources(resources_config, deployment_name)

    s3_connection = json.loads(
        BaseHook.get_connection("spark_ycs_connection").get_extra()
    )
    s3_search_research_connection = json.loads(
        BaseHook.get_connection("spark_search_research_connection").get_extra()
    )

    resources_values = {
        "<driver_cores>": str(task_resources["driver_cores"]),
        "<driver_memory>": str(task_resources["driver_memory"]),
        "<executor_cores>": str(task_resources["executor_cores"]),
        "<executor_instances>": str(task_resources["executor_instances"]),
        "<executor_memory>": str(task_resources["executor_memory"]),
    }
    random_string = "".join(
        random.choices(string.ascii_letters + string.digits, k=10)
    ).lower()

    from_to_replacement = {
        "<trigger_date>": "{{ ds }}",
        "<random_string>": random_string,
        "<app_type>": str(resources_config["app_type"]),
        "<spark_event_log_bucket_name>": str(resources_config["spark_event_log_bucket"]),
        "<hive_metastore_uris>": str(resources_config["hive_metastore_uris"]),
        "<table_name>": dag_config["table_name"],
        "<s3_secret_key>": s3_connection["aws_secret_access_key"],
        "<s3_access_key>": s3_connection["aws_access_key_id"],
        "<s3_search_research_secret_key>": s3_search_research_connection["aws_secret_access_key"],
        "<s3_search_research_access_key>": s3_search_research_connection["aws_access_key_id"],
    }
    from_to_replacement.update(resources_values)

    for from_replacement, to_replacement in from_to_replacement.items():
        deployment_content = deployment_content.replace(from_replacement, to_replacement)

    return deployment_content
