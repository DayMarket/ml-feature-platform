import hashlib
import json
import os
import random
import re
import string
from typing import Any, Dict

from airflow.sdk import BaseHook


def _dag_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _repo_root() -> str:
    return os.path.abspath(os.path.join(_dag_root(), "..", "..", ".."))


def _repo_relative_dag_root() -> str:
    return os.path.relpath(_dag_root(), _repo_root())


def _shared_upload_root() -> str:
    return os.path.join(_repo_root(), "upload", "features_service_upload", "v1")


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


def _sanitize_task_id(value: str) -> str:
    task_id = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
    return task_id or "default"


def _feature_group_catalog(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(feature_group["name"]): feature_group
        for feature_group in config["feature_groups"]
    }


def _model_feature_groups(
    model: dict[str, Any],
    catalog: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    requested_groups: dict[str, list[str]] = {}
    for raw_group in model.get("feature_groups", []):
        if isinstance(raw_group, str):
            group_name = raw_group
            features = catalog[group_name]["features"]
        else:
            group_name = str(raw_group["name"])
            features = raw_group.get("features", catalog[group_name]["features"])
        requested_groups[group_name] = [str(feature) for feature in features]
    return requested_groups


def _component_id(model_names: list[str]) -> str:
    if len(model_names) == 1:
        return _sanitize_task_id(model_names[0])
    component_hash = hashlib.sha1(
        ",".join(sorted(model_names)).encode("utf-8")
    ).hexdigest()[:8]
    return f"component_{component_hash}"


def _build_model_components(config: dict[str, Any]) -> list[dict[str, Any]]:
    catalog = _feature_group_catalog(config)
    models = config["models"]
    model_names = [str(model["name"]) for model in models]
    parent = {model_name: model_name for model_name in model_names}

    def find(model_name: str) -> str:
        while parent[model_name] != model_name:
            parent[model_name] = parent[parent[model_name]]
            model_name = parent[model_name]
        return model_name

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    model_group_requests = {
        str(model["name"]): _model_feature_groups(model, catalog)
        for model in models
    }
    feature_group_models: dict[str, list[str]] = {}
    for model_name, requested_groups in model_group_requests.items():
        for group_name in requested_groups:
            feature_group_models.setdefault(group_name, []).append(model_name)

    for sharing_models in feature_group_models.values():
        first_model = sharing_models[0]
        for model_name in sharing_models[1:]:
            union(first_model, model_name)

    components_by_root: dict[str, list[str]] = {}
    for model_name in model_names:
        components_by_root.setdefault(find(model_name), []).append(model_name)

    components = []
    for component_models in components_by_root.values():
        requested_features_by_group: dict[str, set[str]] = {}
        for model_name in component_models:
            for group_name, features in model_group_requests[model_name].items():
                requested_features_by_group.setdefault(group_name, set()).update(features)

        component_groups = []
        for group_name in sorted(requested_features_by_group):
            catalog_features = [str(feature) for feature in catalog[group_name]["features"]]
            requested_features = requested_features_by_group[group_name]
            component_groups.append(
                {
                    "name": group_name,
                    "features": [
                        feature
                        for feature in catalog_features
                        if feature in requested_features
                    ],
                }
            )

        components.append(
            {
                "id": _component_id(component_models),
                "models": component_models,
                "feature_groups": component_groups,
            }
        )

    return sorted(components, key=lambda component: model_names.index(component["models"][0]))


def _source_dependencies(
    component_id: str,
    feature_groups: list[dict[str, Any]],
    catalog: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    dependencies = []
    seen_tables = set()
    for feature_group in feature_groups:
        source = catalog[feature_group["name"]]["source"]
        source_key = (source["schema"], source["table"])
        if source_key in seen_tables:
            continue
        seen_tables.add(source_key)
        dependencies.append(
            {
                "task_id": (
                    f"wait_for_{component_id}_"
                    f"{_sanitize_task_id(str(source['table']))}"
                ),
                "external_dag_id": str(
                    source.get("dependency_dag_id")
                    or (
                        f"dbt.source.trino.ml_feature_platform_{source['schema']}."
                        f"{source['table']}.dq"
                    )
                ),
                "execution_delta_minutes": int(
                    source.get(
                        "dependency_execution_delta_minutes",
                        source.get("dq_execution_delta_minutes", 60),
                    )
                ),
            }
        )
    return dependencies


def get_upload_components() -> list[dict[str, Any]]:
    config = get_config()
    catalog = _feature_group_catalog(config)
    components = _build_model_components(config)
    for component in components:
        component["dependencies"] = _source_dependencies(
            component["id"],
            component["feature_groups"],
            catalog,
        )
        component["feature_groups_argument"] = json.dumps(
            component["feature_groups"],
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return components


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


def get_deployment(feature_groups_argument: str = "") -> str:
    deployment_path = os.path.join(
        _shared_upload_root(),
        "config",
        "upload_ranking_features.yaml",
    )
    with open(deployment_path, "r", encoding="utf-8") as deployment_file:
        deployment_content = deployment_file.read()

    config = get_config()
    resources_path = os.path.join(_dag_root(), config["resources"]["path"])
    resources = _load_json(resources_path)
    s3_connection = json.loads(
        BaseHook.get_connection("spark_ycs_connection").extra
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
        "<config_path>": f"/git/repo/{_repo_relative_dag_root()}/config.yaml",
        "<feature_groups>": feature_groups_argument,
        "<kafka_topic>": str(config["kafka"]["topic"]),
        "<kafka_brokers>": _kafka_brokers(kafka_connection),
        "<kafka_login>": str(kafka_connection.login or ""),
        "<kafka_password>": str(kafka_connection.password or ""),
    }
    for source, target in replacements.items():
        deployment_content = deployment_content.replace(source, target)
    return deployment_content
