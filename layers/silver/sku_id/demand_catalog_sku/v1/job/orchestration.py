"""Открыть CH/Trino Connections и связать SKU capture с exact seller DQ XCom."""

from contextlib import ExitStack

from dq.config import load_dq_settings
from dq.results_writer import load_results_catalog
from feature_stats.config import load_feature_stats_settings

from .inputs import validate_reference
from .runtime import execute_load, validate_arguments


def connection_ids(config):
    dq, stats = load_dq_settings(config), load_feature_stats_settings(config)
    names = (config["source"].get("conn_id"), config["dq"].get("trino_conn_id"),
             config.get("feature_stats", {}).get("trino_conn_id"))
    if (any(not isinstance(name, str) or not name.strip() or name != name.strip() for name in names)
            or names[1] != names[2] or (dq.trino_conn_id, stats.trino_conn_id) != names[1:]):
        raise ValueError("Нужны явные CH и общий Trino connection для seller/DQ/stats")
    if (dq.scope != "partition" or dq.partition_column != "ingested_at"
            or dq.partition_granularity != "timestamp" or not stats.enabled):
        raise ValueError("SKU требует DQ и stats точного времени захвата")
    return names[0], names[1]


def read_checked(task_instance, source, reference):
    validate_reference(source, reference)
    payload = task_instance.xcom_pull(dag_id=reference["dag_id"], task_ids="dq", run_id=reference["run_id"],
                                      include_prior_dates=False)
    if (not isinstance(payload, dict) or payload.get("dq_status") != "passed"
            or any(payload.get(key) != reference[key] for key in reference)):
        raise ValueError("Нет passed DQ payload точного seller run, latest запрещён")
    return payload


def execute_capture(config, repo_root, *, reference, source_manifest_id, task_instance=None,
                    get_checked=None, catalog=None, client=None, connection=None, ingested_at=None):
    """Закрывает только созданные здесь клиенты; источники проверяются runtime до commit."""
    captured, _, source, _ = validate_arguments(config, repo_root, reference, source_manifest_id, ingested_at)
    ch_name, trino_name = connection_ids(config)
    if get_checked is None:
        if task_instance is None:
            from airflow.sdk import get_current_context
            task_instance = get_current_context()["ti"]
        def get_checked(ref):
            return read_checked(task_instance, source, ref)
    if not callable(get_checked):
        raise ValueError("Нужен exact seller DQ getter")
    with ExitStack() as stack:
        if catalog is None:
            catalog = load_results_catalog(config["table"]["catalog"])
        if client is None:
            from airflow_commons.hooks.clickhouse_hook import ClickHouseHook
            client = stack.enter_context(ClickHouseHook(clickhouse_conn_id=ch_name, use_numpy=False).get_conn())
        if connection is None:
            from airflow.providers.trino.hooks.trino import TrinoHook
            connection = TrinoHook(trino_conn_id=trino_name).get_conn()
            stack.callback(connection.close)
        return execute_load(config, repo_root, catalog=catalog, client=client, connection=connection,
                            reference=reference, get_checked=get_checked, source_manifest_id=source_manifest_id,
                            ingested_at=captured)
