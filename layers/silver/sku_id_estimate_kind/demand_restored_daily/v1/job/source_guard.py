"""Проверить immutable-run декларацию producer и фактическую CH metadata до переноса."""

from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
import json
import re
from uuid import UUID

from .manifest import day_manifest, unique_object
from .query import source_ref
from .ranges import validate_request
from .runtime import read_run

POLICY = "immutable_completed_run_v1"
TABLES_SQL = ("SELECT name, toString(uuid), engine, create_table_query FROM system.tables "
              "WHERE database = %(database)s AND name IN %(tables)s")
MUTATIONS_SQL = ("SELECT count() FROM system.mutations WHERE database = %(database)s "
                 "AND table IN %(tables)s AND is_done = 0")


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=True, allow_nan=False).encode()).hexdigest()


def ddl_tokens(ddl):
    if not isinstance(ddl, str) or not ddl.strip():
        raise ValueError("Нет CH CREATE TABLE metadata")
    # Литералы и quoted identifiers не являются ключевыми словами TTL.
    pattern = r"'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\"|`(?:\\.|``|[^`\\])*`|--[^\n]*|/\*.*?\*/|[A-Za-z_][A-Za-z0-9_]*"
    return [token.upper() for token in re.findall(pattern, ddl, re.S)
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token)]


class ImmutableRunGuard:
    """Не создаёт блокировку: требует договор producer, отсутствие TTL и стабильные UUID/DDL."""

    def __init__(self, config, client):
        source_ref(config)
        source_ref(config, registry=True)
        self.config, self.client, self.baseline = config, client, None

    def __call__(self, selected, passport):
        day_manifest(self.config, selected, passport, selected["start"])
        document = json.loads(passport["output_manifest"], object_pairs_hook=unique_object)
        policy = document.get("fp_source_policy")
        required = {"version", "mode", "run_id", "prediction_date", "producer_code_version",
                    "data_table_uuid", "registry_table_uuid", "daily_manifest_sha256"}
        if not isinstance(policy, dict) or set(policy) != required:
            raise ValueError("Нет полного fp_source_policy: нужен новый неизменяемый E3 producer")
        if (type(policy["version"]) is not int or policy["version"] != 1 or policy["mode"] != POLICY
                or policy["run_id"] != selected["run_id"]
                or policy["prediction_date"] != selected["prediction_date"].isoformat()
                or policy["producer_code_version"] != passport["code_version"]
                or policy["daily_manifest_sha256"] != digest(document["fp_daily_copy"])):
            raise ValueError("Immutable policy не соответствует точному E3 run/manifest")
        source = self.config["source"]
        names = (source["table"], source["registry_table"])
        if len(set(names)) != 2:
            raise ValueError("Нужны разные result/registry таблицы")
        params = {"database": source["database"], "tables": names}
        rows = self.client.execute(TABLES_SQL, params=params)
        if (not isinstance(rows, (list, tuple)) or len(rows) != 2
                or any(not isinstance(row, (list, tuple)) or len(row) != 4 for row in rows)
                or {row[0] for row in rows} != set(names)):
            raise ValueError("Не видны обе CH таблицы immutable-run")
        metadata = {}
        for name, uuid, engine, ddl in rows:
            key = "data_table_uuid" if name == source["table"] else "registry_table_uuid"
            if (not isinstance(uuid, str) or not isinstance(policy[key], str)
                    or UUID(uuid).int == 0 or str(UUID(uuid)) != policy[key]):
                raise ValueError("UUID CH таблицы не соответствует producer policy")
            tokens = ddl_tokens(ddl)
            if (engine not in {"ReplacingMergeTree", "ReplicatedReplacingMergeTree"}
                    or not {"CREATE", "TABLE", "ENGINE"}.issubset(tokens) or "TTL" in tokens):
                raise ValueError("CH result/registry требует MergeTree без TTL")
            metadata[name] = (uuid, engine, ddl)
        mutations = self.client.execute(MUTATIONS_SQL, params=params)
        if (not isinstance(mutations, (list, tuple)) or len(mutations) != 1
                or not isinstance(mutations[0], (list, tuple))
                or len(mutations[0]) != 1 or type(mutations[0][0]) is not int or mutations[0][0] != 0):
            raise ValueError("Есть незавершённые CH mutations или их состояние неизвестно")
        identity = (selected["run_id"], selected["prediction_date"], passport, metadata)
        if self.baseline is not None and identity != self.baseline:
            raise ValueError("E3 паспорт или CH metadata изменились во время переноса")
        self.baseline = deepcopy(identity)
        return True


@contextmanager
def checked_output(config, context):
    """Повторить source policy до и после terminal DQ/stats, не использовать task status."""
    from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

    request = context["ti"].xcom_pull(task_ids="prepare_request", include_prior_dates=False)
    written = context["ti"].xcom_pull(task_ids="write_range", include_prior_dates=False)
    selected = validate_request(config, request)[0]
    with ClickHouseHook(clickhouse_conn_id=config["source"]["conn_id"], use_numpy=False).get_conn() as client:
        guard = ImmutableRunGuard(config, client)
        def verify():
            passport = read_run(config, client, selected)
            if guard(selected, passport) is not True:
                raise ValueError("Source policy не подтверждена")
            identity = day_manifest(config, selected, passport, selected["start"])
            expected = written.get("source_identity") if isinstance(written, dict) else None
            keys = ("source_run_id", "source_prediction_date", "source_state_version", "output_manifest_sha256")
            if expected != {key: identity[key] for key in keys}:
                raise ValueError("Source policy не соответствует записанному диапазону")
        verify()
        yield
        verify()
