"""Immutable-run gate отказывает при legacy паспорте, TTL, mutation и подмене UUID."""

from copy import deepcopy
import json
from uuid import uuid4

import pytest

from ci_test.test_demand_daily_preparation import config, module
from ci_test.test_demand_restored_writer import bundle

guard = module("restored", "source_guard")


def policy_bundle():
    selected, passport, _ = bundle()
    passport = deepcopy(passport)
    document = json.loads(passport["output_manifest"])
    ids = [str(uuid4()), str(uuid4())]
    document["fp_source_policy"] = {"version": 1, "mode": guard.POLICY,
        "run_id": selected["run_id"], "prediction_date": selected["prediction_date"].isoformat(),
        "producer_code_version": passport["code_version"], "data_table_uuid": ids[0],
        "registry_table_uuid": ids[1], "daily_manifest_sha256": guard.digest(document["fp_daily_copy"])}
    passport["output_manifest"] = json.dumps(document)
    cfg = config("restored")
    rows = [(name, uuid, "ReplicatedReplacingMergeTree", "CREATE TABLE t (x UInt64 COMMENT 'TTL kept') ENGINE = ReplicatedReplacingMergeTree ORDER BY x")
            for name, uuid in zip((cfg["source"]["table"], cfg["source"]["registry_table"]), ids)]
    class Client:
        def __init__(self):
            self.rows, self.mutations, self.calls = rows, [(0,)], []
        def execute(self, sql, *, params):
            self.calls.append((sql, params))
            assert params == {"database": cfg["source"]["database"], "tables": tuple(row[0] for row in rows)}
            return self.rows if sql == guard.TABLES_SQL else self.mutations
    return cfg, selected, passport, Client()


def test_complete_policy_is_rechecked_and_not_inferred_from_status():
    cfg, selected, passport, client = policy_bundle()
    checked = guard.ImmutableRunGuard(cfg, client)
    assert checked(selected, passport) is True
    assert checked(selected, deepcopy(passport)) is True
    assert len(client.calls) == 4


@pytest.mark.parametrize("failure", ["missing", "version", "mode", "run", "cutoff", "code", "digest",
                                    "uuid", "extra", "ttl", "mutation", "missing_table", "view"])
def test_incompatible_source_fails_closed(failure):
    cfg, selected, passport, client = policy_bundle()
    document = json.loads(passport["output_manifest"])
    policy = document["fp_source_policy"]
    if failure == "missing":
        del document["fp_source_policy"]
    elif failure in {"version", "mode", "run", "cutoff", "code", "digest", "uuid", "extra"}:
        key = {"version": "version", "mode": "mode", "run": "run_id", "cutoff": "prediction_date",
               "code": "producer_code_version", "digest": "daily_manifest_sha256", "uuid": "data_table_uuid", "extra": "extra"}[failure]
        policy[key] = True if failure == "version" else "wrong"
    elif failure == "ttl":
        name, uuid, engine, ddl = client.rows[0]
        client.rows[0] = name, uuid, engine, ddl + " TTL x + INTERVAL 1 DAY"
    elif failure == "view":
        name, uuid, _, ddl = client.rows[0]
        client.rows[0] = name, uuid, "View", ddl
    elif failure == "mutation":
        client.mutations = [(1,)]
    else:
        client.rows = client.rows[:1]
    passport["output_manifest"] = json.dumps(document)
    with pytest.raises(ValueError):
        guard.ImmutableRunGuard(cfg, client)(selected, passport)


def test_metadata_change_between_days_blocks():
    cfg, selected, passport, client = policy_bundle()
    checked = guard.ImmutableRunGuard(cfg, client)
    checked(selected, passport)
    name, uuid, engine, ddl = client.rows[0]
    client.rows[0] = name, uuid, engine, ddl + " SETTINGS index_granularity=4096"
    with pytest.raises(ValueError, match="изменились"):
        checked(selected, passport)


def test_ddl_tokenizer_does_not_mistake_comments_or_literals_for_ttl():
    assert "TTL" not in guard.ddl_tokens("CREATE TABLE t (`TTL` String COMMENT 'TTL\\\'x') /* TTL */ -- TTL\n ENGINE=ReplacingMergeTree")
    assert "TTL" in guard.ddl_tokens("CREATE TABLE t (d Date TTL d + INTERVAL 1 DAY) ENGINE=ReplacingMergeTree")
