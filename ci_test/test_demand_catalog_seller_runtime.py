"""Проверить полный CH capture seller-каталога и отказ при изменении источника."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import IdentityTransform

from layers.silver.seller_id.demand_catalog_seller.v1.job import preparation as prep, runtime

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/seller_id/demand_catalog_seller/v1"
NOW = datetime(2026, 9, 8, 21, 0, 0, 123456, tzinfo=timezone.utc)
COLUMNS = [("seller_id", "UInt64"), ("source_master_seller_id", "Nullable(String)"),
           ("is_1p", "Nullable(Bool)"), ("seller_registered_at", "Nullable(DateTime64(6, 'UTC'))")]
ROWS = [(1, " master-a ", True, NOW), (2, "", False, NOW), (3, None, None, None)]


class Source:
    def __init__(self, rows=ROWS):
        self.rows = list(rows)
        self.columns = deepcopy(COLUMNS)
        self.audit = None
        self.queries = []
        self.streams = 0
        self.closed = 0
        self.repeat_rows = None
        self.repeat_columns = None
        self.fail_on_repeat = False

    def execute(self, sql, **kwargs):
        self.queries.append((sql, kwargs))
        if " LIMIT 0 " in sql:
            return [], deepcopy(self.columns)
        if self.audit is not None:
            return self.audit
        return [(len(self.rows), len({row[0] for row in self.rows}), 0)]

    def execute_iter(self, sql, **kwargs):
        self.queries.append((sql, kwargs))
        self.streams += 1
        repeat = self.streams > 1
        rows = self.repeat_rows if repeat and self.repeat_rows is not None else self.rows
        columns = self.repeat_columns if repeat and self.repeat_columns is not None else self.columns
        chunk_size = kwargs["chunk_size"]
        assert chunk_size == kwargs["settings"]["max_block_size"]

        def stream():
            try:
                buffer = [columns]
                for index, row in enumerate(rows):
                    if repeat and self.fail_on_repeat and index == 1:
                        raise RuntimeError("source network failure")
                    buffer.append(row)
                    if len(buffer) == chunk_size:
                        yield buffer
                        buffer = []
                if buffer:
                    yield buffer
            finally:
                self.closed += 1
        return stream()


@pytest.fixture
def env(tmp_path, monkeypatch):
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    cfg["runtime"]["max_batch_rows"] = 2
    cat = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                     warehouse=(tmp_path / "warehouse").as_uri())
    cat.create_namespace("silver")
    table = cat.create_table(prep.target_ref(cfg, cat.name), schema=prep.expected_schema())
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    monkeypatch.setattr(runtime, "utc_now", lambda: NOW)
    yield cfg, cat
    cat.engine.dispose()


def load(env, source, **kwargs):
    return runtime.load_catalog(*env, source, catalog_version="catalog-1", source_manifest_id="capture-1", **kwargs)


def target(env):
    return env[1].load_table(prep.target_ref(env[0], env[1].name))


def test_complete_capture_receipt_and_repeat_check(env):
    src = Source()
    receipt = load(env, src)
    assert receipt["status"] == "written" and receipt["rows_written"] == 3
    assert src.streams == src.closed == 2
    assert sum("rows_count" in sql for sql, _ in src.queries) == 3
    assert json.loads(json.dumps(receipt)) == receipt
    assert receipt["date_min"] == "2026-09-09"
    assert receipt["source_audit"]["rows_count"] == 3
    assert receipt["source_audit"]["source_columns"] == [list(item) for item in COLUMNS]
    assert len(receipt["source_audit"]["content_sha256"]) == 64
    actual = target(env).scan().to_arrow().sort_by([("seller_id", "ascending")]).to_pylist()
    assert [r["seller_mapping_status"] for r in actual] == ["matched", "unmatched", "unavailable"]
    assert actual[-1]["master_seller_id"] is None and actual[-1]["seller_registered_at"] is None
    assert actual[0]["source_master_seller_id"] == " master-a "
    assert actual[0]["seller_registered_at"] == NOW.replace(tzinfo=None)
    assert set(target(env).refs()) == {"main"}


@pytest.mark.parametrize("column,value", [(1, "new-master"), (2, False), (3, NOW + timedelta(seconds=1))])
def test_same_count_changed_payload_aborts_before_commit(env, column, value):
    load(env, Source())
    before = target(env).current_snapshot().snapshot_id
    src = Source()
    changed = list(ROWS[0])
    changed[column] = value
    src.repeat_rows = [tuple(changed), *ROWS[1:]]
    with pytest.raises(ValueError, match="Source capture"):
        load(env, src)
    assert src.closed == 2
    assert target(env).current_snapshot().snapshot_id == before


def test_network_failure_during_precommit_check_preserves_previous_capture(env):
    load(env, Source())
    before = target(env).current_snapshot().snapshot_id
    src = Source()
    src.fail_on_repeat = True
    with pytest.raises(RuntimeError, match="network failure"):
        load(env, src)
    assert src.closed == 2
    assert target(env).current_snapshot().snapshot_id == before


@pytest.mark.parametrize("rows", [[], [ROWS[0]], [*ROWS, (4, "x", True, NOW)],
                                  [ROWS[0], ROWS[1], ROWS[1]], [ROWS[1], ROWS[0], ROWS[2]]])
def test_truncated_extra_duplicate_or_unordered_source_is_rejected(env, rows):
    src = Source(rows)
    src.audit = [(3, 3, 0)]
    with pytest.raises(ValueError):
        load(env, src)
    assert src.streams == src.closed == 1
    assert target(env).current_snapshot() is None


@pytest.mark.parametrize("audit", [[], [None], [(0, 0, 0)], [(3, 2, 0)], [(3, 3, 1)],
                                    [(True, 1, 0)], [(3.0, 3, 0)], [(3, 3)]])
def test_invalid_source_audit_rejected_before_stream(env, audit):
    src = Source()
    src.audit = audit
    with pytest.raises(ValueError, match="audit"):
        load(env, src)
    assert src.streams == 0
    assert target(env).current_snapshot() is None


@pytest.mark.parametrize("field,value", [(0, 1.0), (0, True), (0, None), (0, 2**63),
    (1, 1), (2, 1), (3, NOW.replace(tzinfo=None)), (3, NOW.isoformat())])
def test_native_source_types_not_coerced(env, field, value):
    row = list(ROWS[0])
    row[field] = value
    src = Source([tuple(row), *ROWS[1:]])
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        load(env, src)
    assert src.streams == src.closed == 1
    assert target(env).current_snapshot() is None


@pytest.mark.parametrize("columns", [None, [], COLUMNS[:-1], [COLUMNS[0]] * 4,
    [("seller_id", "Float64"), *COLUMNS[1:]],
    [*COLUMNS[:2], ("is_1p", "UInt8"), COLUMNS[-1]],
    [*COLUMNS[:3], ("seller_registered_at", "Nullable(DateTime64(6, 'Asia/Tashkent'))")]])
def test_bad_source_schema_rejected_before_audit(env, columns):
    src = Source()
    src.columns = columns
    with pytest.raises(ValueError):
        load(env, src)
    assert len(src.queries) == 1 and src.streams == 0


def test_repeat_metadata_change_is_not_silent(env):
    src = Source()
    src.repeat_columns = [("seller_id", "Nullable(UInt64)"), *COLUMNS[1:]]
    with pytest.raises(ValueError, match="metadata изменилась"):
        load(env, src)
    assert src.closed == 2
    assert target(env).current_snapshot() is None


@pytest.mark.parametrize("key,value", [("max_batch_rows", 0), ("max_batch_bytes", None),
    ("max_catalog_bytes", True)])
def test_invalid_limits_fail_before_connections_queries(env, key, value):
    env[0]["runtime"][key] = value
    src = Source()
    with pytest.raises(ValueError, match="лимиты"):
        load(env, src)
    assert not src.queries


@pytest.mark.parametrize("key", ["max_batch_bytes", "max_catalog_bytes"])
def test_memory_limit_is_failure_not_partial_catalog(env, key):
    env[0]["runtime"][key] = 1
    src = Source()
    with pytest.raises(ValueError, match="лимит памяти"):
        load(env, src)
    assert src.streams == src.closed == 1
    assert target(env).current_snapshot() is None


def test_missing_target_fails_before_source_queries(env):
    env[0]["table"]["name"] = "missing"
    src = Source()
    with pytest.raises(ValueError, match="миграцию"):
        load(env, src)
    assert not src.queries


def test_repeat_uses_exact_all_fields_and_digest_ignores_batch_size(env):
    small = load(env, Source())
    env[0]["runtime"]["max_batch_rows"] = 3
    larger = load(env, Source())
    assert small["source_audit"]["content_sha256"] == larger["source_audit"]["content_sha256"]
    assert target(env).scan().to_arrow().num_rows == 3


def test_target_changed_during_extraction_is_not_replaced(env, monkeypatch):
    load(env, Source())
    prepare = runtime.prepare_catalog
    newer = None

    def changed(*args, **kwargs):
        nonlocal newer
        table = target(env)
        with table.transaction() as transaction:
            transaction.set_properties({"external-change": "1"})
        newer = table.metadata_location
        return prepare(*args, **kwargs)

    monkeypatch.setattr(runtime, "prepare_catalog", changed)
    with pytest.raises(ValueError, match="изменилась во время извлечения"):
        load(env, Source())
    assert target(env).metadata_location == newer


def test_source_count_change_before_commit_aborts(env):
    src = Source()
    original = src.execute

    def execute(sql, **kwargs):
        if src.streams:
            return [(4, 4, 0)]
        return original(sql, **kwargs)

    src.execute = execute
    with pytest.raises(ValueError, match="Source capture"):
        load(env, src)
    assert target(env).current_snapshot() is None


def test_returned_source_type_utc_converts_same_instant():
    rows = [(1, None, None, NOW.astimezone(timezone(timedelta(hours=5))))]
    raw = runtime.source_arrow(rows, COLUMNS)
    assert raw["seller_registered_at"][0].as_py() == NOW


def test_wrong_source_never_reaches_client(env):
    env[0]["source"]["table"] = "other"
    src = Mock()
    with pytest.raises(ValueError, match="marts.sellers_info"):
        load(env, src)
    src.execute.assert_not_called()
