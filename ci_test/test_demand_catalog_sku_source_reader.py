"""Проверить полные CH captures, native типы и повторное сравнение содержимого."""

from copy import deepcopy
from datetime import timezone, timedelta
from uuid import UUID

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_catalog_sku_preparation import NOW, ROOT, arguments, raw_source
from layers.silver.sku_id.demand_catalog_sku.v1.job import source_reader as reader
from layers.silver.sku_id.demand_catalog_sku.v1.job.query import capture_query


def config():
    cfg = yaml.safe_load((ROOT / "layers/silver/sku_id/demand_catalog_sku/v1/config.yaml").read_text())
    cfg["runtime"].update(max_batch_rows=2, max_batch_bytes=100000)
    return cfg


class Client:
    def __init__(self, cfg=None):
        self.cfg = cfg or config()
        self.metadata = {
            "sku": list(zip(reader.FIELDS["sku"], ["UInt64", "Int64", "Int64", "Nullable(Int64)",
                         "Nullable(Int64)", "DateTime64(6, 'UTC')", "String"], strict=True)),
            "category": [(name, "String" if name.endswith("title") else "UInt64") for name in reader.FIELDS["category"]],
            "golden": [("golden_sku_id", "UUID"), ("is_merged", "UInt8"), ("merged_into", "UUID")],
            "active_links": list(zip(reader.FIELDS["active_links"], ["UUID", "UUID", "LowCardinality(String)",
                                        "UInt8", "Nullable(String)", "Nullable(String)"], strict=True)),
        }
        args = arguments()
        source = raw_source().to_pylist()
        source[1].update(product_id=0, sku_created_at=NOW, sku_status="")
        captures = dict(sku=source, category=args["categories"], golden=args["goldens"], active_links=args["active_links"])
        self.rows = {kind: [[UUID(row[name]) if dtype == "UUID" else row[name] for name, dtype in self.metadata[kind]]
                           for row in rows] for kind, rows in captures.items()}
        self.queries, self.streams, self.closed = [], [], []
        self.on_stream = lambda kind, number: None

    def execute(self, sql, with_column_types=False):
        self.queries.append(sql)
        for kind in reader.FIELDS:
            if sql == capture_query(self.cfg, kind, metadata_only=True):
                assert with_column_types
                return [], deepcopy(self.metadata[kind])
            if sql == reader.audit_query(self.cfg, kind):
                rows = self.rows[kind]
                if kind == "active_links":
                    return [(len(rows), sum(row[3] == 1 and row[4] == "uzum" for row in rows), sum(row[3] == 0 for row in rows))]
                return [(len(rows), len({row[0] for row in rows}), 0)]
        raise AssertionError(sql)

    def execute_iter(self, sql, *, with_column_types, chunk_size, settings):
        assert with_column_types and chunk_size == settings["max_block_size"] > 0
        kind = next(kind for kind in reader.FIELDS if sql == capture_query(self.cfg, kind))
        self.streams.append(kind)
        self.on_stream(kind, self.streams.count(kind))
        try:
            payload = [deepcopy(self.metadata[kind]), *deepcopy(self.rows[kind])]
            for start in range(0, len(payload), chunk_size):
                yield payload[start:start + chunk_size]
        finally:
            self.closed.append(kind)


def capture(client, **limits):
    meta = reader.read_metadata(client.cfg, client)
    counts = reader.read_counts(client.cfg, client)
    args = dict(max_batch_rows=2, max_batch_bytes=100000) | limits
    result = reader.capture_all(client.cfg, client, counts, meta, **args)
    return result, counts, meta


def test_all_four_sources_and_exact_repeat_with_different_chunks():
    client = Client()
    captures, counts, metadata = capture(client)
    assert counts == arguments()["counts"]
    assert captures["sku"]["product_id"].type == pa.int64()
    assert captures["sku"]["sku_created_at"].type == pa.timestamp("us", "UTC")
    assert captures["golden"]["golden_sku_id"][0].as_py() == str(UUID(int=1))
    assert reader.verify_captures(client.cfg, client, captures, counts, metadata, max_batch_rows=1, max_batch_bytes=100000)
    assert client.streams == list(reader.FIELDS) * 2 and client.closed == client.streams


@pytest.mark.parametrize("kind,position,value", [("sku", 6, "changed status"), ("category", 8, "changed title"),
    ("golden", 1, 0), ("active_links", 2, "changed provenance")])
def test_changed_content_with_same_count_fails_repeat(kind, position, value):
    client = Client()
    captures, counts, metadata = capture(client)
    client.rows[kind][0][position] = value
    assert not reader.verify_captures(client.cfg, client, captures, counts, metadata, max_batch_rows=2, max_batch_bytes=100000)
    assert client.closed == client.streams


@pytest.mark.parametrize("kind,position,value", [("sku", 0, True), ("sku", 6, 123), ("sku", 5, NOW.replace(tzinfo=None)),
    ("sku", 5, NOW.astimezone(timezone(timedelta(hours=5)))), ("sku", 1, None),
    ("golden", 0, str(UUID(int=1))), ("golden", 1, True), ("active_links", 3, True),
    ("category", 0, -1)])
def test_native_values_cannot_be_silently_cast(kind, position, value):
    client = Client()
    client.rows[kind][0][position] = value
    with pytest.raises((ValueError, pa.ArrowInvalid, OverflowError)):
        reader.source_arrow(kind, client.rows[kind], client.metadata[kind])


@pytest.mark.parametrize("kind,position,dtype", [("sku", 1, "UInt64"), ("sku", 0, "Int64"),
    ("sku", 5, "DateTime"), ("golden", 0, "String"), ("golden", 1, "Bool"),
    ("category", 1, "Int64"), ("active_links", 4, "UInt64")])
def test_wrong_source_metadata_fails_before_scan(kind, position, dtype):
    client = Client()
    name, _ = client.metadata[kind][position]
    client.metadata[kind][position] = name, dtype
    with pytest.raises(ValueError):
        reader.read_metadata(client.cfg, client)
    assert not client.streams


@pytest.mark.parametrize("kind", list(reader.FIELDS))
def test_truncated_or_excess_rows_rejected_and_stream_closed(kind):
    for change in (-1, 1):
        client = Client()
        metadata = reader.read_metadata(client.cfg, client)
        stream = reader.read_batches(client.cfg, client, kind, expected_rows=len(client.rows[kind]) + change,
                                      columns=metadata[kind], max_batch_rows=1, max_batch_bytes=100000)
        with pytest.raises(ValueError):
            list(stream)
        assert client.closed == client.streams


def test_changed_header_after_preflight_blocks():
    client = Client()
    metadata = reader.read_metadata(client.cfg, client)
    client.metadata["sku"][6] = "sku_status", "LowCardinality(String)"
    with pytest.raises(ValueError, match="изменилась"):
        list(reader.read_batches(client.cfg, client, "sku", expected_rows=3, columns=metadata["sku"],
                                  max_batch_rows=2, max_batch_bytes=100000))
    assert client.closed == ["sku"]


def test_order_across_batches_and_byte_limit():
    client = Client()
    client.rows["sku"] = list(reversed(client.rows["sku"]))
    with pytest.raises(ValueError, match="неупорядоченный"):
        capture(client, max_batch_rows=1)
    assert client.closed == client.streams
    client = Client()
    with pytest.raises(ValueError, match="лимит Arrow"):
        capture(client, max_batch_bytes=1)
    assert client.closed == client.streams


def test_orphan_audit_blocks_before_large_capture():
    client = Client()
    client.rows["active_links"][0][3] = 0
    with pytest.raises(ValueError, match="orphan"):
        capture(client)
    assert not client.streams


def test_audits_use_declared_sources_and_no_provenance_filter():
    cfg = config()
    assert "FINAL" in reader.audit_query(cfg, "golden")
    sql = reader.audit_query(cfg, "active_links")
    assert "dictHas(" in sql and "'source', meta_sku_id) = 'uzum'" in sql
    assert "deleted_at IS NULL" in sql and "source = 'uzum'" not in sql
    assert "LIMIT" not in reader.audit_query(cfg, "sku")
    cfg["source"]["sku"] = "invalid.schema.table"
    with pytest.raises(ValueError):
        reader.audit_query(cfg, "sku")
