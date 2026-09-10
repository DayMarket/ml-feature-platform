"""Проверить все 38 SKU полей, полноту справочников и запрет ложных fallback."""

from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
import re
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ci_test.test_demand_catalog_categories_links import category
from ci_test.test_demand_catalog_golden_graph import row as golden
from layers.silver.seller_id.demand_catalog_seller.v1.job import preparation as seller_prep
from layers.silver.sku_id.demand_catalog_sku.v1.job import preparation as prep

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 9, 5, tzinfo=timezone.utc)


def link(sku=1, target=1):
    return dict(meta_sku_id=str(UUID(int=100 + sku)), golden_sku_id=str(UUID(int=target)),
                link_provenance="manual", meta_present=1, meta_source="uzum", source_sku_id=str(sku))


def raw_source():
    fields = [pa.field(name, pa.uint64()) for name in prep.SOURCE_FIELDS[:5]]
    fields += [pa.field("sku_created_at", pa.timestamp("us", "UTC")), pa.field("sku_status", pa.string())]
    rows = [dict(sku_id=1, product_id=101, category_id=10, seller_id=42, shop_id=0,
                 sku_created_at=NOW, sku_status="RAW_STATUS"),
            dict(sku_id=2, product_id=None, category_id=999, seller_id=43, shop_id=None,
                 sku_created_at=None, sku_status=None),
            dict(sku_id=3, product_id=101, category_id=20, seller_id=42, shop_id=5,
                 sku_created_at=NOW, sku_status="")]
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))


def arguments():
    raw = pa.table({"seller_id": [42, 43], "source_master_seller_id": [" master ", ""],
                    "is_1p": pa.array([None, False], pa.bool_()),
                    "seller_registered_at": pa.array([None, NOW], pa.timestamp("us", "UTC"))})
    seller = seller_prep.prepare_catalog(raw, seller_prep.expected_schema(), expected_source_rows=2,
        catalog_version="catalog1", source_manifest_id="sellers1", source_contract_version="seller_v1", ingested_at=NOW)
    bound = dict(date=date(2026, 9, 9), captured_at=NOW,
                 receipt=dict(snapshot_id=123, rows_written=2, catalog_version="catalog1",
                              source_manifest_id="sellers1", source_contract_version="seller_v1"))
    return dict(categories=[category(), category(20, (0, 2, 0, 0, 0, 0))],
                goldens=[golden(1, 2), golden(2, 3), golden(3)], active_links=[link()],
                counts=dict(sku=3, category=2, golden=3, active_links=1, uzum_links=1),
                seller=seller, bound_seller=bound, source_manifest_id="sku1",
                source_contract_version="current_sku_catalog_v1", ingested_at=NOW)


def run(source=None, schema=None, **changes):
    args = arguments() | changes
    return prep.prepare_catalog(raw_source() if source is None else source,
                                prep.expected_schema() if schema is None else schema, **args)


def replace(table, name, values, dtype=None):
    return table.set_column(table.schema.get_field_index(name), name,
                            pa.array(values, type=dtype or table[name].type))


def test_full_contract_and_parquet_roundtrip(tmp_path):
    output, audit = run()
    assert output.schema == prep.expected_schema() and len(output.column_names) == 38
    assert output["sku_id"].to_pylist() == [1, 2, 3]
    first, second, third = output.to_pylist()
    assert first["golden_sku_id"] == str(UUID(int=3)) and first["unit_id"] == "g:" + str(UUID(int=3))
    assert first["source_master_seller_id"] == " master " and first["master_seller_id"] == "master"
    assert first["is_1p"] is None and first["sku_status"] == "RAW_STATUS" and first["shop_id"] == 0
    assert first["sku_created_at"] == NOW.replace(tzinfo=None)
    assert first["raw_l2_category_id"] == 0 and first["raw_l6_category_id"] == 999
    assert first["leaf"] == "leaf:10" and first["l2"] == "l2:1"
    assert second["category_id"] == 999 and second["raw_l1_category_id"] is None
    assert second["category_path_status"] == "missing" and second["unit_id"] == "s:2"
    assert second["golden_mapping_status"] == "unmatched" and second["master_seller_id"] == "43"
    assert second["source_master_seller_id"] == "" and second["has_master"] is False
    assert second["product_id"] is None and second["sku_created_at"] is None and second["sku_status"] is None
    assert third["category_path_status"] == "missing" and third["raw_l2_category_id"] == 2
    assert third["market"] is None and third["unit_id"] == "s:3" and third["sku_status"] == ""
    assert output["catalog_seller_snapshot_id"].to_pylist() == [123] * 3
    assert audit["golden_graph"]["max_merge_hops"] == 2
    assert audit["category_path_status"] == {"valid": 1, "missing": 2}
    path = tmp_path / "sku.parquet"
    pq.write_table(output, path)
    assert pq.read_table(path).equals(output)


def test_migration_and_large_strings_reordered_target():
    ddl = (ROOT / "layers/silver/sku_id/demand_catalog_sku/v1/migrations/create_table.sql").read_text()
    kinds = {"BIGINT": pa.int64(), "STRING": pa.string(), "DATE": pa.date32(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    fields = re.findall(r"^    (\w+) ([A-Z]+)( NOT NULL)? COMMENT ", ddl, re.M)
    assert pa.schema([pa.field(n, kinds[t], nullable=not required) for n, t, required in fields]) == prep.expected_schema()
    schema = pa.schema([f.with_type(pa.large_string()) if pa.types.is_string(f.type) else f
                        for f in reversed(prep.expected_schema())])
    output, _ = run(schema=schema)
    assert output.schema == schema and output["unit_id"][1].as_py() == "s:2"


def test_chunked_inputs_and_unsorted_dimension_rows_keep_sku_order():
    source = raw_source()
    chunked = pa.concat_tables([source.slice(0, 1), source.slice(1, 1), source.slice(2)])
    args = arguments()
    args["seller"] = args["seller"].take(pa.array([1, 0]))
    args["categories"].reverse()
    args["goldens"].reverse()
    output, audit = run(chunked, **args)
    expected, expected_audit = run()
    assert output.equals(expected)
    # Размер validity-буферов зависит от числа chunks, бизнес-аудит — нет.
    assert audit.pop("output_bytes") > 0 and expected_audit.pop("output_bytes") > 0
    assert audit == expected_audit


@pytest.mark.parametrize("field,values,dtype", [
    ("sku_id", [1, 1, 3], None), ("sku_id", [2, 1, 3], None), ("sku_id", [0, 2, 3], None),
    ("sku_id", [None, 2, 3], None), ("sku_id", [1, 2, 2**63], None),
    ("product_id", [1, -1, 2], pa.int64()), ("seller_id", [True, False, True], pa.bool_()),
    ("sku_created_at", [NOW, NOW, NOW], pa.timestamp("us")), ("sku_status", [1, 2, 3], pa.int64())])
def test_bad_raw_fields_rejected(field, values, dtype):
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        run(replace(raw_source(), field, values, dtype))


@pytest.mark.parametrize("key", ["sku", "category", "golden", "active_links", "uzum_links"])
def test_truncated_captures_cannot_become_unmatched(key):
    args = arguments()
    args["counts"][key] += 1
    with pytest.raises(ValueError):
        run(**args)


@pytest.mark.parametrize("value", [None, True, 0, -1, 2**63])
def test_invalid_counts(value):
    args = arguments()
    args["counts"]["sku"] = value
    with pytest.raises(ValueError):
        run(**args)


def test_category_conflict_blocks_used_paths_but_not_unreferenced_categories():
    args = arguments()
    args["categories"] = [category(10, (1, 3, 0, 0, 0, 0)), category(20, (2, 3, 0, 0, 0, 0))]
    with pytest.raises(ValueError, match="category_path_status"):
        run(**args)
    args["categories"] += [category(30)]
    args["counts"]["category"] = 3
    output, audit = run(replace(raw_source(), "category_id", [30, 30, 30]), **args)
    assert output["category_path_status"].to_pylist() == ["valid"] * 3
    assert audit["category"]["conflict_category_rows"] == 2


@pytest.mark.parametrize("kind", ["cycle", "missing_target", "missing_golden", "orphan", "ambiguous"])
def test_mdm_errors_never_produce_fallback(kind):
    args = arguments()
    if kind == "cycle":
        args["goldens"][-1] = golden(3, 1)
    elif kind == "missing_target":
        args["goldens"][-1] = golden(3, 99)
    elif kind == "missing_golden":
        args["active_links"] = [link(target=99)]
    elif kind == "orphan":
        args["active_links"][0].update(meta_present=0, meta_source=None, source_sku_id=None)
    else:
        args["goldens"].append(golden(4))
        args["active_links"].append(link(target=4))
        args["counts"].update(golden=4, active_links=2, uzum_links=2)
    with pytest.raises(ValueError):
        run(**args)


@pytest.mark.parametrize("values", [[42, 99, 42], [42, None, 42], [42, 0, 42]])
def test_missing_seller_is_not_unmatched(values):
    with pytest.raises(ValueError, match="seller_mapping_status"):
        run(replace(raw_source(), "seller_id", values))


@pytest.mark.parametrize("field,values", [("catalog_version", ["other", "catalog1"]),
    ("source_manifest_id", ["other", "sellers1"]), ("seller_id", [42, 42]),
    ("source_contract_version", ["other", "seller_v1"]), ("has_master", [False, False]),
    ("master_seller_id", ["arbitrary", "43"]), ("seller_mapping_status", ["unmatched", "unmatched"])])
def test_invalid_seller_snapshot_payload(field, values):
    args = arguments()
    args["seller"] = replace(args["seller"], field, values)
    with pytest.raises(ValueError):
        run(**args)


def test_unknown_seller_master_blocks_and_unknown_is_1p_does_not():
    args = arguments()
    for field, values in [("source_master_seller_id", [None, ""]), ("master_seller_id", [None, "43"]),
                          ("seller_mapping_status", ["unavailable", "unmatched"]), ("has_master", [None, False])]:
        args["seller"] = replace(args["seller"], field, values)
    with pytest.raises(ValueError, match="seller_mapping_status"):
        run(**args)


def test_capture_date_uses_tashkent_and_seller_version_is_retained():
    output, _ = run(ingested_at=datetime(2026, 9, 9, 20, tzinfo=timezone.utc))
    assert output["date"].to_pylist() == [date(2026, 9, 10)] * 3
    assert output["catalog_version"].to_pylist() == ["catalog1"] * 3
    with pytest.raises(ValueError):
        run(ingested_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError):
        run(ingested_at=datetime(2026, 9, 8, 5, tzinfo=timezone.utc))


def test_missing_or_extra_columns_and_empty_manifest_rejected():
    for source in (raw_source().drop(["sku_status"]), raw_source().append_column("extra", pa.array([1, 2, 3]))):
        with pytest.raises(ValueError):
            run(source)
    for field in ("source_manifest_id", "source_contract_version"):
        with pytest.raises(ValueError):
            run(**{field: " "})
    for name, value in (("snapshot_id", 0), ("rows_written", 3)):
        args = arguments()
        bound = deepcopy(args["bound_seller"])
        bound["receipt"][name] = value
        with pytest.raises(ValueError):
            run(bound_seller=bound)
