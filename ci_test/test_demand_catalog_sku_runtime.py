"""Проверить полный SKU runtime на локальном Iceberg и типизированных doubles источников."""

from copy import deepcopy
import json
import re
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_catalog_sku_inputs import env as input_env  # noqa: F401
from ci_test.test_demand_catalog_sku_preparation import NOW, ROOT
from ci_test.test_demand_catalog_sku_source_reader import Client
from layers.silver.sku_id.demand_catalog_sku.v1.job import inputs, preparation as prep, runtime, seller_reader


def description(schema):
    kinds = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.bool_(): "boolean",
             pa.timestamp("us"): "timestamp(6)", pa.float64(): "double"}
    return [(field.name, "varchar" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
             else kinds[field.type], None, None, None, None, None) for field in schema]


class Trino:
    """DB-API double: читает реальные локальные файлы только запрошенного snapshot."""
    def __init__(self, catalog):
        self.catalog = catalog
        self.queries, self.closed = [], []
        self.transform = lambda rows: rows
        self.description_transform = lambda values: values

    def cursor(self):
        owner = self
        class Cursor:
            def execute(self, sql):
                owner.queries.append(sql)
                match = re.search(r'FROM "[^"]+"\."([^"]+)"\."([^"]+)"', sql)
                assert match, sql
                table = owner.catalog.load_table((match[1], match[2]))
                version = re.search(r"FOR VERSION AS OF ([0-9]+)", sql)
                snapshot_id = int(version[1]) if version else None
                schema = (table.schemas()[table.snapshot_by_id(snapshot_id).schema_id].as_arrow()
                          if version else table.schema().as_arrow())
                self.description = owner.description_transform(description(schema))
                if "LIMIT 0" in sql:
                    self.rows = []
                else:
                    assert version and sql.endswith('ORDER BY "seller_id"')
                    batch = table.scan(snapshot_id=snapshot_id).to_arrow().select(schema.names).sort_by([("seller_id", "ascending")])
                    self.rows = owner.transform([list(row.values()) for row in batch.to_pylist()])

            def fetchmany(self, size):
                result, self.rows = self.rows[:size], self.rows[size:]
                return result

            def close(self):
                owner.closed.append(self)
        return Cursor()


@pytest.fixture
def env(input_env):  # noqa: F811
    cfg, src, schema, catalog, seller_table, reference, checked = input_env
    cfg["runtime"].update(max_batch_rows=1, max_batch_bytes=100000)
    table = catalog.create_table(prep.target_ref(cfg, catalog.name), schema=prep.expected_schema())
    from pyiceberg.transforms import IdentityTransform
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    for relative in ("dq/results", "feature_stats/results"):
        path = ROOT / relative
        service = yaml.safe_load((path / "config.yaml").read_text())
        catalog.create_table(prep.target_ref(service, catalog.name), schema=inputs.migration_schema(path))
    return dict(cfg=cfg, src=src, schema=schema, catalog=catalog, seller=seller_table, reference=reference,
                checked=checked, client=Client(cfg), connection=Trino(catalog))


def execute(env, **changes):
    args = dict(catalog=env["catalog"], client=env["client"], connection=env["connection"], reference=env["reference"],
                get_checked=lambda ref: env["checked"], source_manifest_id="raw-capture", ingested_at=NOW)
    return runtime.execute_load(env["cfg"], ROOT, **(args | changes))


def output(env):
    return env["catalog"].load_table(prep.target_ref(env["cfg"], env["catalog"].name))


def test_full_load_repeat_all_sources_and_exact_receipt(env):
    getter = Mock(side_effect=lambda ref: env["checked"])
    result = execute(env, get_checked=getter)
    assert result["status"] == "written" and result["rows_written"] == 3
    assert json.loads(json.dumps(result)) == result
    assert getter.call_count == 3
    assert all(call.args == (env["reference"],) for call in getter.call_args_list)
    assert env["client"].streams == ["sku", "category", "golden", "active_links"] * 2
    assert env["client"].closed == env["client"].streams
    assert len(env["connection"].closed) == len(env["connection"].queries) == 5
    assert result["source_audit"]["seller"]["receipt"] == env["checked"]["receipt"]
    assert result["source_audit"]["captures"]["sku"]["rows_count"] == 3
    batch = output(env).scan().to_arrow().sort_by([("sku_id", "ascending")])
    assert batch["golden_mapping_status"].to_pylist() == ["matched", "unmatched", "unmatched"]
    assert batch["is_1p"].to_pylist() == [None, False, None]
    assert set(output(env).refs()) == {"main"}


def test_prior_snapshot_and_prior_schema_are_used(env):
    from pyiceberg.types import StringType
    table = env["seller"]
    with table.update_schema() as update:
        update.add_column("new_field", StringType())
    original = table.scan(snapshot_id=env["checked"]["receipt"]["snapshot_id"]).to_arrow()
    batch = original.set_column(original.schema.get_field_index("is_1p"), "is_1p", pa.array([True, True]))
    batch = batch.append_column("new_field", pa.array([None, None], pa.large_string()))
    table.overwrite(batch.cast(table.schema().as_arrow()))
    result = execute(env)
    assert result["catalog_seller_snapshot_id"] != table.current_snapshot().snapshot_id
    assert output(env).scan().to_arrow().sort_by([("sku_id", "ascending")])["is_1p"].to_pylist() == [None, False, None]


@pytest.mark.parametrize("phase", [2, 3])
def test_failed_exact_seller_dq_before_or_after_source_recheck_preserves_old_snapshot(env, phase):
    initial = execute(env)
    failed = deepcopy(env["checked"])
    failed["dq_status"] = "failed"
    getter = Mock(side_effect=[env["checked"]] * (phase - 1) + [failed])
    with pytest.raises(ValueError, match="passed DQ"):
        execute(env, get_checked=getter)
    assert output(env).current_snapshot().snapshot_id == initial["snapshot_id"]


def test_same_count_content_change_blocks_commit(env):
    initial = execute(env)
    def mutate(kind, number):
        if kind == "sku" and number == 4:
            env["client"].rows["sku"][0][-1] = "changed between captures"
    env["client"].on_stream = mutate
    with pytest.raises(ValueError, match="перед commit"):
        execute(env)
    assert output(env).current_snapshot().snapshot_id == initial["snapshot_id"]
    assert env["client"].closed == env["client"].streams


@pytest.mark.parametrize("fault", ["missing_service", "wrong_service_schema", "failed_dq", "source_type", "orphan"])
def test_early_preflight_rejects_before_ch_capture(env, fault):
    if fault == "missing_service":
        env["catalog"].drop_table(("silver", "feature_platform_dq_results"))
    elif fault == "wrong_service_schema":
        table = env["catalog"].load_table(("silver", "feature_platform_dq_results"))
        with table.update_schema() as update:
            update.delete_column("test_name")
    elif fault == "failed_dq":
        env["checked"]["dq_status"] = "failed"
    elif fault == "source_type":
        env["client"].metadata["golden"][0] = "golden_sku_id", "String"
    else:
        env["client"].rows["active_links"][0][3] = 0
    with pytest.raises(ValueError):
        execute(env)
    assert not env["client"].streams and output(env).current_snapshot() is None
    assert len(env["connection"].closed) == len(env["connection"].queries)


@pytest.mark.parametrize("fault", ["missing_row", "duplicate", "wrong_bool", "wrong_capture"])
def test_bad_seller_payload_blocks_before_ch_capture(env, fault):
    def mutate(rows):
        if fault == "missing_row":
            return rows[:1]
        if fault == "duplicate":
            return [rows[0], rows[0]]
        field = "is_1p" if fault == "wrong_bool" else "catalog_version"
        position = env["schema"].get_field_index(field)
        rows[0][position] = 1 if fault == "wrong_bool" else "other"
        return rows
    env["connection"].transform = mutate
    with pytest.raises(ValueError):
        execute(env)
    assert not env["client"].streams and output(env).current_snapshot() is None
    assert len(env["connection"].closed) == len(env["connection"].queries)


@pytest.mark.parametrize("field,value", [("max_batch_rows", 0), ("max_batch_bytes", True)])
def test_bad_limits_before_io(env, field, value):
    env["cfg"]["runtime"][field] = value
    with pytest.raises(ValueError):
        execute(env)
    assert not env["client"].queries and not env["connection"].queries


def test_seller_limit_and_metadata_native_types(env):
    bound = inputs.bind_source(env["src"], env["reference"], env["checked"], captured_at=NOW)
    with pytest.raises(ValueError, match="порция"):
        seller_reader.read_seller(env["src"], ROOT, env["connection"], bound, env["schema"], max_batch_rows=1, max_batch_bytes=1)
    env["connection"].description_transform = lambda values: [(v[0], "varchar", *v[2:]) for v in values]
    with pytest.raises(ValueError, match="Trino тип"):
        seller_reader.read_seller(env["src"], ROOT, env["connection"], bound, env["schema"], max_batch_rows=1, max_batch_bytes=10000)
    assert len(env["connection"].closed) == len(env["connection"].queries)


def test_passed_dq_with_changed_receipt_cannot_replace_bound_input(env):
    initial = execute(env)
    changed = deepcopy(env["checked"])
    changed["receipt"]["catalog_version"] = "another-valid-version"
    getter = Mock(side_effect=[env["checked"], changed])
    with pytest.raises(ValueError, match="payload выбранного seller run изменился"):
        execute(env, get_checked=getter)
    assert output(env).current_snapshot().snapshot_id == initial["snapshot_id"]


def test_target_changed_during_source_extraction_is_preserved(env):
    execute(env)
    competing = []
    def mutate(kind, number):
        if kind == "active_links" and number == 3:
            table = output(env)
            table.overwrite(table.scan().to_arrow().slice(0, 1))
            competing.append(table.current_snapshot().snapshot_id)
    env["client"].on_stream = mutate
    with pytest.raises(ValueError, match="target изменился"):
        execute(env)
    assert competing and output(env).current_snapshot().snapshot_id == competing[0]
    assert output(env).scan().to_arrow().num_rows == 1
    assert env["client"].closed == env["client"].streams
