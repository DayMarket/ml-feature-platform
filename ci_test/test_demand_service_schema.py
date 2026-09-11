"""Полная схема служебной таблицы обязательна даже при пустом target."""

from importlib import import_module
from pathlib import Path

import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(params=['layers.silver.sku_id.demand_stock_daily.v1',
                       'layers.silver.sku_id_seller_key.demand_finance_daily.v1'])
def module(request):
    return import_module(request.param + '.job.service_schema')


@pytest.mark.parametrize('service', ['dq/results', 'feature_stats/results'])
def test_service_migrations_have_complete_fields(module, service):
    schema = module.service_schema(ROOT / service)
    assert len(schema) > 10
    module.validate_service_schema(schema, schema)
    large_strings = pa.schema([pa.field(f.name, pa.large_string() if pa.types.is_string(f.type) else f.type,
                                       nullable=f.nullable) for f in schema])
    module.validate_service_schema(large_strings, schema)


@pytest.mark.parametrize('ddl', [
    'CREATE TABLE unsupported (x INT)',
    "CREATE TABLE IF NOT EXISTS {target_table} (\n x MAP COMMENT 'bad'\n) USING iceberg",
    "CREATE TABLE IF NOT EXISTS {target_table} (\n x STRING COMMENT 'x',\n x STRING COMMENT 'x'\n) USING iceberg",
])
def test_unknown_service_ddl_fails_closed(module, tmp_path, ddl):
    migrations = tmp_path / 'migrations'
    migrations.mkdir()
    (migrations / 'create_table.sql').write_text(ddl, encoding='utf-8')
    with pytest.raises(ValueError):
        module.service_schema(tmp_path)
