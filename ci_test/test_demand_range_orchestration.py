"""Preflight и connection bridge дневных writers без внешних сервисов."""

from datetime import date
from importlib import import_module
from pathlib import Path
import sys
from types import ModuleType

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_daily_preparation import PATHS, config, raw, schema
from ci_test.test_demand_sales_finance_ranges import DailyClient
from ci_test.test_demand_sales_finance_runtime import wire
from ci_test.test_demand_stock_daily import SourceClient, config as stock_config, raw as stock_raw, schema as stock_schema

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2025, 1, 1)


def module(kind, name):
    path = 'layers/silver/sku_id/demand_stock_daily/v1' if kind == 'stock' else PATHS[kind]
    return import_module(path.replace('/', '.') + '.job.' + name)


class MetadataClient:
    def __init__(self, kind, cfg):
        self.inner = SourceClient(day=DAY) if kind == 'stock' else DailyClient(kind, cfg)
        self.kind, self.cfg = kind, cfg
        self.metadata_calls = 0
        self.closed, self.bad_metadata = False, False

    def execute(self, sql, **kwargs):
        if kwargs.get('with_column_types'):
            assert sql == module(self.kind, 'orchestration').source_schema_query(self.cfg, DAY)
            self.metadata_calls += 1
            source = stock_raw(day=DAY) if self.kind == 'stock' else raw(self.kind, {'date': DAY})
            _, columns = wire(source, self.kind)
            return ([], columns[:-1] if self.bad_metadata else columns)
        return self.inner.execute(sql)

    def execute_iter(self, *args, **kwargs):
        yield from self.inner.execute_iter(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True


@pytest.fixture(params=['stock', 'finance'])
def setup(request, tmp_path):
    SqlCatalog = pytest.importorskip("pyiceberg.catalog.sql").SqlCatalog

    kind = request.param
    cfg = stock_config() if kind == 'stock' else config(kind)
    catalog = SqlCatalog('iceberg', uri=f'sqlite:///{tmp_path}/catalog.db',
                         warehouse=(tmp_path / 'warehouse').as_uri())
    catalog.create_namespace('silver')
    target = catalog.create_table((cfg['table']['schema'], cfg['table']['name']),
                                  schema=stock_schema() if kind == 'stock' else schema(kind))
    with target.update_spec() as spec:
        spec.add_identity('date')
    for file in ('dq/results/config.yaml', 'feature_stats/results/config.yaml'):
        info = yaml.safe_load((ROOT / file).read_text())['table']
        service = module(kind, 'service_schema').service_schema((ROOT / file).parent)
        catalog.create_table((info['schema'], info['name']), schema=service)
    plan = module(kind, 'ranges').build_request(cfg, run_id='manual', mode='manual',
        interval_start='2025-01-01T04:00:00Z', interval_end='2025-01-02T04:00:00Z',
        history_start=DAY)
    yield kind, cfg, catalog, target, plan
    catalog.engine.dispose()


def test_execute_range_preflights_trino_and_source_schema(setup):
    kind, cfg, catalog, target, request = setup
    client, sql = MetadataClient(kind, cfg), []
    def query(value):
        sql.append(value)
        return []
    result = module(kind, 'orchestration').execute_range(
        cfg, ROOT, request, catalog=catalog, client=client, queries={'trino_search': query})
    assert result['status'] == 'written' and 'dq_status' not in result
    assert client.metadata_calls == 1 and not client.closed
    assert len(sql) == 3 and all('"dwh-iceberg"."silver".' in q and q.endswith('LIMIT 0') for q in sql)
    assert target.refresh().scan().count() > 0


@pytest.mark.parametrize('failure', ['missing_service', 'trino', 'source_schema', 'bad_target'])
def test_failed_preflight_does_not_write(setup, failure):
    kind, cfg, catalog, target, request = setup
    client = MetadataClient(kind, cfg)
    if failure == 'missing_service':
        info = yaml.safe_load((ROOT / 'dq/results/config.yaml').read_text())['table']
        catalog.drop_table((info['schema'], info['name']))
    elif failure == 'source_schema':
        client.bad_metadata = True
    elif failure == 'bad_target':
        with target.update_spec() as spec:
            spec.remove_field('date')
    def query(sql):
        if failure == 'trino':
            raise RuntimeError('Trino unavailable')
        return []
    with pytest.raises((ValueError, RuntimeError)):
        module(kind, 'orchestration').execute_range(
            cfg, ROOT, request, catalog=catalog, client=client, queries={'trino_search': query})
    assert target.refresh().current_snapshot() is None
    if failure != 'source_schema':
        assert client.metadata_calls == 0


@pytest.mark.parametrize('setup', ['stock', 'finance'], indirect=True)
@pytest.mark.parametrize('service', ['dq/results/config.yaml', 'feature_stats/results/config.yaml'])
@pytest.mark.parametrize('drift', ['missing', 'extra', 'type', 'nullable'])
def test_service_schema_drift_blocks_before_source(setup, service, drift):
    kind, cfg, catalog, target, request = setup
    info = yaml.safe_load((ROOT / service).read_text())['table']
    identifier = (info['schema'], info['name'])
    original = module(kind, 'service_schema').service_schema((ROOT / service).parent)
    fields = list(original)
    if drift == 'missing':
        fields.pop()
    elif drift == 'extra':
        fields.append(pa.field('unexpected', pa.string()))
    elif drift == 'type':
        fields[0] = pa.field(fields[0].name, pa.bool_(), nullable=fields[0].nullable)
    else:
        fields[0] = pa.field(fields[0].name, fields[0].type, nullable=not fields[0].nullable)
    catalog.drop_table(identifier)
    catalog.create_table(identifier, schema=pa.schema(fields))
    client = MetadataClient(kind, cfg)
    with pytest.raises(ValueError, match='schema|nullable'):
        module(kind, 'orchestration').execute_range(
            cfg, ROOT, request, catalog=catalog, client=client, queries={'trino_search': lambda sql: []})
    assert client.metadata_calls == 0 and target.refresh().current_snapshot() is None


def test_airflow_connections_and_client_cleanup(setup, monkeypatch):
    kind, cfg, catalog, _, request = setup
    bridge, client = module(kind, 'orchestration'), MetadataClient(kind, cfg)
    seen = []
    ch_module = ModuleType('airflow_commons.hooks.clickhouse_hook')
    class Hook:
        def __init__(self, **kwargs):
            seen.append(kwargs)
        def get_conn(self):
            return client
    ch_module.ClickHouseHook = Hook
    monkeypatch.setitem(sys.modules, ch_module.__name__, ch_module)
    trino_module = ModuleType('airflow.providers.trino.hooks.trino')
    class Trino:
        def __init__(self, **kwargs):
            seen.append(kwargs)
        def get_records(self, sql):
            return []
    trino_module.TrinoHook = Trino
    monkeypatch.setitem(sys.modules, trino_module.__name__, trino_module)
    monkeypatch.setattr(bridge, 'load_results_catalog', lambda name: catalog)
    result = bridge.execute_range(cfg, ROOT, request)
    assert result['status'] == 'written' and client.closed
    assert seen == [{'clickhouse_conn_id': 'clickhouse_dwh_team_logistics', 'use_numpy': False},
                    {'trino_conn_id': 'trino_search'}]
    client.closed, client.bad_metadata = False, True
    with pytest.raises(ValueError):
        bridge.execute_range(cfg, ROOT, request)
    assert client.closed


def test_different_stats_connection_is_also_checked(setup):
    kind, cfg, catalog, _, _ = setup
    cfg['feature_stats']['trino_conn_id'] = 'trino_recsys'
    bridge, client = module(kind, 'orchestration'), MetadataClient(kind, cfg)
    calls = []
    bridge.preflight(cfg, ROOT, catalog, client,
        {'trino_search': lambda sql: calls.append('dq') or [],
         'trino_recsys': lambda sql: calls.append('stats') or []}, DAY)
    assert calls.count('dq') == calls.count('stats') == 3
