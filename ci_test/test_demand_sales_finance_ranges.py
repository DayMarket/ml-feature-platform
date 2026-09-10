"""Диапазон finance: дневная атомарность, resume и terminal DQ."""

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal

import pyarrow as pa
import pytest

from ci_test.test_demand_daily_preparation import CAPTURE, config, fx, module, raw
from ci_test.test_demand_sales_finance_runtime import Client, wire
from ci_test.test_demand_daily_preparation import schema

DAYS = [date(2025, 1, 1), date(2025, 1, 2)]


@pytest.fixture
def env(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog

    kind, cfg = "finance", config("finance")
    catalog = SqlCatalog(
        "iceberg",
        uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    catalog.create_namespace("silver")
    table = catalog.create_table(
        module(kind, "preparation").target_ref(cfg, catalog.name), schema=schema(kind)
    )
    with table.update_spec() as spec:
        spec.add_identity("date")
    yield kind, cfg, catalog, table
    catalog.engine.dispose()


def plan(kind, cfg=None, mode='manual', run_id='manual'):
    return module(kind, 'ranges').build_request(
        cfg or config(kind), run_id=run_id, mode=mode,
        interval_start='2025-01-02T04:00:00Z', interval_end='2025-01-03T04:00:00Z',
        history_start=DAYS[0])


class DailyClient:
    def __init__(self, kind, cfg):
        self.kind, self.cfg = kind, cfg
        self.streams, self.calls = [], []
        self.missing, self.fail_stream = None, None
        self.rate, self.delta = 10.0, {}
        query = module(kind, 'query')
        self.sql = {}
        for day in DAYS:
            for name in ('coverage_totals_query', 'fx_query', 'source_query',
                         'coverage_query' if kind == 'sales' else 'source_audit_query'):
                if name == 'fx_query':
                    sql = query.fx_query(day)
                elif name == 'source_query':
                    sql = query.source_query(cfg, day, fx_available=True)
                else:
                    sql = getattr(query, name)(cfg, day)
                self.sql[sql] = (name, day)

    def source(self, day):
        name = 'sales_payment_value' if self.kind == 'sales' else 'finance_gmv_generated'
        base = raw(self.kind, {'date': day}).to_pylist()[0]
        base[name] += Decimal(self.delta.get(day, 0))
        for field in list(base):
            if field.endswith('_usd'):
                base[field] = float(base[field[:-4]]) / self.rate
        return raw(self.kind, base)

    def execute(self, sql):
        name, day = self.sql[sql]
        self.calls.append((name, day))
        source = self.source(day)
        if name == 'coverage_totals_query':
            values = Client(self.kind, self.cfg, source).coverage()
            if day == self.missing:
                values[0] = 0
            return [values]
        if name == 'fx_query':
            receipt = fx() | {'date': day, 'fx_rate_date': day, 'fx_rate_uzs_per_usd': self.rate}
            return [list(receipt.values())]
        if name == 'coverage_query':
            count = sum(source['sales_order_items'].to_pylist())
            return [(count, count, source.num_rows, sum(source['sales_units'].to_pylist()),
                     sum(source['sales_gmv'].to_pylist()))]
        if name == 'source_audit_query':
            return [(sum(source['source_rows'].to_pylist()), 0, 0)]
        raise AssertionError(name)

    def execute_iter(self, sql, **kwargs):
        name, day = self.sql[sql]
        assert name == 'source_query'
        self.streams.append(day)
        if day == self.fail_stream:
            raise RuntimeError('interrupted day')
        rows, columns = wire(self.source(day), self.kind)
        size = kwargs['chunk_size']
        payload = [columns, *rows]
        for start in range(0, len(payload), size):
            yield payload[start:start + size]


def load(target, client, request=None, **kwargs):
    kind, cfg, catalog, _ = target
    return module(kind, 'ranges').load_range(
        cfg, catalog, client, request or plan(kind, cfg), preflight=lambda *args: True, **kwargs)


@pytest.mark.parametrize('kind', ['finance'])
@pytest.mark.parametrize('value', ['2025-01-03T04:00:00', '2025-01-03T04:00:00Z',
    '2025-01-03 04:00:00+00:00', '2025-01-03 04:00:00', '2025-01-03T09:00:00+05:00'])
def test_interval_utc(kind, value):
    assert module(kind, 'ranges').interval_utc(value).isoformat() == '2025-01-03T04:00:00+00:00'


@pytest.mark.parametrize('kind', ['finance'])
def test_regular_31_days_without_old_dq_holes(kind):
    ranges, cfg = module(kind, 'ranges'), config(kind)
    start = date(2025, 1, 1)
    request = ranges.build_request(cfg, run_id='regular', mode='regular',
        interval_start='2025-02-28T04:00:00Z', interval_end='2025-03-01T04:00:00Z',
        history_start=start)
    assert len(request['dates']) == 31 and request['dates'][0] == '2025-01-29'
    assert request['dates'][-1] == '2025-02-28'
    ranges.validate_request(cfg, request)
    changed = deepcopy(request)
    changed['dates'].pop()
    with pytest.raises(ValueError):
        ranges.validate_request(cfg, changed)
    cfg['runtime']['refresh_days'] = 60
    with pytest.raises(ValueError):
        ranges.validate_request(cfg, request)


def test_history_and_resume_without_airflow_logs(env):
    kind, cfg, _, table = env
    client = DailyClient(kind, cfg)
    result = load(env, client)
    assert result['dates'] == [d.isoformat() for d in DAYS]
    assert result['status'] == 'written' and 'dq_status' not in result
    assert all(len(r['source_signature']) == 64 for r in result['day_receipts'])
    repeated = load(env, client)
    assert repeated['snapshot_id'] == result['snapshot_id']
    assert all(r['resumed'] for r in repeated['day_receipts'])
    assert client.streams == DAYS
    assert table.refresh().scan().count() == 2


def test_missing_later_day_preserves_first_commit(env):
    kind, cfg, _, table = env
    client = DailyClient(kind, cfg)
    client.missing = DAYS[1]
    with pytest.raises(ValueError, match='Пустой'):
        load(env, client)
    assert client.streams == [DAYS[0]]
    assert table.refresh().scan().count() == 1


def test_failure_after_first_day_resumes_second(env):
    kind, cfg, _, table = env
    client = DailyClient(kind, cfg)
    client.fail_stream = DAYS[1]
    with pytest.raises(RuntimeError, match='interrupted'):
        load(env, client)
    assert table.refresh().scan().count() == 1
    client.fail_stream = None
    result = load(env, client)
    assert [r['resumed'] for r in result['day_receipts']] == [True, False]
    assert client.streams == [DAYS[0], DAYS[1], DAYS[1]]


@pytest.mark.parametrize('changed', ['totals', 'fx'])
def test_same_run_resume_keeps_completed_days(env, changed):
    kind, cfg, _, table = env
    client = DailyClient(kind, cfg)
    before = load(env, client)
    if changed == 'totals':
        client.delta[DAYS[0]] = 10
    else:
        client.rate = 20.0
    result = load(env, client)
    assert result['snapshot_id'] == before['snapshot_id']
    assert all(receipt['resumed'] for receipt in result['day_receipts'])
    assert table.refresh().scan().count() == 2


def test_bad_target_rejected_before_source(env):
    kind, cfg, _, table = env
    with table.update_spec() as spec:
        spec.remove_field('date')
    client = DailyClient(kind, cfg)
    with pytest.raises(ValueError, match='identity partition'):
        load(env, client)
    assert not client.calls and not client.streams


def test_dq_requires_every_date_of_same_snapshot(env):
    kind, cfg, _, _ = env
    ranges, request = module(kind, 'ranges'), plan(kind, cfg)
    written = load(env, DailyClient(kind, cfg), request)
    checks = [{'date': r['date'], 'source_manifest_id': r['source_manifest_id'],
               'table_uuid': written['table_uuid'], 'snapshot_id': written['snapshot_id'],
               'request_id': request['request_id'], 'rows_checked': r['rows_written'],
               'dq_status': 'passed'} for r in written['day_receipts']]
    assert ranges.require_range_dq(cfg, request, written, checks)['status'] == 'ready'
    with pytest.raises(ValueError, match='каждого дня'):
        ranges.require_range_dq(cfg, request, written, checks[-1:])
    checks[0]['snapshot_id'] += 1
    with pytest.raises(ValueError, match='точную запись'):
        ranges.require_range_dq(cfg, request, written, checks)


def test_source_signature_ignores_only_fx_capture_time(env):
    kind, cfg, _, _ = env
    runtime, client = module(kind, 'runtime'), DailyClient(kind, cfg)
    coverage = runtime.read_coverage(cfg, client, DAYS[0])
    receipt = fx() | {'date': DAYS[0], 'fx_rate_date': DAYS[0]}
    signature = runtime.source_signature(coverage, receipt)
    assert signature == runtime.source_signature(coverage, receipt | {'fx_captured_at': CAPTURE + timedelta(days=1)})
    assert signature != runtime.source_signature(coverage, receipt | {'fx_rate_source': 'latest_available'})
    assert signature != runtime.source_signature(coverage, receipt | {'fx_rate_uzs_per_usd': 11.0})


def test_lost_ack_recovers_signature_from_committed_metadata(env, monkeypatch):
    kind, cfg, _, table = env
    writer, client = module(kind, 'writer'), DailyClient(kind, cfg)
    verify = writer.verify_proof
    def failed(*args):
        raise RuntimeError('lost acknowledgement')
    monkeypatch.setattr(writer, 'verify_proof', failed)
    with pytest.raises(RuntimeError, match='acknowledgement'):
        load(env, client)
    assert table.refresh().scan().count() == 1
    monkeypatch.setattr(writer, 'verify_proof', verify)
    result = load(env, client)
    assert [r['resumed'] for r in result['day_receipts']] == [True, False]
    assert client.streams == DAYS


def test_bad_service_preflight_never_queries_source(env):
    kind, cfg, catalog, _ = env
    client = DailyClient(kind, cfg)
    with pytest.raises(ValueError, match='preflight'):
        module(kind, 'ranges').load_range(cfg, catalog, client, plan(kind, cfg), preflight=lambda *args: False)
    assert not client.calls and not client.streams


def test_finance_seller_grain_in_range_resume(env):
    kind, cfg, _, table = env
    if kind != 'finance':
        pytest.skip('Проверка составного ключа финансов')
    class SellersClient(DailyClient):
        def source(self, day):
            source = super().source(day)
            unknown = raw('finance', source.to_pylist()[0] | {'seller_id': None, 'seller_key': 'unknown'})
            return pa.concat_tables([source, unknown])
    client = SellersClient(kind, cfg)
    result = load(env, client)
    assert [r['rows_written'] for r in result['day_receipts']] == [2, 2]
    assert all(r['resumed'] for r in load(env, client)['day_receipts'])
    assert table.refresh().scan().count() == 4
