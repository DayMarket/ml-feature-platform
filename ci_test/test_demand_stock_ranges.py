"""Планирование диапазона, восстановление commit и запрет частичного DQ receipt."""

from copy import deepcopy
from datetime import date, timedelta

import pytest

from ci_test.test_demand_stock_daily import CAPTURE, DAY, SourceClient, batch, config, env as env, write
from layers.silver.sku_id.demand_stock_daily.v1.job import checkpoint, ranges, writer


@pytest.mark.parametrize('value', [
    '2026-09-09T04:00:00', '2026-09-09T04:00:00+00:00', '2026-09-09T04:00:00Z',
    '2026-09-09 04:00:00+00:00', '2026-09-09 04:00:00', '2026-09-09T09:00:00+05:00',
])
def test_interval_formats(value):
    assert ranges.interval_utc(value).isoformat() == '2026-09-09T04:00:00+00:00'


@pytest.mark.parametrize('value', ['2026-09-09', 'bad', '', None, DAY])
def test_invalid_interval(value):
    with pytest.raises(ValueError, match='граница Airflow'):
        ranges.interval_utc(value)


def request(cfg=None, mode='manual', run_id='manual__range'):
    return ranges.build_request(cfg or config(), run_id=run_id, mode=mode,
                                interval_start='2026-09-02T04:00:00Z', interval_end='2026-09-03T04:00:00Z',
                                history_start=DAY)


def test_regular_refresh_is_exactly_31_days():
    cfg = config()
    first = date(2026, 7, 1)
    plan = ranges.build_request(cfg, run_id='run', mode='regular', interval_start='2026-09-08T04:00:00Z',
                                interval_end='2026-09-09T04:00:00Z', history_start=first)
    assert len(plan['dates']) == 31
    assert plan['dates'][0] == '2026-08-09'
    assert plan['dates'][-1] == '2026-09-08'
    ranges.validate_request(cfg, plan)


def test_manual_keeps_entire_range_and_request_is_frozen():
    cfg = config()
    plan = request(cfg)
    assert plan['dates'] == ['2026-09-01', '2026-09-02']
    assert request(cfg, run_id='another')['request_id'] != plan['request_id']
    changed = deepcopy(plan)
    changed['dates'].pop()
    with pytest.raises(ValueError, match='изменён'):
        ranges.validate_request(cfg, changed)
    cfg['source']['contract_version'] = 'new'
    with pytest.raises(ValueError, match='Конфигурация'):
        ranges.validate_request(cfg, plan)


def resume(env, day=DAY, manifest='run', version='source_eod_positive_availability_v1'):
    cfg, cat, _ = env
    return checkpoint.resume_day(cfg, cat, day=day, manifest=manifest, version=version)


def test_resume_reads_ancestor_proof_but_verifies_current_day(env):
    original = write(env)
    later = write(env, day=DAY + timedelta(days=1))
    receipt = resume(env)
    assert receipt['resumed'] is True and receipt['status'] == 'written'
    assert receipt['snapshot_id'] == later['snapshot_id'] != original['snapshot_id']
    assert 'dq_status' not in receipt
    assert resume(env, manifest='other') is None
    assert resume(env, version='other') is None


def test_commit_survives_failure_before_acknowledgement(env, monkeypatch):
    def fail(*args):
        raise RuntimeError('worker killed after commit')
    monkeypatch.setattr(writer, 'verify_proof', fail)
    with pytest.raises(RuntimeError, match='worker killed'):
        write(env)
    assert resume(env)['resumed'] is True


def test_existing_rows_without_proof_are_not_a_checkpoint(env):
    _, _, table = env
    table.append(batch(table.schema().as_arrow()))
    assert resume(env) is None


def test_changed_partition_with_same_count_is_not_skipped(env):
    from pyiceberg.expressions import EqualTo
    _, _, table = env
    write(env)
    table.refresh()
    table.overwrite(batch(table.schema().as_arrow(), ids=(3, 4)), overwrite_filter=EqualTo('date', DAY))
    assert resume(env) is None


def test_failed_preflight_never_reads_source(env):
    cfg, cat, _ = env
    def unexpected(day):
        pytest.fail('source must not be touched')
    with pytest.raises(ValueError, match='preflight'):
        ranges.load_range(cfg, cat, None, request(cfg), preflight=lambda *args: False,
                          require_source_ready=unexpected)


def test_missing_source_day_preserves_previous_day(env):
    cfg, cat, table = env
    class MissingDayClient(SourceClient):
        def execute(self, sql):
            assert 'uniqExact' in sql
            from datetime import datetime
            stamp = datetime(2026, 9, 2)
            return [(2, 2, 0, 0, 1, stamp)] if '2026-09-01' in sql else [(0, 0, 0, 0, 0, stamp)]
    with pytest.raises(ValueError, match='2026-09-02'):
        ranges.load_range(cfg, cat, MissingDayClient(), request(cfg), preflight=lambda *args: True)
    table.refresh()
    assert table.scan().count() == 2


def test_manual_range_uses_source_data_without_airflow_statuses(env):
    import re

    cfg, cat, table = env
    class DailyClient(SourceClient):
        def select_day(self, sql):
            self.day = date.fromisoformat(re.search(r"toDate\('([0-9-]+)'\)", sql)[1])
        def execute(self, sql):
            self.select_day(sql)
            return super().execute(sql)
        def execute_iter(self, sql, **kwargs):
            self.select_day(sql)
            yield from super().execute_iter(sql, **kwargs)
    plan = ranges.build_request(cfg, run_id='manual', mode='manual',
                                interval_start='2025-01-02T04:00:00Z', interval_end='2025-01-03T04:00:00Z',
                                history_start=date(2025, 1, 1))
    result = ranges.load_range(cfg, cat, DailyClient(), plan, preflight=lambda *args: True)
    assert result['dates'] == ['2025-01-01', '2025-01-02']
    assert result['status'] == 'written' and 'dq_status' not in result
    table.refresh()
    assert table.scan().count() == 4


def test_range_resumes_committed_day_after_process_restart(env, monkeypatch):
    cfg, cat, table = env
    plan = request(cfg)
    calls = []
    failure = True
    def load(config, catalog, client, *, day, manifest, require_source_ready):
        calls.append(day)
        if day != DAY and failure:
            raise RuntimeError('source failure on second day')
        return writer.write_day(config, catalog, [batch(table.schema().as_arrow(), day=day, manifest=manifest)],
                                 day=day, expected_rows=2, manifest=manifest,
                                 version=config['source']['contract_version'], ingested_at=CAPTURE,
                                 verify_source=lambda: require_source_ready(day))
    monkeypatch.setattr(ranges, 'load_day', load)
    kwargs = {'preflight': lambda *args: True, 'require_source_ready': lambda day: True}
    with pytest.raises(RuntimeError, match='second day'):
        ranges.load_range(cfg, cat, None, plan, **kwargs)
    table.refresh()
    assert table.scan().count() == 2
    failure = False
    result = ranges.load_range(cfg, cat, None, plan, **kwargs)
    assert calls == [DAY, DAY + timedelta(days=1), DAY + timedelta(days=1)]
    assert [r['resumed'] for r in result['day_receipts']] == [True, False]
    table.refresh()
    assert table.scan().count() == 4
    snapshot_id = table.current_snapshot().snapshot_id
    again = ranges.load_range(cfg, cat, None, plan, **kwargs)
    assert all(r['resumed'] for r in again['day_receipts'])
    table.refresh()
    assert table.current_snapshot().snapshot_id == snapshot_id
    assert set(table.refs()) == {'main'}


def dq_inputs():
    cfg, plan = config(), request()
    receipts = [{'status': 'written', 'date': d, 'source_manifest_id': ranges.day_manifest(plan, date.fromisoformat(d)),
                 'table_uuid': 'table-uuid', 'source_contract_version': cfg['source']['contract_version'],
                 'rows_written': 2} for d in plan['dates']]
    written = {'status': 'written', 'request_id': plan['request_id'], 'dates': plan['dates'],
               'snapshot_id': 123, 'table_uuid': 'table-uuid', 'day_receipts': receipts}
    checks = [{'date': r['date'], 'source_manifest_id': r['source_manifest_id'], 'table_uuid': 'table-uuid',
               'dq_status': 'passed', 'snapshot_id': 123, 'request_id': plan['request_id'], 'rows_checked': 2}
              for r in receipts]
    return cfg, plan, written, checks


def test_range_dq_requires_all_exact_day_receipts():
    cfg, plan, written, checks = dq_inputs()
    assert ranges.require_range_dq(cfg, plan, written, checks)['status'] == 'ready'
    assert written['status'] == 'written'
    with pytest.raises(ValueError, match='каждого дня'):
        ranges.require_range_dq(cfg, plan, written, checks[-1:])


@pytest.mark.parametrize('field,value', [('date', '2026-09-02'), ('dq_status', 'failed'),
    ('dq_status', 'missing'), ('snapshot_id', 124), ('table_uuid', 'other'),
    ('source_manifest_id', 'other'), ('request_id', 'other'), ('rows_checked', 1)])
def test_wrong_day_dq_blocks_entire_range(field, value):
    cfg, plan, written, checks = dq_inputs()
    checks[0][field] = value
    with pytest.raises(ValueError, match='точную запись'):
        ranges.require_range_dq(cfg, plan, written, checks)
