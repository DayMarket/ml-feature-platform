"""Полный дневной observed runtime: DB-API fixtures → настоящий локальный Iceberg."""

from copy import deepcopy
from datetime import timedelta
from importlib import import_module

import pytest

from ci_test.test_demand_observed_inputs import payload
from ci_test.test_demand_observed_preparation import DAY, NOW, ROOT, PREP, schema, source
from ci_test.test_demand_observed_reader import Cursor, description
from ci_test.test_demand_observed_writer import env as env

RUNTIME = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.runtime")


class SourceConnection:
    def __init__(self, tables):
        self.tables, self.cursors = tables, []
        self.failure = None
    def cursor(self):
        parent = self
        class RoutedCursor(Cursor):
            def __init__(self):
                super().__init__([], [])
            def execute(self, sql):
                super().execute(sql)
                if 'AS rows_expected' in sql:
                    self.rows = [(4,)]
                    return
                kind = 'sales' if 'feature_platform_demand_sales_daily' in sql else 'stock'
                day = next((day for day in parent.tables if not isinstance(day, str)
                            and f"DATE '{day.isoformat()}'" in sql), None)
                batch = parent.tables[day][kind] if day is not None else parent.tables[kind]
                columns = PREP.SOURCE_FIELDS[kind]
                self.description = description(batch.schema, columns)
                self.rows = [[r[name] for name in columns] for r in batch.to_pylist()]
                if kind == 'stock' and parent.failure == 'stream':
                    self.fail_at = 2
                if kind == 'sales' and parent.failure == 'partial':
                    self.rows.pop()
        cursor = RoutedCursor()
        self.cursors.append(cursor)
        return cursor


@pytest.fixture
def setup(env):
    config, catalog, gold = env
    config['runtime']['max_batch_rows'] = 2
    _, sources, references, checked = payload()
    catalog.create_namespace('silver')
    batches = {}
    for kind, cfg in sources.items():
        table = catalog.create_table((cfg['table']['schema'], cfg['table']['name']),
                                     schema=schema((ROOT / config['inputs'][f'{kind}_config']).parent))
        with table.update_spec() as spec:
            spec.add_identity('date')
        ids = (1, 3) if kind == 'sales' else (2, 3, 4)
        batches[kind] = source(kind, ids, source_manifest_id='capture',
                               source_contract_version=cfg['source']['contract_version'])
        table.append(batches[kind])
        version = {'snapshot_id': table.current_snapshot().snapshot_id,
                   'table_uuid': str(table.metadata.table_uuid)}
        outcome = checked[kind]
        outcome['receipt'].update(version)
        outcome['receipt']['day_receipts'][0].update(table_uuid=version['table_uuid'], rows_written=len(ids))
        outcome['day_checks'][0].update(version, rows_checked=len(ids))
    connection = SourceConnection(batches)
    return env, sources, references, checked, connection


def execute(setup, fetch_checked=None):
    (config, catalog, _), _, refs, checked, connection = setup
    return RUNTIME.load_day(config, ROOT, catalog, connection, day=DAY, references=refs,
        fetch_checked=fetch_checked or (lambda _: deepcopy(checked)), manifest='gold-run', ingested_at=NOW)


def test_load_and_resume_without_source_rescan(setup):
    result = execute(setup)
    gold = setup[0][2].refresh()
    assert result['rows_written'] == 4 and result['status'] == 'written'
    rows = gold.scan().to_arrow().sort_by([('sku_id', 'ascending')]).to_pylist()
    assert [r['sku_id'] for r in rows] == [1, 2, 3, 4]
    assert [(r['sales_component_present'], r['is_in_stock_eod']) for r in rows] == [
        (True, False), (False, True), (True, True), (False, True)]
    assert len(setup[4].cursors) == 3 and all(c.closed for c in setup[4].cursors)
    calls = []
    def checked(refs):
        assert refs == setup[2]
        calls.append(True)
        return deepcopy(setup[3])
    resumed = execute(setup, checked)
    assert resumed['resumed'] and resumed['snapshot_id'] == result['snapshot_id']
    assert len(calls) == 2 and len(setup[4].cursors) == 3


@pytest.mark.parametrize('failure', ['stream', 'partial', 'dq_changed', 'snapshot_missing'])
def test_failed_read_or_changed_proof_never_commits(setup, failure):
    setup[4].failure = failure
    calls = []
    def checked(_):
        value = deepcopy(setup[3])
        calls.append(True)
        if len(calls) > 1:
            if failure == 'dq_changed':
                value['sales']['dq_status'] = 'failed'
            elif failure == 'snapshot_missing':
                catalog = setup[0][1]
                cfg = setup[1]['sales']['table']
                catalog.drop_table((cfg['schema'], cfg['name']))
        return value
    with pytest.raises((ValueError, RuntimeError)):
        execute(setup, checked)
    assert setup[0][2].refresh().current_snapshot() is None
    assert all(c.closed for c in setup[4].cursors)


def test_unavailable_input_preflight_before_query(setup):
    setup[3]['sales']['receipt']['snapshot_id'] = 1
    setup[3]['sales']['day_checks'][0]['snapshot_id'] = 1
    with pytest.raises(ValueError, match='snapshot/UUID'):
        execute(setup)
    assert setup[4].cursors == []


def test_changed_dq_blocks_even_existing_gold_resume(setup):
    old = execute(setup)
    calls = []
    def checked(_):
        value = deepcopy(setup[3])
        calls.append(True)
        if len(calls) > 1:
            value['stock']['day_checks'][0]['dq_status'] = 'failed'
        return value
    with pytest.raises(ValueError):
        execute(setup, checked)
    assert setup[0][2].refresh().current_snapshot().snapshot_id == old['snapshot_id']
    assert len(setup[4].cursors) == 3


@pytest.mark.parametrize('option,value', [('day', NOW), ('day', NOW.date()),
    ('manifest', ''), ('manifest', ' '), ('ingested_at', NOW.replace(tzinfo=None)),
    ('ingested_at', ''), ('ingested_at', False)])
def test_invalid_request_fails_before_checked_or_io(setup, option, value):
    config, catalog, _ = setup[0]
    args = dict(day=DAY, references=setup[2], manifest='gold-run', ingested_at=NOW)
    args[option] = value
    def unexpected(_):
        pytest.fail('DQ payload не читается для неверного запроса')
    with pytest.raises(ValueError):
        RUNTIME.load_day(config, ROOT, catalog, setup[4], fetch_checked=unexpected, **args)
    assert setup[4].cursors == []


def extend_range(setup):
    next_day = DAY + timedelta(days=2)
    config, catalog, _ = setup[0]
    for kind, cfg in setup[1].items():
        receipt = deepcopy(setup[3][kind]['receipt']['day_receipts'][0])
        receipt['date'] = next_day.isoformat()
        check = deepcopy(setup[3][kind]['day_checks'][0])
        check['date'] = next_day.isoformat()
        batch = source(kind, (1, 3) if kind == 'sales' else (2, 3, 4), date=next_day,
            source_manifest_id='capture', source_contract_version=cfg['source']['contract_version'])
        table = catalog.load_table((cfg['table']['schema'], cfg['table']['name']))
        table.append(batch)
        snapshot = table.current_snapshot().snapshot_id
        setup[3][kind]['receipt']['snapshot_id'] = snapshot
        setup[3][kind]['day_checks'][0]['snapshot_id'] = snapshot
        check['snapshot_id'] = snapshot
        setup[3][kind]['receipt']['dates'].append(next_day.isoformat())
        setup[3][kind]['receipt']['day_receipts'].append(receipt)
        setup[3][kind]['day_checks'].append(check)
        setup[4].tables.setdefault(next_day, {})[kind] = batch
    return [DAY, next_day]


def execute_range(setup, days, checked=None):
    return RUNTIME.load_range(setup[0][0], ROOT, setup[0][1], setup[4], days=days,
        references=setup[2], fetch_checked=checked or (lambda _: deepcopy(setup[3])),
        request_id='range-request', manifest='gold-run', ingested_at=NOW)


def test_range_writes_only_selected_days_and_returns_strict_dq_receipt(setup):
    from dq.day_range import validate_written
    days = extend_range(setup)
    result = execute_range(setup, days)
    assert validate_written(result) == days
    assert 'dq_status' not in result
    rows = setup[0][2].refresh().scan().to_arrow()
    assert set(rows['date'].to_pylist()) == set(days) and rows.num_rows == 8
    assert len(setup[4].cursors) == 6
    resumed = execute_range(setup, days)
    assert all(r['resumed'] for r in resumed['day_receipts']) and len(setup[4].cursors) == 6


def test_range_preflights_all_days_before_first_write(setup):
    days = extend_range(setup)
    setup[3]['stock']['day_checks'][1]['dq_status'] = 'failed'
    with pytest.raises(ValueError):
        execute_range(setup, days)
    assert setup[0][2].refresh().current_snapshot() is None and setup[4].cursors == []


def test_range_retry_keeps_completed_day_and_resumes_it(setup):
    days = extend_range(setup)
    calls = []
    def checked(_):
        calls.append(True)
        # Общий preflight, первый день до чтения и перед commit уже прошли.
        if len(calls) == 4:
            raise RuntimeError('upstream lookup interrupted')
        return deepcopy(setup[3])
    with pytest.raises(RuntimeError, match='interrupted'):
        execute_range(setup, days, checked)
    assert set(setup[0][2].refresh().scan().to_arrow()['date'].to_pylist()) == {DAY}
    before = len(setup[4].cursors)
    result = execute_range(setup, days)
    assert result['day_receipts'][0]['resumed'] and not result['day_receipts'][1]['resumed']
    assert len(setup[4].cursors) == before + 3
