"""Спланировать дневной диапазон и продолжить его запись без повторной выгрузки готовых дней."""

from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json

from .checkpoint import resume_day
from .preparation import target_ref, validate_schema
from .runtime import load_day
from .query import source_ref


def interval_utc(value):
    """Airflow ISO с зоной или без неё; без зоны означает UTC."""
    try:
        if isinstance(value, datetime):
            result = value
        elif isinstance(value, str) and len(value.strip()) > 10:
            result = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        else:
            raise ValueError('Нет времени')
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError) as error:
        raise ValueError(f'Неподдерживаемая граница Airflow: {value!r}') from error


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()


def build_request(config, *, run_id, mode, interval_start, interval_end, history_start):
    """Regular обновляет фиксированное окно, manual — весь явный диапазон."""
    start, end = interval_utc(interval_start), interval_utc(interval_end)
    if start >= end or type(history_start) is not date or history_start >= end.date():
        raise ValueError('Пустой или неверный диапазон')
    if not isinstance(run_id, str) or not run_id.strip() or mode not in ('regular', 'manual'):
        raise ValueError('Нужны run_id и режим regular/manual')
    refresh = config['runtime']['refresh_days']
    if type(refresh) is not int or refresh <= 0:
        raise ValueError('Неверный refresh_days')
    all_days = [history_start + timedelta(days=n) for n in range((end.date() - history_start).days)]
    recent = end.date() - timedelta(days=refresh)
    days = [day.isoformat() for day in all_days if mode == 'manual' or day >= recent]
    request = {'run_id': run_id, 'mode': mode, 'interval_start': start.isoformat(),
               'interval_end': end.isoformat(), 'history_start': history_start.isoformat(),
               'end_exclusive': end.date().isoformat(), 'dates': days,
               'config_digest': digest(config)}
    return request | {'request_id': digest(request)}


def validate_request(config, request):
    required = {'run_id', 'mode', 'interval_start', 'interval_end', 'history_start',
                'end_exclusive', 'dates', 'config_digest', 'request_id'}
    if not isinstance(request, dict) or set(request) != required:
        raise ValueError('Неверная схема range request')
    if request['request_id'] != digest({k: v for k, v in request.items() if k != 'request_id'}):
        raise ValueError('Range request изменён после подготовки')
    if request['config_digest'] != digest(config):
        raise ValueError('Конфигурация изменилась: нужен новый запрос')
    if (not isinstance(request['run_id'], str) or not request['run_id'].strip()
            or request['mode'] not in ('regular', 'manual')):
        raise ValueError('Неверные run_id/mode')
    start, end = interval_utc(request['interval_start']), interval_utc(request['interval_end'])
    first, stop = date.fromisoformat(request['history_start']), date.fromisoformat(request['end_exclusive'])
    if start >= end or first >= stop or stop != end.date():
        raise ValueError('Неверные границы запроса')
    days = [date.fromisoformat(value) for value in request['dates']]
    if not days or days != sorted(set(days)) or any(not first <= day < stop for day in days):
        raise ValueError('Даты должны быть непустыми, уникальными и внутри диапазона')
    required_start = first if request['mode'] == 'manual' else max(
        first, stop - timedelta(days=config['runtime']['refresh_days']))
    required_days = {required_start + timedelta(days=n) for n in range((stop - required_start).days)}
    if not required_days.issubset(days):
        raise ValueError('В плане пропущены обязательные дни')
    return days


def day_manifest(request, day):
    return f"seller-sales:{request['request_id']}:{day.isoformat()}"



def preflight_target(config, catalog):
    from pyiceberg.transforms import IdentityTransform

    identifier = target_ref(config, catalog.name)
    source_ref(config)
    version = config['source']['contract_version']
    if not isinstance(version, str) or not version.strip():
        raise ValueError('Нужна версия source контракта')
    if any(type(config['runtime'][key]) is not int or config['runtime'][key] <= 0
           for key in ('max_batch_rows', 'max_batch_bytes')):
        raise ValueError('Неверные лимиты порций')
    if not catalog.table_exists(identifier):
        raise ValueError(f'Нет таблицы {identifier}: сначала миграции')
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field('date').field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError('Нужен identity partition по date')
    return table


def require_head(config, catalog, table_uuid, snapshot_id):
    table = catalog.load_table(target_ref(config, catalog.name))
    head = table.current_snapshot()
    if str(table.metadata.table_uuid) != table_uuid or (head.snapshot_id if head else None) != snapshot_id:
        raise RuntimeError('Snapshot/table UUID сменился во время диапазона')
    return table


def load_range(config, catalog, client, request, *, preflight, require_source_ready=None):
    """Последовательно записать дни; дневной writer сам проверяет источник."""
    days = validate_request(config, request)
    if not callable(preflight) or (require_source_ready is not None and not callable(require_source_ready)):
        raise ValueError('Нужны preflight всех таблиц и корректный coordination hook')
    table = preflight_target(config, catalog)
    table_uuid = str(table.metadata.table_uuid)
    head = table.current_snapshot()
    snapshot_id = head.snapshot_id if head else None
    if preflight(config, catalog, client) is not True:
        raise ValueError('Range preflight не пройден')
    receipts = []
    for day in days:
        require_head(config, catalog, table_uuid, snapshot_id)
        if require_source_ready is not None and require_source_ready(day) is not True:
            raise ValueError(f'Upstream перестал быть готов за {day}')
        manifest = day_manifest(request, day)
        receipt = resume_day(config, catalog, day=day, manifest=manifest,
                             version=config['source']['contract_version'])
        if receipt is None:
            receipt = load_day(config, catalog, client, day=day, manifest=manifest,
                               require_source_ready=require_source_ready)
        if (receipt['status'] != 'written' or receipt['table_uuid'] != table_uuid
                or receipt['date'] != day.isoformat() or receipt['source_manifest_id'] != manifest
                or not isinstance(receipt.get('source_signature'), str)
                or not receipt['source_signature'] or receipt['rows_written'] <= 0):
            raise ValueError('Неверный дневной receipt')
        snapshot_id = receipt['snapshot_id']
        require_head(config, catalog, table_uuid, snapshot_id)
        receipts.append(receipt)
    require_head(config, catalog, table_uuid, snapshot_id)
    return {'status': 'written', 'request_id': request['request_id'], 'dates': request['dates'],
            'snapshot_id': snapshot_id, 'table_uuid': table_uuid, 'day_receipts': receipts}


def require_range_dq(config, request, written, checks):
    """Сверить receipts уже исполненного и сохранённого DQ; сами тесты выполняет dq/."""
    days = validate_request(config, request)
    if (written.get('status') != 'written' or written.get('request_id') != request['request_id']
            or written.get('dates') != request['dates']):
        raise ValueError('Writer receipt не соответствует запросу')
    receipts = written['day_receipts']
    if len(receipts) != len(days) or len(checks) != len(days):
        raise ValueError('Нет DQ каждого дня диапазона')
    if type(written.get('snapshot_id')) is not int or not written.get('table_uuid'):
        raise ValueError('Нет snapshot/table UUID диапазона')
    for day, receipt, check in zip(days, receipts, checks, strict=True):
        expected = {'date': day.isoformat(), 'source_manifest_id': day_manifest(request, day),
                    'table_uuid': written['table_uuid']}
        if (receipt.get('status') != 'written' or type(receipt.get('rows_written')) is not int
                or receipt['rows_written'] <= 0
                or receipt.get('source_contract_version') != config['source']['contract_version']
                or any(receipt.get(k) != v for k, v in expected.items())):
            raise ValueError('Неверный дневной writer receipt')
        expected |= {'dq_status': 'passed', 'snapshot_id': written['snapshot_id'],
                     'request_id': request['request_id'], 'rows_checked': receipt['rows_written']}
        if any(check.get(k) != v for k, v in expected.items()):
            raise ValueError(f'DQ не подтверждает точную запись за {day}')
    return written | {'status': 'ready', 'dq_status': 'passed'}
