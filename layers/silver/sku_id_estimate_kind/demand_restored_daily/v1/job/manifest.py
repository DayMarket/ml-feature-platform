"""Проверить дневной состав output_manifest и SHA-256 исходных значений E3."""

from datetime import date, datetime, timezone
from hashlib import sha256
import json
import math
import re

from .preparation import MEASURES, validate_run
from .query import source_ref

RAW_COLUMNS = (
    "date", "sku_id", "estimate_kind", "run_id", "prediction_date",
    *MEASURES, "currency_code", "price_model_version", "rate_ok", "settled_at",
    "method_version", "quality_status", "unavailable_reason", "source_updated_at",
)
ALGORITHM = "e3_daily_rows_sha256_v1"


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Повтор JSON ключа {key}")
        result[key] = value
    return result


def day_manifest(config, selected, run, day):
    """Состав дня берётся из готового паспорта, не из выгруженного числа строк."""
    validate_run(run, selected)
    source_ref(config)
    if type(day) is not date or not selected["start"] <= day < selected["end"]:
        raise ValueError("День вне выбранного диапазона E3")
    document = json.loads(run["output_manifest"], object_pairs_hook=unique_object)
    block = document.get("fp_daily_copy")
    if not isinstance(block, dict):
        raise ValueError("Нет fp_daily_copy в output_manifest")
    if (type(block.get("version")) is not int or block["version"] != 1
            or block.get("checksum_algorithm") != ALGORITHM):
        raise ValueError("Неподдержанный формат/алгоритм manifest")
    source = config["source"]
    if (block.get("table") != f'{source["database"]}.{source["table"]}'
            or block.get("run_id") != selected["run_id"]
            or block.get("prediction_date") != selected["prediction_date"].isoformat()):
        raise ValueError("Manifest другой таблицы/run/cutoff")
    entries = block.get("days")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Нет дневного состава E3")
    previous, found = None, None
    covered = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"date", "rows", "estimate_counts", "sha256"}:
            raise ValueError("Неверный состав записи дня manifest")
        value = entry["date"]
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("Нужна ISO DATE в manifest")
        current = date.fromisoformat(value)
        if current >= selected["prediction_date"] or (previous is not None and current <= previous):
            raise ValueError("Дни manifest повторяются, не отсортированы или выходят за cutoff")
        previous = current
        counts = entry["estimate_counts"]
        if (not isinstance(counts, dict) or not counts or set(counts) - {"provisional", "final"}
                or any(type(n) is not int or n <= 0 for n in counts.values())
                or type(entry["rows"]) is not int or entry["rows"] != sum(counts.values())):
            raise ValueError("Неверные counts дня E3")
        if not isinstance(entry["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]):
            raise ValueError("Нет SHA-256 дня E3")
        if selected["start"] <= current < selected["end"]:
            covered.add(current)
        if current == day:
            found = entry
    if len(covered) != (selected["end"] - selected["start"]).days or found is None:
        raise ValueError("Manifest не покрывает весь запрошенный диапазон")
    # Обычное форматирование JSON не должно менять identity содержимого паспорта.
    canonical = json.dumps(document, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
    return dict(found) | {"output_manifest_sha256": sha256(canonical.encode("utf-8")).hexdigest(),
                          "source_run_id": selected["run_id"],
                          "source_prediction_date": selected["prediction_date"].isoformat(),
                          "source_state_version": run["state_version"]}


def row_bytes(row):
    """Канонический wire: даты ISO, Float64.hex, UTC микросекунды, JSON-массив UTF-8."""
    values = []
    for name in RAW_COLUMNS:
        value = row[name]
        if value is None:
            values.append(None)
        elif name in MEASURES:
            if type(value) is not float or not math.isfinite(value):
                raise ValueError("Checksum требует конечный исходный Float64")
            values.append(value.hex())
        elif name == "source_updated_at":
            if not isinstance(value, datetime):
                raise ValueError("Checksum требует timestamp")
            # На этой границе Arrow writer уже нормализовал TIMESTAMP в naive UTC.
            utc = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
            values.append(utc.isoformat(timespec="microseconds").replace("+00:00", "Z"))
        elif name in {"date", "prediction_date", "settled_at"}:
            if type(value) is not date:
                raise ValueError("Checksum требует DATE")
            values.append(value.isoformat())
        elif name in {"sku_id", "rate_ok"}:
            if type(value) is not int:
                raise ValueError("Checksum требует integer")
            values.append(value)
        elif isinstance(value, str):
            values.append(value)
        else:
            raise ValueError(f"Неверный checksum type {name}")
    return json.dumps(values, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


class ContentDigest:
    """Порядок (date,sku_id,estimate_kind) и состав сохраняются между порциями."""

    def __init__(self):
        self._digest = sha256((ALGORITHM + "\n").encode("ascii"))
        self._previous = None
        self.counts = {}
        self.rows = 0

    def update(self, batch):
        if not set(RAW_COLUMNS).issubset(batch.column_names):
            raise ValueError("Не хватает исходных колонок checksum")
        for row in batch.select(RAW_COLUMNS).to_pylist():
            key = row["date"], row["sku_id"], row["estimate_kind"]
            if row["estimate_kind"] not in {"provisional", "final"}:
                raise ValueError("Неизвестный estimate_kind")
            if self._previous is not None and key <= self._previous:
                raise ValueError("Неверный порядок/дубликат checksum")
            payload = row_bytes(row)
            self._digest.update(len(payload).to_bytes(8, "big"))
            self._digest.update(payload)
            self._previous = key
            self.rows += 1
            self.counts[key[2]] = self.counts.get(key[2], 0) + 1

    def hexdigest(self):
        return self._digest.hexdigest()

    def verify(self, entry):
        if (self.rows != entry["rows"] or self.counts != entry["estimate_counts"]
                or self.hexdigest() != entry["sha256"]):
            raise ValueError("Поток E3 не совпал с counts/checksum output_manifest")
