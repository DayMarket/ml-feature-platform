"""Observed допускает только полное покрытие выбранных дней точным upstream DQ."""

from copy import deepcopy
from datetime import timedelta
from importlib import import_module

import pytest
import yaml

from ci_test.test_demand_observed_preparation import DAY, ENTITY, NOW, ROOT, VERSIONS, schema, source
from ci_test.test_demand_observed_writer import env as env

INPUTS = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.inputs")


def payload():
    config = yaml.safe_load((ENTITY / "config.yaml").read_text())
    sources = INPUTS.source_configs(config, ROOT)
    references = {kind: {"dag_id": cfg["dag"]["id"], "run_id": f"run-{kind}"}
                  for kind, cfg in sources.items()}
    checked = {}
    for kind, cfg in sources.items():
        receipt = {"status": "written", "date": DAY.isoformat(), "rows_written": 1,
                   "table_uuid": VERSIONS[kind]["table_uuid"], "source_manifest_id": "capture",
                   "source_contract_version": cfg["source"]["contract_version"], "ingested_at": NOW.isoformat()}
        written = {"status": "written", "request_id": f"request-{kind}", **VERSIONS[kind],
                   "dates": [DAY.isoformat()], "day_receipts": [receipt]}
        check = {"date": DAY.isoformat(), "dq_status": "passed", **references[kind],
                 "request_id": written["request_id"], **VERSIONS[kind],
                 "source_manifest_id": "capture", "rows_checked": 1}
        checked[kind] = {"dq_status": "passed", **references[kind], "receipt": written, "day_checks": [check]}
    return config, sources, references, checked


def bind(data, **options):
    _, sources, refs, checked = data
    return INPUTS.bind_inputs(sources, refs, checked, days=options.get("days", [DAY]),
                              captured_at=options.get("captured_at", NOW))


def test_passed_inputs_bind_both_versions_and_detach_receipts():
    data = payload()
    result = bind(data)
    assert set(result) == {"sales", "stock"}
    assert result["sales"]["snapshot_id"] == VERSIONS["sales"]["snapshot_id"]
    assert result["stock"]["day_receipts"][DAY.isoformat()]["rows_written"] == 1
    data[3]["stock"]["receipt"]["day_receipts"][0]["rows_written"] = 99
    assert result["stock"]["day_receipts"][DAY.isoformat()]["rows_written"] == 1


@pytest.mark.parametrize("field,value", [("dq_status", "failed"), ("dag_id", "other"),
    ("run_id", "other"), ("request_id", "other"), ("snapshot_id", True), ("table_uuid", "other"),
    ("rows_checked", True), ("rows_checked", 2), ("source_manifest_id", "other")])
def test_mismatched_day_check_never_admits_input(field, value):
    data = payload()
    data[3]["sales"]["day_checks"][0][field] = value
    with pytest.raises(ValueError, match="DQ"):
        bind(data)


@pytest.mark.parametrize("failure", ["missing_source", "outer_run", "outer_status", "owner", "no_checks",
    "extra_checks", "wrong_contract", "future_capture", "missing_date", "duplicate_dates", "bad_date"])
def test_incomplete_or_unrelated_proof_rejected(failure):
    data = payload()
    _, sources, references, checked = data
    options = {}
    if failure == "missing_source":
        del checked["stock"]
    elif failure == "outer_run":
        checked["sales"]["run_id"] = "other"
    elif failure == "outer_status":
        checked["sales"]["dq_status"] = "written"
    elif failure == "owner":
        references["sales"]["dag_id"] = sources["stock"]["dag"]["id"]
    elif failure == "no_checks":
        checked["sales"]["day_checks"] = []
    elif failure == "extra_checks":
        checked["sales"]["day_checks"] *= 2
    elif failure == "wrong_contract":
        checked["sales"]["receipt"]["day_receipts"][0]["source_contract_version"] = "old"
    elif failure == "future_capture":
        options["captured_at"] = NOW - timedelta(seconds=1)
    else:
        options["days"] = {"missing_date": [DAY + timedelta(days=1)],
                           "duplicate_dates": [DAY, DAY], "bad_date": [NOW]}[failure]
    with pytest.raises(ValueError):
        bind(data, **options)


def test_preflight_uses_exact_snapshot_even_with_newer_head(env):
    config, catalog, _ = env
    _, sources, _, _ = payload()
    catalog.create_namespace("silver")
    bound = deepcopy(VERSIONS)
    for kind, cfg in sources.items():
        table = catalog.create_table((cfg["table"]["schema"], cfg["table"]["name"]),
                                     schema=schema((ROOT / config["inputs"][f"{kind}_config"]).parent))
        table.append(source(kind, [1]))
        bound[kind] = {"snapshot_id": table.current_snapshot().snapshot_id,
                       "table_uuid": str(table.metadata.table_uuid)}
        table.append(source(kind, [2]))
    result = INPUTS.preflight_inputs(config, sources, catalog, bound)
    assert set(result) == {"sales", "stock", "output"}
    for kind in sources:
        assert result[kind].scan(snapshot_id=bound[kind]["snapshot_id"]).count() == 1
        assert result[kind].scan().count() == 2
    bound["sales"]["snapshot_id"] = 1
    with pytest.raises(ValueError, match="latest"):
        INPUTS.preflight_inputs(config, sources, catalog, bound)


def test_config_path_cannot_escape_repo(tmp_path):
    config = payload()[0]
    config["inputs"]["sales_config"] = "../outside.yaml"
    with pytest.raises(ValueError, match="внутри FP"):
        INPUTS.source_configs(config, tmp_path)


def test_daily_lineage_does_not_duplicate_full_history():
    bound = bind(payload())
    day = INPUTS.day_inputs(bound, DAY)
    assert all("day_receipts" not in source for source in day.values())
    assert day["sales"]["day_receipt"]["date"] == DAY.isoformat()
    day["sales"]["day_receipt"]["rows_written"] = 99
    assert bound["sales"]["day_receipts"][DAY.isoformat()]["rows_written"] == 1
    with pytest.raises(ValueError, match="Нет проверенного дня"):
        INPUTS.day_inputs(bound, DAY + timedelta(days=1))
