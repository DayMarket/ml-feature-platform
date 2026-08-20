import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import DqConfigError, load_dq_settings, trino_catalog_alias


def test_defaults_without_dq_block() -> None:
    settings = load_dq_settings(
        {"table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"}}
    )
    assert settings.enabled is True
    assert settings.trino_conn_id == "trino_search"
    assert settings.scope == "partition"
    assert settings.partition_column == "date"
    assert settings.sample_rows == 5
    assert settings.query_timeout_seconds == 600
    assert settings.warmup_days == 1
    assert settings.active_from is None
    assert [spec.name for spec in settings.tests] == [
        "primary_key_not_null",
        "primary_key_unique",
        "row_count_min",
        "row_count_growth",
        "freshness",
    ]
    by_name = {spec.name: spec for spec in settings.tests}
    assert by_name["row_count_min"].params["min_rows"] == 0
    assert by_name["row_count_growth"].params["max_growth_ratio"] == 0.2
    assert by_name["row_count_growth"].params["direction"] == "both"
    assert by_name["freshness"].params["max_lag_days"] == 2
    assert all(spec.severity == "error" for spec in settings.tests)


def test_base_test_override_and_disable() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {
                "tests": [
                    {"name": "row_count_min", "min_rows": 1000},
                    {"name": "row_count_growth", "enabled": False},
                ]
            },
        }
    )
    by_name = {spec.name: spec for spec in settings.tests}
    assert by_name["row_count_min"].params["min_rows"] == 1000
    assert "row_count_growth" not in by_name


def test_optional_test_severity_and_where() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {
                "tests": [
                    {
                        "name": "accepted_range",
                        "column": "conversion_rate",
                        "min": 0,
                        "max": 1,
                        "severity": "warn",
                        "where": "platform = 'ios'",
                    }
                ]
            },
        }
    )
    spec = [item for item in settings.tests if item.name == "accepted_range"][0]
    assert spec.severity == "warn"
    assert spec.where == "platform = 'ios'"
    assert spec.family == "domain_values"
    assert spec.params == {
        "column": "conversion_rate",
        "min": 0,
        "max": 1,
        "min_inclusive": True,
        "max_inclusive": True,
        "ignore_nulls": True,
    }


def test_unknown_test_name_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "no_such_test"}]},
            }
        )
    except DqConfigError as error:
        assert "no_such_test" in str(error)
    else:
        raise AssertionError("unknown test name must be rejected")


def test_missing_required_param_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "accepted_values", "column": "platform"}]},
            }
        )
    except DqConfigError as error:
        assert "values" in str(error)
    else:
        raise AssertionError("missing required param must be rejected")


def test_bad_severity_rejected() -> None:
    try:
        load_dq_settings(
            {
                "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
                "dq": {"tests": [{"name": "non_negative", "columns": ["x"], "severity": "critical"}]},
            }
        )
    except DqConfigError as error:
        assert "severity" in str(error)
    else:
        raise AssertionError("bad severity must be rejected")


def test_catalog_alias_from_ci_config() -> None:
    assert trino_catalog_alias(Path("."), "iceberg") == "dwh-iceberg"


def main() -> int:
    test_defaults_without_dq_block()
    test_base_test_override_and_disable()
    test_optional_test_severity_and_where()
    test_unknown_test_name_rejected()
    test_missing_required_param_rejected()
    test_bad_severity_rejected()
    test_catalog_alias_from_ci_config()
    print("DQ config tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
