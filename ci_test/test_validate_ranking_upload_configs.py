import importlib.util
from pathlib import Path


def load_validator():
    module_path = Path("scripts/validate_ranking_upload_configs.py")
    spec = importlib.util.spec_from_file_location(
        "validate_ranking_upload_configs",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def main() -> int:
    validator = load_validator()
    config_path = Path("upload/features_service_upload/v1/config.yaml")
    feature_groups_by_name = {
        "fs_price": {
            "name": "fs_price",
            "features": ["sell_price_eod", "abs_discount", "fraq_discount"],
        }
    }
    valid_config = {
        "models": [
            {
                "name": "model_core",
                "feature_groups": [
                    {
                        "name": "fs_price",
                        "features": ["sell_price_eod", "abs_discount"],
                    }
                ],
            },
            {
                "name": "model_extended",
                "feature_groups": [
                    {
                        "name": "fs_price",
                        "features": [
                            "sell_price_eod",
                            "abs_discount",
                            "fraq_discount",
                        ],
                    }
                ],
            },
        ]
    }
    assert validator.validate_models(
        config_path,
        valid_config,
        feature_groups_by_name,
    ) == []

    invalid_config = {
        "models": [
            {
                "name": "model_unknown_feature",
                "feature_groups": [
                    {
                        "name": "fs_price",
                        "features": ["sell_price_eod", "unknown_feature"],
                    }
                ],
            }
        ]
    }
    errors = validator.validate_models(
        config_path,
        invalid_config,
        feature_groups_by_name,
    )
    assert any("unknown_feature" in error for error in errors)

    timestamp_tables = {
        (
            "gold",
            "feature_platform_dynamic_pricing_sku_group_price_features",
        ): {
            "catalog": "iceberg",
            "schema": "gold",
            "table": "feature_platform_dynamic_pricing_sku_group_price_features",
            "primary_key": ["calculated_at", "sku_group_id", "promotion_id"],
            "columns": {
                "calculated_at",
                "sku_group_id",
                "promotion_id",
                "avg_sell_price",
            },
        }
    }
    timestamp_feature_group = {
        "source": {
            "schema": "gold",
            "table": "feature_platform_dynamic_pricing_sku_group_price_features",
            "timestamp_column": "calculated_at",
            "read_mode": "latest_timestamp",
            "dq_execution_delta_minutes": 0,
        },
        "name": "fs_dynamic_pricing",
        "features": ["avg_sell_price"],
    }
    assert validator.validate_feature_group(
        config_path,
        timestamp_feature_group,
        timestamp_tables,
    ) == []

    invalid_timestamp_feature_group = {
        **timestamp_feature_group,
        "source": {
            "schema": "gold",
            "table": "feature_platform_dynamic_pricing_sku_group_price_features",
            "timestamp_column": "calculated_at",
        },
    }
    errors = validator.validate_feature_group(
        config_path,
        invalid_timestamp_feature_group,
        timestamp_tables,
    )
    assert any("read_mode=latest_timestamp" in error for error in errors)

    print("Ranking upload model manifest validation tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
