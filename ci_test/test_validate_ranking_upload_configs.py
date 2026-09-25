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


def check_postgres_sink_skips_ranking_checks(validator) -> list[str]:
    """Postgres-выгрузка не имеет models и не обязана иметь ranking-группы."""
    errors = []
    if validator.sink_type({}) != "kafka":
        errors.append("конфиг без ключа sink обязан считаться kafka-выгрузкой")
    if validator.sink_type({"sink": {"type": "postgres"}}) != "postgres":
        errors.append("sink.type postgres не распознан")
    postgres_config = {
        "sink": {"type": "postgres"},
        "feature_groups": [
            {
                "name": "sku_buyout_features_postgres",
                "source": {
                    "schema": "gold",
                    "table": "feature_platform_sku_buyout_features",
                    "dependency_dag_id": (
                        "feature-platform.layers.gold.sku_id.sku_buyout_features"
                    ),
                    "dependency_execution_delta_minutes": 0,
                    "dependency_task_id": "dq",
                },
                "features": ["sku_buyout"],
            }
        ],
    }
    model_errors = validator.validate_models(
        Path("upload/buyout_sku_postgres_upload/v1/config.yaml"),
        postgres_config,
        {},
    )
    if model_errors:
        errors.append(
            "validate_models не должен вызываться для postgres-выгрузки, "
            f"получено: {model_errors}"
        )
    return errors


def check_sink_requires_connection_schema_table(validator) -> list[str]:
    """Postgres sink обязан объявить БД, connection_id, schema и table."""
    errors = []
    config_path = Path("upload/buyout_sku_postgres_upload/v1/config.yaml")

    full_sink_errors = validator.validate_sink(
        config_path,
        {
            "sink": {
                "type": "postgres",
                "connection_id": "postgres_non_buyout_service_connect",
                "database": "mlgrowth",
                "schema": "public",
                "table": "sku_buyout_features",
            }
        },
    )
    if full_sink_errors:
        errors.append(
            f"полный sink-блок не должен давать ошибок, получено: {full_sink_errors}"
        )

    kafka_sink_errors = validator.validate_sink(config_path, {"sink": {"type": "kafka"}})
    if kafka_sink_errors:
        errors.append(
            f"kafka-выгрузка не должна проверяться на connection_id/schema/table, "
            f"получено: {kafka_sink_errors}"
        )

    for missing_field in ("database", "connection_id", "schema", "table"):
        sink = {
            "type": "postgres",
            "connection_id": "postgres_non_buyout_service_connect",
            "database": "mlgrowth",
            "schema": "public",
            "table": "sku_buyout_features",
        }
        sink.pop(missing_field)
        found = validator.validate_sink(config_path, {"sink": sink})
        if not any(f"sink.{missing_field}" in error for error in found):
            errors.append(
                f"отсутствие sink.{missing_field} должно быть замечено, "
                f"получено: {found}"
            )

    empty_string_errors = validator.validate_sink(
        config_path,
        {
            "sink": {
                "type": "postgres",
                "connection_id": "",
                "database": "mlgrowth",
                "schema": "public",
                "table": "sku_buyout_features",
            }
        },
    )
    if not any("sink.connection_id" in error for error in empty_string_errors):
        errors.append(
            f"пустая строка в sink.connection_id должна считаться ошибкой, "
            f"получено: {empty_string_errors}"
        )

    return errors


def check_full_table_and_dq_waiver(validator) -> list[str]:
    """full_table требует date в ключе; сенсор не на dq — только с явной причиной."""
    errors = []
    config_path = Path("upload/query_category_relevance_upload/v1/config.yaml")
    table_key = ("gold", "feature_platform_query_category_relevance")
    base_table = {
        "catalog": "iceberg",
        "schema": "gold",
        "table": table_key[1],
        "primary_key": ["date", "category_id", "query_text"],
        "columns": {"date", "query_id", "query_text", "category_id", "relevance"},
        "has_dq_task": False,
    }
    base_group = {
        "source": {
            "schema": "gold",
            "table": table_key[1],
            "read_mode": "full_table",
            "dependency_dag_id": (
                "feature-platform.layers.gold.category_id_query_text."
                "query_category_relevance"
            ),
            "dependency_execution_delta_minutes": 60,
            "dependency_task_id": "materialize",
            "dq_waiver_reason": "DAG витрины — заглушка без dq",
        },
        "name": "query_category_relevance",
        "features": ["relevance"],
    }

    def run(group=base_group, table=base_table):
        return validator.validate_feature_group(config_path, group, {table_key: table})

    found = run()
    if found:
        errors.append(f"category_id,query_text + full_table + waiver валиден, получено: {found}")

    no_reason = {**base_group, "source": {**base_group["source"]}}
    no_reason["source"].pop("dq_waiver_reason")
    found = run(no_reason)
    if not any('dependency_task_id must be "dq"' in error for error in found):
        errors.append(f"без dq_waiver_reason сенсор не на dq должен падать, получено: {found}")

    found = run(table={**base_table, "has_dq_task": True})
    if not any("dq_waiver_reason" in error for error in found):
        errors.append(
            f"исключение при DAG'е-владельце с build_dq_task должно падать, получено: {found}"
        )

    with_dq = {**base_group, "source": {**base_group["source"], "dependency_task_id": "dq"}}
    found = run(with_dq, {**base_table, "has_dq_task": True})
    if not any("dq_waiver_reason" in error for error in found):
        errors.append(f"dq_waiver_reason вместе с сенсором на dq лишний, получено: {found}")

    no_date_table = {
        **base_table,
        "primary_key": ["category_id", "query_text"],
    }
    found = run(table=no_date_table)
    if not any("read_mode=full_table" in error for error in found):
        errors.append(f"full_table без date в primary_key должен падать, получено: {found}")

    unknown_mode = {**base_group, "source": {**base_group["source"], "read_mode": "all"}}
    found = run(unknown_mode)
    if not any("read_mode" in error for error in found):
        errors.append(f"неизвестный read_mode должен падать, получено: {found}")

    return errors


def check_query_id_dictionary(validator) -> list[str]:
    """Справочник query_id: только full_table, ключ с query_text, query_id в обеих таблицах."""
    errors = []
    config_path = Path("upload/query_category_relevance_upload/v1/config.yaml")
    source_key = ("gold", "feature_platform_query_category_relevance")
    dictionary_key = ("gold", "feature_platform_search_query_id")
    source_table = {
        "catalog": "iceberg",
        "schema": "gold",
        "table": source_key[1],
        "primary_key": ["date", "category_id", "query_text"],
        "columns": {"date", "query_id", "query_text", "category_id", "relevance"},
        "has_dq_task": False,
    }
    dictionary_table = {
        "catalog": "iceberg",
        "schema": "gold",
        "table": dictionary_key[1],
        "primary_key": ["query_text", "version"],
        "columns": {"updated_at", "query_text", "query_id", "version"},
        "has_dq_task": True,
    }
    group = {
        "source": {
            "schema": "gold",
            "table": source_key[1],
            "read_mode": "full_table",
            "dependency_dag_id": (
                "feature-platform.layers.gold.category_id_query_text."
                "query_category_relevance"
            ),
            "dependency_execution_delta_minutes": 60,
            "dependency_task_id": "materialize",
            "dq_waiver_reason": "DAG витрины — заглушка без dq",
            "query_id_dictionary": {"schema": "gold", "table": dictionary_key[1]},
        },
        "name": "query_category_relevance",
        "features": ["relevance"],
    }

    def run(group=group, source=source_table, dictionary=dictionary_table):
        tables = {source_key: source}
        if dictionary is not None:
            tables[dictionary_key] = dictionary
        return validator.validate_feature_group(config_path, group, tables)

    def with_source(**changes):
        return {**group, "source": {**group["source"], **changes}}

    found = run()
    if found:
        errors.append(f"full_table + query_id_dictionary валиден, получено: {found}")

    invalid_cases = {
        "справочник не объявлен в layers": run(dictionary=None),
        "в справочнике нет query_id": run(
            dictionary={**dictionary_table, "columns": {"query_text", "version"}}
        ),
        "в витрине нет query_id": run(
            source={**source_table, "columns": source_table["columns"] - {"query_id"}}
        ),
        "read_mode не full_table": run(with_source(read_mode=None)),
        "справочник без table": run(with_source(query_id_dictionary={"schema": "gold"})),
        "ключ без query_text": run(
            source={
                **source_table,
                "primary_key": ["date", "category_id", "sku_group_id"],
                "columns": source_table["columns"] | {"sku_group_id"},
            }
        ),
    }
    for case, found in invalid_cases.items():
        if not any("query_id_dictionary" in error for error in found):
            errors.append(f"{case}: ожидалась ошибка query_id_dictionary, получено: {found}")

    return errors


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
            "dependency_dag_id": (
                "feature-platform.layers.gold.calculated_at_sku_group_id_promotion_id."
                "dynamic_pricing_sku_group_price_features"
            ),
            "dependency_execution_delta_minutes": 0,
            "dependency_task_id": "dq",
        },
        "name": "fs_dynamic_pricing",
        "features": ["avg_sell_price"],
    }
    assert validator.validate_feature_group(
        config_path,
        timestamp_feature_group,
        timestamp_tables,
    ) == []

    # Репозиторный источник обязан ждать таску dq, а не весь DAG-владелец:
    # успешная запись при упавшем DQ не даёт права публиковать фичи.
    for bad_task_id in (None, "getting_dynamic_pricing_sku_group_price_features"):
        source = {**timestamp_feature_group["source"]}
        if bad_task_id is None:
            source.pop("dependency_task_id")
        else:
            source["dependency_task_id"] = bad_task_id
        errors = validator.validate_feature_group(
            config_path,
            {**timestamp_feature_group, "source": source},
            timestamp_tables,
        )
        assert any(
            'dependency_task_id must be "dq"' in error for error in errors
        ), (bad_task_id, errors)

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

    external_feature_group = {
        "source": {
            "external": True,
            "catalog": "iceberg",
            "schema": "um_prod_feature_store_iceberg",
            "table": "cold_start_boosted_pw_convs_query_atc_order_90",
            "primary_key": ["date", "query", "sku_group_id"],
            "columns": [
                "date",
                "query",
                "sku_group_id",
                "query_skg_conv_imp2atc_90",
                "query_skg_conv_imp2order_90",
            ],
            "dependency_dag_id": "spark.pyspark_feature_store_dag",
            "dependency_task_id": "fetch_boosted_conversions_etl",
            "dependency_execution_delta_minutes": 240,
        },
        "name": "fs_search_query_skg_atc_order_features_cold_start",
        "features": [
            "query_skg_conv_imp2atc_90",
            "query_skg_conv_imp2order_90",
        ],
    }
    assert validator.validate_feature_group(
        config_path,
        external_feature_group,
        {},
    ) == []

    errors = check_postgres_sink_skips_ranking_checks(validator)
    assert not errors, errors

    errors = check_sink_requires_connection_schema_table(validator)
    assert not errors, errors

    errors = check_full_table_and_dq_waiver(validator)
    assert not errors, errors

    errors = check_query_id_dictionary(validator)
    assert not errors, errors

    print("Ranking upload model manifest validation tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
