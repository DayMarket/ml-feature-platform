import json
import re
from pathlib import Path
from typing import Any


CREATE_TABLE_BODY_PATTERN = re.compile(
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[^\s(]+\s*\((?P<body>.*?)\)"
    r"\s*USING\s+iceberg",
    re.IGNORECASE | re.DOTALL,
)
ADD_COLUMN_PATTERN = re.compile(
    r"ALTER\s+TABLE\s+[^\s]+\s+ADD\s+COLUMN(?:\s+IF\s+NOT\s+EXISTS)?\s+"
    r"`?(?P<column>[A-Za-z_][A-Za-z0-9_]*)`?",
    re.IGNORECASE,
)
SUPPORTED_ENTITY_KEYS = {
    ("account_id",),
    ("query",),
    ("sku_group_id",),
    ("account_id", "category_id"),
    ("category_id", "sku_group_id"),
    ("query", "sku_group_id"),
}


def read_simple_nested_config(config_path: Path) -> dict[str, Any]:
    config: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, config)]
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, separator, value = raw_line.strip().partition(":")
        if not separator or not key:
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value.strip():
            parent[key.strip()] = value.strip()
        else:
            nested: dict[str, Any] = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


def parse_primary_key(primary_key: str) -> list[str]:
    return [column.strip() for column in primary_key.split(",") if column.strip()]


def extract_migration_columns(layer_path: Path) -> set[str]:
    columns: set[str] = set()
    for migration_path in sorted((layer_path / "migrations").glob("*.sql")):
        sql = migration_path.read_text(encoding="utf-8")
        for create_match in CREATE_TABLE_BODY_PATTERN.finditer(sql):
            for line in create_match.group("body").splitlines():
                normalized = line.strip().rstrip(",")
                if not normalized or normalized.startswith("--"):
                    continue
                column_match = re.match(
                    r"`?(?P<column>[A-Za-z_][A-Za-z0-9_]*)`?\s+",
                    normalized,
                )
                if column_match:
                    columns.add(column_match.group("column"))
        columns.update(match.group("column") for match in ADD_COLUMN_PATTERN.finditer(sql))
    return columns


def discover_tables(repo_root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    tables = {}
    for config_path in sorted(repo_root.glob("layers/**/config.yaml")):
        config = read_simple_nested_config(config_path)
        table = config.get("table")
        if not table:
            continue
        key = (str(table["schema"]), str(table["name"]))
        tables[key] = {
            "catalog": str(table["catalog"]),
            "schema": key[0],
            "table": key[1],
            "primary_key": parse_primary_key(str(table["primary_key"])),
            "columns": extract_migration_columns(config_path.parent),
            "config_path": config_path,
        }
    return tables


def validate_feature_group(
    config_path: Path,
    feature_group: dict[str, Any],
    tables: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    errors = []
    group_name = feature_group.get("name", "<missing name>")
    source = feature_group.get("source", {})
    source_key = (str(source.get("schema", "")), str(source.get("table", "")))
    source_limit = source.get("limit")
    if source_limit is not None and (
        isinstance(source_limit, bool)
        or not isinstance(source_limit, int)
        or source_limit <= 0
    ):
        errors.append(
            f"{config_path}: feature group {group_name} source.limit must be "
            "a positive integer"
        )
    source_delta = source.get("dq_execution_delta_minutes")
    if source_delta is not None and (
        isinstance(source_delta, bool)
        or not isinstance(source_delta, int)
        or source_delta <= 0
    ):
        errors.append(
            f"{config_path}: feature group {group_name} "
            "source.dq_execution_delta_minutes must be a positive integer"
        )
    table = tables.get(source_key)
    if not table:
        return [
            f"{config_path}: feature group {group_name} references unknown source "
            f"{source_key[0]}.{source_key[1]}"
        ]
    if table["schema"] != "gold":
        errors.append(
            f"{config_path}: feature group {group_name} must use a gold source table, "
            f"got {table['schema']}.{table['table']}"
        )

    source_catalog = str(source.get("catalog", table["catalog"]))
    if source_catalog != table["catalog"]:
        errors.append(
            f"{config_path}: feature group {group_name} source catalog "
            f"{source_catalog} does not match {table['catalog']}"
        )

    primary_key = table["primary_key"]
    date_column = "date" if "date" in primary_key else None
    if not date_column:
        errors.append(
            f"{config_path}: feature group {group_name} source table primary_key "
            "must contain date"
        )
    expected_entity_keys = [
        column for column in primary_key if column != date_column
    ]
    if tuple(sorted(expected_entity_keys)) not in SUPPORTED_ENTITY_KEYS:
        errors.append(
            f"{config_path}: feature group {group_name} has unsupported entity keys "
            f"{expected_entity_keys}"
        )

    features = feature_group.get("features", [])
    if not features:
        errors.append(f"{config_path}: feature group {group_name} has no features")
        return errors
    if len(features) != len(set(features)):
        errors.append(f"{config_path}: feature group {group_name} has duplicate features")

    required_columns = set(expected_entity_keys)
    required_columns.update(features)
    if date_column:
        required_columns.add(date_column)
    missing_columns = sorted(required_columns - table["columns"])
    if missing_columns:
        errors.append(
            f"{config_path}: feature group {group_name} columns are missing from "
            f"{table['schema']}.{table['table']} migrations: {missing_columns}"
        )

    unknown_log_features = sorted(
        set(feature_group.get("log1p_features", [])) - set(features)
    )
    if unknown_log_features:
        errors.append(
            f"{config_path}: feature group {group_name} has unknown log1p_features "
            f"{unknown_log_features}"
        )

    print(
        f"Valid ranking feature group: {group_name} -> "
        f"{table['catalog']}.{table['schema']}.{table['table']} "
        f"entity_keys={expected_entity_keys} features={len(features)}"
    )
    return errors


def validate_models(
    config_path: Path,
    config: dict[str, Any],
    feature_groups_by_name: dict[str, dict[str, Any]],
) -> list[str]:
    errors = []
    models = config.get("models")
    if not isinstance(models, list) or not models:
        return [f"{config_path}: models must not be empty"]

    seen_model_names: set[str] = set()
    referenced_group_names: set[str] = set()
    for model in models:
        model_name = str(model.get("name", ""))
        if not model_name:
            errors.append(f"{config_path}: model name must not be empty")
            continue
        if model_name in seen_model_names:
            errors.append(f"{config_path}: duplicate model name {model_name}")
        seen_model_names.add(model_name)

        model_feature_groups = model.get("feature_groups")
        if not isinstance(model_feature_groups, list) or not model_feature_groups:
            errors.append(f"{config_path}: model {model_name} feature_groups must not be empty")
            continue

        model_group_names: set[str] = set()
        for raw_group in model_feature_groups:
            if not isinstance(raw_group, dict):
                errors.append(
                    f"{config_path}: model {model_name} feature_groups items "
                    "must be objects with name and features"
                )
                continue

            group_name = str(raw_group.get("name", ""))
            if not group_name:
                errors.append(f"{config_path}: model {model_name} has empty feature group name")
                continue
            if group_name in model_group_names:
                errors.append(
                    f"{config_path}: model {model_name} references feature group "
                    f"{group_name} more than once"
                )
            model_group_names.add(group_name)
            referenced_group_names.add(group_name)

            feature_group = feature_groups_by_name.get(group_name)
            if feature_group is None:
                errors.append(
                    f"{config_path}: model {model_name} references unknown "
                    f"feature group {group_name}"
                )
                continue

            features = raw_group.get("features")
            if not isinstance(features, list) or not features:
                errors.append(
                    f"{config_path}: model {model_name} feature group "
                    f"{group_name} features must not be empty"
                )
                continue
            if len(features) != len(set(features)):
                errors.append(
                    f"{config_path}: model {model_name} feature group "
                    f"{group_name} has duplicate features"
                )

            catalog_features = set(feature_group.get("features", []))
            unknown_features = sorted(set(features) - catalog_features)
            if unknown_features:
                errors.append(
                    f"{config_path}: model {model_name} feature group "
                    f"{group_name} references unknown features {unknown_features}"
                )

    unused_group_names = sorted(set(feature_groups_by_name) - referenced_group_names)
    if unused_group_names:
        errors.append(
            f"{config_path}: feature groups are not referenced by any model: "
            f"{unused_group_names}"
        )
    return errors


def print_model_components(config_path: Path, config: dict[str, Any]) -> None:
    models = config["models"]
    parent = {str(model["name"]): str(model["name"]) for model in models}

    def find(model_name: str) -> str:
        while parent[model_name] != model_name:
            parent[model_name] = parent[parent[model_name]]
            model_name = parent[model_name]
        return model_name

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    feature_group_models: dict[str, list[str]] = {}
    for model in models:
        model_name = str(model["name"])
        group_names = [str(group["name"]) for group in model["feature_groups"]]
        print(f"Ranking upload model: {model_name} feature_groups={group_names}")
        for group_name in group_names:
            feature_group_models.setdefault(group_name, []).append(model_name)

    for sharing_models in feature_group_models.values():
        first_model = sharing_models[0]
        for model_name in sharing_models[1:]:
            union(first_model, model_name)

    components: dict[str, list[str]] = {}
    for model in models:
        model_name = str(model["name"])
        components.setdefault(find(model_name), []).append(model_name)

    for component_index, component_models in enumerate(components.values(), start=1):
        component_groups = sorted(
            group_name
            for group_name, sharing_models in feature_group_models.items()
            if set(sharing_models) & set(component_models)
        )
        print(
            f"Ranking upload component {component_index} "
            f"config={config_path} models={component_models} "
            f"feature_groups={component_groups}"
        )


def main() -> int:
    repo_root = Path(".")
    tables = discover_tables(repo_root)
    errors = []
    group_names: set[str] = set()
    config_paths = sorted(repo_root.glob("upload/**/config.yaml"))
    if not config_paths:
        print("No ranking upload configs found")
        return 0

    for config_path in config_paths:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        feature_groups = config.get("feature_groups", [])
        if not feature_groups:
            errors.append(f"{config_path}: feature_groups must not be empty")
        feature_groups_by_name = {
            str(feature_group.get("name", "")): feature_group
            for feature_group in feature_groups
        }
        errors.extend(validate_models(config_path, config, feature_groups_by_name))
        for feature_group in feature_groups:
            group_name = feature_group.get("name", "")
            if group_name in group_names:
                errors.append(
                    f"{config_path}: duplicate ranking feature group name {group_name}"
                )
            group_names.add(group_name)
            errors.extend(validate_feature_group(config_path, feature_group, tables))
        if not errors:
            print_model_components(config_path, config)

    if errors:
        print("Invalid ranking upload configs:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Ranking upload config validation completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
