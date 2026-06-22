import os
import re
from pathlib import Path


REQUIRED_FILES = (
    ".drone.yaml",
    "ci_config.yaml",
    "layers/silver/sku_group_id_query_category/sku_group_install/v1/dag.py",
    "layers/silver/sku_group_id_query_category/sku_group_install/v1/config.yaml",
)

CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?P<table>[^\s(]+)",
    re.IGNORECASE,
)

PRIMARY_KEY_GROUP_EXCEPTIONS = {
    ("silver", "sku_group_install"): "sku_group_id_query_category",
}


def main() -> int:
    branch = os.getenv("DRONE_COMMIT_BRANCH", "local")
    print(f"Running test step for branch: {branch}")
    print_secret_status()

    missing_files = [
        file_path
        for file_path in REQUIRED_FILES
        if not Path(file_path).is_file()
    ]
    if missing_files:
        print("Missing required files:")
        for file_path in missing_files:
            print(f"- {file_path}")
        return 1

    layout_errors = validate_layer_layout(Path("."))
    if layout_errors:
        print("Invalid layer layout:")
        for error in layout_errors:
            print(f"- {error}")
        return 1

    config_errors = validate_table_configs(Path("."))
    if config_errors:
        print("Invalid table configs:")
        for error in config_errors:
            print(f"- {error}")
        return 1

    print_created_tables()

    migration_errors = validate_migrations_are_idempotent(Path("."))
    if migration_errors:
        print("Non-idempotent migrations:")
        for error in migration_errors:
            print(f"- {error}")
        return 1

    print("Test step completed successfully")
    return 0


def print_secret_status() -> None:
    if os.getenv("SECRET"):
        print("Drone secret SECRET is available")
    else:
        print("Drone secret SECRET is not available")


def print_created_tables() -> None:
    created_tables = find_created_tables(Path("."))
    if not created_tables:
        print("No CREATE TABLE statements found")
        return

    print("Tables created by migrations:")
    for migration_path, table_name in created_tables:
        print(f"- {table_name} ({migration_path})")


def find_created_tables(repo_root: Path) -> list[tuple[str, str]]:
    created_tables = []
    for migration_path in sorted(repo_root.glob("layers/**/migrations/*.sql")):
        sql = migration_path.read_text(encoding="utf-8")
        for match in CREATE_TABLE_PATTERN.finditer(sql):
            created_tables.append(
                (
                    migration_path.as_posix(),
                    normalize_table_name(match.group("table")),
                )
            )
    return created_tables


def validate_migrations_are_idempotent(repo_root: Path) -> list[str]:
    errors = []
    for migration_path in sorted(repo_root.glob("layers/**/migrations/*.sql")):
        statements = split_sql(migration_path.read_text(encoding="utf-8"))
        for statement in statements:
            normalized = " ".join(statement.split()).upper()
            if normalized.startswith("CREATE TABLE") and not normalized.startswith(
                "CREATE TABLE IF NOT EXISTS"
            ):
                errors.append(
                    f"{migration_path}: CREATE TABLE must use IF NOT EXISTS"
                )
            if normalized.startswith("ALTER TABLE") and " ADD COLUMN " in normalized:
                if " ADD COLUMN IF NOT EXISTS " not in normalized:
                    errors.append(
                        f"{migration_path}: ADD COLUMN must use IF NOT EXISTS"
                    )
            if normalized.startswith(("DROP ", "DELETE ", "TRUNCATE ")):
                errors.append(f"{migration_path}: destructive statement is not allowed")
    return errors


def split_sql(sql: str) -> list[str]:
    statements = []
    current_lines = []
    for line in sql.splitlines():
        if line.lstrip().startswith("--"):
            continue
        current_lines.append(line)
        if line.rstrip().endswith(";"):
            statement = "\n".join(current_lines).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current_lines = []

    tail_statement = "\n".join(current_lines).strip()
    if tail_statement:
        statements.append(tail_statement)
    return statements


def normalize_table_name(table_name: str) -> str:
    return table_name.strip().strip(";").replace("{target_table}", "<target_table>")


def validate_table_configs(repo_root: Path) -> list[str]:
    errors = []
    for config_path in sorted(repo_root.glob("layers/**/config.yaml")):
        config = read_simple_nested_config(config_path)
        table = config.get("table")
        if not table:
            continue

        missing_fields = []
        for field_name in ("catalog", "schema", "name", "primary_key"):
            if not table.get(field_name):
                missing_fields.append(f"table.{field_name}")

        meta = table.get("meta")
        if not isinstance(meta, dict) or not meta.get("team"):
            missing_fields.append("table.meta.team")

        if missing_fields:
            errors.append(f"{config_path}: missing {', '.join(missing_fields)}")
            continue

        print(
            "Valid table config: "
            f"{config_path} -> {table['catalog']}.{table['schema']}.{table['name']} "
            f"primary_key={table['primary_key']} team={meta['team']}"
        )
    return errors


def validate_layer_layout(repo_root: Path) -> list[str]:
    errors = []
    for config_path in sorted(repo_root.glob("layers/**/config.yaml")):
        relative = config_path.relative_to(repo_root)
        if len(relative.parts) != 6:
            errors.append(
                f"{config_path}: expected layers/<layer>/<primary_key_group>/"
                "<entity>/vN/config.yaml"
            )
            continue

        _, layer, actual_group, entity, version, _ = relative.parts
        if not re.fullmatch(r"v[1-9][0-9]*", version):
            errors.append(f"{config_path}: invalid version directory {version!r}")
            continue

        config = read_simple_nested_config(config_path)
        table = config.get("table", {})
        primary_key = [
            column.strip()
            for column in str(table.get("primary_key", "")).split(",")
            if column.strip() and column.strip() != "date"
        ]
        expected_group = PRIMARY_KEY_GROUP_EXCEPTIONS.get(
            (layer, entity), "_".join(primary_key)
        )
        if not expected_group:
            errors.append(f"{config_path}: primary key has no non-date columns")
            continue
        if actual_group != expected_group:
            errors.append(
                f"{config_path}: group {actual_group!r} must be {expected_group!r}"
            )

        expected_dag_id = f"feature-platform.layers.{layer}.{actual_group}.{entity}"
        dag_path = config_path.parent / "dag.py"
        readme_path = config_path.parent / "README.md"
        dag_contract = config_path.read_text(encoding="utf-8")
        if dag_path.is_file():
            dag_contract += dag_path.read_text(encoding="utf-8")
        if expected_dag_id not in dag_contract:
            errors.append(f"{config_path}: missing DAG id {expected_dag_id!r}")
        if not readme_path.is_file() or expected_dag_id not in readme_path.read_text(
            encoding="utf-8"
        ):
            errors.append(f"{config_path}: README must state DAG id {expected_dag_id!r}")

    return errors


def read_simple_nested_config(config_path: Path) -> dict:
    config = {}
    stack = [(-1, config)]
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        key, separator, value = line.partition(":")
        if not separator or not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        key = key.strip()
        value = value.strip()
        if value:
            parent[key] = value
        else:
            nested = {}
            parent[key] = nested
            stack.append((indent, nested))
    return config


if __name__ == "__main__":
    raise SystemExit(main())
