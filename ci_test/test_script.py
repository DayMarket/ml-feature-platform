import os
import re
from pathlib import Path


REQUIRED_FILES = (
    ".drone.yaml",
    "ci_config.yaml",
    "layers/silver/sku_group_install/v1/dag.py",
    "layers/silver/sku_group_install/v1/config.yaml",
)

CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?P<table>[^\s(]+)",
    re.IGNORECASE,
)


def main() -> int:
    branch = os.getenv("DRONE_COMMIT_BRANCH", "local")
    print(f"Running test step for branch: {branch}")
    print_secret_status()

    missing_files = [file_path for file_path in REQUIRED_FILES if not Path(file_path).is_file()]
    if missing_files:
        print("Missing required files:")
        for file_path in missing_files:
            print(f"- {file_path}")
        return 1

    config_errors = validate_table_configs(Path("."))
    if config_errors:
        print("Invalid table configs:")
        for error in config_errors:
            print(f"- {error}")
        return 1

    print_created_tables()

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
