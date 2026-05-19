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


if __name__ == "__main__":
    raise SystemExit(main())
