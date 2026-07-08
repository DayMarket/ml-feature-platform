import base64
import binascii
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


NAME_LINE_PATTERN = re.compile(r"^-\s+name:\s+['\"]?(?P<name>[A-Za-z0-9_]+)['\"]?\s*$")
SCHEMA_LINE_PATTERN = re.compile(r"^schema:\s+['\"]?(?P<schema>[A-Za-z0-9_]+)['\"]?\s*$")
SOURCES_FILE_NAME = "sources.yaml"
TABLE_CONFIG_ROOTS = ("layers", "datasets")
CREATE_DBT_PR_FLAG = "create_dbt_pr"


@dataclass(frozen=True)
class RuntimeConfig:
    branch: str
    dbt_repo_url: str
    git_token: str
    workspace: Path
    dry_run: bool
    build_url: str


def main() -> int:
    repo_root = Path.cwd()
    _log_section("load config")
    print(f"repo_root={repo_root}")
    ci_config = json.loads((repo_root / "ci_config.yaml").read_text(encoding="utf-8"))
    dbt_config = ci_config["dbt"]
    table_configs = discover_table_configs(repo_root)
    runtime = RuntimeConfig(
        branch=os.getenv("DRONE_COMMIT_BRANCH", "local"),
        dbt_repo_url=_required_env(dbt_config.get("repo_url_env", "DBT_REPO_URL")),
        git_token=_required_env("GIT_TOKEN"),
        workspace=repo_root / ".tmp_dbt_repo",
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true",
        build_url=os.getenv("DRONE_BUILD_LINK", ""),
    )
    print(f"branch={runtime.branch}")
    print(f"dry_run={runtime.dry_run}")
    print(f"dbt_repo_url={_mask_token(runtime.dbt_repo_url)}")
    print(f"workspace={runtime.workspace}")
    print(f"models_path={dbt_config['models_path']}")
    print(f"discovered_tables={len(table_configs)}")
    for table_config in table_configs:
        print(
            "discovered_table="
            f"{table_config['catalog']}.{table_config['schema']}.{table_config['name']} "
            f"team={table_config['team']} primary_key={table_config['primary_key']} "
            f"create_dbt_pr={table_config['create_dbt_pr']} "
            f"config={table_config['config_path']}"
        )

    if runtime.workspace.exists():
        print(f"remove_existing_workspace={runtime.workspace}")
        shutil.rmtree(runtime.workspace)

    _log_section("clone dbt repo")
    clone_url = _with_token(runtime.dbt_repo_url, runtime.git_token)
    _run(["git", "clone", clone_url, runtime.workspace.as_posix()])

    _log_section("checkout automation branch")
    target_branch = _checkout_branch(runtime, dbt_config["base_branch"])
    print(f"target_branch={target_branch}")
    models_path = runtime.workspace / dbt_config["models_path"]
    models_path.mkdir(parents=True, exist_ok=True)
    print(f"models_path_exists={models_path.exists()} path={models_path}")

    _log_section("repair misplaced dbt source tables")
    desired_schemas = _desired_schemas_by_table(dbt_config, table_configs, runtime.branch)
    disabled_dbt_tables = {
        str(table_config["name"])
        for table_config in table_configs
        if not table_config["create_dbt_pr"]
    }
    created_files = _remove_misplaced_tables(
        models_path,
        desired_schemas,
        disabled_dbt_tables,
    )
    sources_path = models_path / SOURCES_FILE_NAME
    if sources_path.exists():
        content = sources_path.read_text(encoding="utf-8")
        repaired_content, descriptions_changed = _ensure_source_descriptions(content)
        if descriptions_changed:
            sources_path.write_text(repaired_content, encoding="utf-8")
            if sources_path not in created_files:
                created_files.append(sources_path)
            print(f"Updated dbt source descriptions: {sources_path}")
    source_schemas = (
        _extract_source_schemas(sources_path.read_text(encoding="utf-8"))
        if sources_path.exists()
        else set()
    )

    _log_section("scan existing dbt source tables")
    existing_tables = _existing_tables(models_path)
    print(f"existing_tables_count={len(existing_tables)}")
    print_table_list("existing_table", existing_tables)

    repo_slug = _github_slug(runtime.dbt_repo_url)
    pending_pr_tables = {}
    if repo_slug:
        _log_section("scan open dbt source PRs")
        pending_pr_tables = _open_pr_tables(runtime, repo_slug, dbt_config)
        print(f"open_pr_tables_count={len(pending_pr_tables)}")
        for pending_table, pr_url in sorted(pending_pr_tables.items()):
            print(f"open_pr_table={pending_table} pr={pr_url}")

    _log_section("render missing sources")
    for table_config in table_configs:
        table_name = str(table_config["name"])
        source_schema = _source_schema(dbt_config, table_config, runtime.branch)
        table_key = (source_schema, table_name)
        print(
            f"table={table_name} catalog={table_config['catalog']} "
            f"schema={source_schema} team={table_config['team']} "
            f"primary_key={table_config['primary_key']} sources_path={sources_path}"
        )
        if not table_config["create_dbt_pr"]:
            print(
                f"Skip dbt source table because table.meta.{CREATE_DBT_PR_FLAG}=false: "
                f"{source_schema}.{table_name}"
            )
            continue
        if table_key in existing_tables:
            print(f"Skip existing dbt source table: {source_schema}.{table_name}")
            continue
        if table_key in pending_pr_tables:
            print(
                f"Skip table already present in open dbt PR: "
                f"{source_schema}.{table_name} pr={pending_pr_tables[table_key]}"
            )
            continue

        source_yaml = render_source_yaml(
            dbt_config,
            table_config,
            runtime.branch,
            include_document_header=not sources_path.exists(),
            include_source_header=source_schema not in source_schemas,
        )
        if source_schema in source_schemas:
            _append_table_to_source_block(sources_path, source_schema, source_yaml)
        else:
            _append_to_sources_file(sources_path, source_yaml)
        if sources_path not in created_files:
            created_files.append(sources_path)
        source_schemas.add(source_schema)
        existing_tables.add(table_key)
        print(
            f"Added dbt source table: {source_schema}.{table_name} -> "
            f"{sources_path.relative_to(runtime.workspace)}"
        )
        print_primary_key_tests(table_config)
        print("rendered_yaml_begin")
        print(source_yaml.rstrip())
        print("rendered_yaml_end")

    if not created_files:
        _log_section("done")
        print("No dbt source changes to publish")
        return 0

    if runtime.dry_run:
        _log_section("dry run")
        _run(["git", "status", "--short"], cwd=runtime.workspace)
        print("DRY_RUN=true, skip commit/push/PR")
        return 0

    if _skip_dbt_pr_creation(runtime):
        _log_section("skip publish")
        _run(["git", "status", "--short"], cwd=runtime.workspace)
        print(f"branch={runtime.branch}, skip commit/push/PR for dbt sources")
        return 0

    _log_section("publish changes")
    _publish_changes(runtime, target_branch, dbt_config["base_branch"], created_files)
    return 0


def discover_table_configs(repo_root: Path) -> list[dict[str, Any]]:
    table_configs = []
    for config_root in TABLE_CONFIG_ROOTS:
        for config_path in sorted(repo_root.glob(f"{config_root}/**/config.yaml")):
            local_config = _read_simple_config(config_path)
            table = local_config.get("table", {})
            if not table:
                continue
            validate_table_config(table, config_path)

            primary_key = _parse_primary_key(str(table["primary_key"]))
            if not primary_key:
                raise ValueError(f"table.primary_key must not be empty in {config_path}")

            meta = table["meta"]
            table_configs.append(
                {
                    "catalog": table["catalog"],
                    "schema": table["schema"],
                    "name": table["name"],
                    "primary_key": primary_key,
                    "team": meta["team"],
                    "create_dbt_pr": _parse_bool_flag(
                        meta,
                        CREATE_DBT_PR_FLAG,
                        config_path,
                    ),
                    "config_path": config_path.relative_to(repo_root).as_posix(),
                }
            )
    return table_configs


def validate_table_config(table: dict[str, Any], config_path: Path) -> None:
    missing_fields = []
    for field_name in ("catalog", "schema", "name", "primary_key"):
        if not table.get(field_name):
            missing_fields.append(f"table.{field_name}")

    meta = table.get("meta")
    if not isinstance(meta, dict) or not meta.get("team"):
        missing_fields.append("table.meta.team")

    if missing_fields:
        raise ValueError(
            f"Missing required table config fields in {config_path}: "
            + ", ".join(missing_fields)
        )


def _parse_primary_key(primary_key: str) -> list[str]:
    return [column.strip() for column in primary_key.split(",") if column.strip()]


def _parse_bool_flag(
    config: dict[str, Any],
    field_name: str,
    config_path: Path,
    default: bool = True,
) -> bool:
    raw_value = config.get(field_name, default)
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        normalized = raw_value.strip().strip('"').strip("'").lower()
        if normalized in {"true", "yes", "1"}:
            return True
        if normalized in {"false", "no", "0"}:
            return False
    raise ValueError(
        f"{config_path}: table.meta.{field_name} must be a boolean value"
    )


def print_primary_key_tests(table_config: dict[str, Any]) -> None:
    print("primary_key_tests_begin")
    print(f"- table: dbt_utils.unique_combination_of_columns({table_config['primary_key']})")
    for column_name in table_config["primary_key"]:
        print(f"- column={column_name}: not_null")
    print("primary_key_tests_end")


def _read_simple_config(config_path: Path) -> dict[str, Any]:
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


def render_source_yaml(
    dbt_config: dict[str, Any],
    table_config: dict[str, Any],
    branch: str,
    include_document_header: bool = True,
    include_source_header: bool = True,
) -> str:
    source_schema = _source_schema(dbt_config, table_config, branch)
    source_name = f"ml_feature_platform_{source_schema}"
    database = dbt_config["database_mapping"][table_config["catalog"]]
    team = table_config["team"]

    lines = []
    if include_document_header:
        lines.extend(["version: 2", "", "sources:"])
    if include_source_header:
        lines.extend(
            [
                f"  - name: {source_name}",
                f'    description: "{_source_description(source_schema)}"',
                f"    database: {database}",
                f"    schema: {source_schema}",
                "    meta:",
                f'      owner: "{team}"',
                "    tables:",
            ]
        )

    has_date_column = _has_date_column(table_config)
    lines.extend(
        [
            f"      - name: {table_config['name']}",
            "        meta:",
            f'          owner: "{team}"',
        ]
    )
    if has_date_column:
        lines.extend(
            [
                "        loaded_at_field: \"CAST(date AS timestamp) + INTERVAL '1' DAY\"",
                "        freshness:",
                "          error_after:",
                "            count: 2",
                "            period: day",
            ]
        )

    lines.extend(
        [
            "        tests:",
            "          - dbt_utils.unique_combination_of_columns:",
            "              combination_of_columns:",
        ]
    )
    for column_name in table_config["primary_key"]:
        lines.append(f"                - {column_name}")
    if has_date_column:
        table_name = table_config["name"]
        lines.extend(
            [
                "          - row_count_greater_than_for_date:",
                f"              name: {table_name}_previous_day_has_rows",
                "              date_column: date",
                "              min_rows: 0",
                "          - row_count_growth_within_limit:",
                f"              name: {table_name}_previous_day_row_count_growth_within_20_percent",
                "              date_column: date",
                "              max_growth_ratio: 0.2",
            ]
        )
    lines.append("        columns:")
    for column_name in table_config["primary_key"]:
        lines.extend(
            [
                f"          - name: {column_name}",
                "            tests:",
                "              - not_null",
            ]
        )

    return "\n".join(lines) + "\n"


def _has_date_column(table_config: dict[str, Any]) -> bool:
    return "date" in table_config["primary_key"]


def _source_schema(
    dbt_config: dict[str, Any],
    table_config: dict[str, Any],
    branch: str,
) -> str:
    return str(dbt_config.get("schema_overrides", {}).get(branch, table_config["schema"]))


def _source_description(source_schema: str) -> str:
    layer_name = source_schema.replace("_", " ").title()
    return (
        f"{layer_name}-layer Iceberg tables produced by ml-feature-platform "
        "and consumed by ML feature pipelines."
    )


def _desired_schemas_by_table(
    dbt_config: dict[str, Any],
    table_configs: list[dict[str, Any]],
    branch: str,
) -> dict[str, str]:
    desired_schemas: dict[str, str] = {}
    for table_config in table_configs:
        table_name = str(table_config["name"])
        source_schema = _source_schema(dbt_config, table_config, branch)
        existing_schema = desired_schemas.get(table_name)
        if existing_schema and existing_schema != source_schema:
            raise ValueError(
                f"Table name {table_name} is declared in multiple schemas: "
                f"{existing_schema}, {source_schema}"
            )
        desired_schemas[table_name] = source_schema
    return desired_schemas


def _remove_misplaced_tables(
    models_path: Path,
    desired_schemas: dict[str, str],
    ignored_table_names: Optional[set[str]] = None,
) -> list[Path]:
    changed_files = []
    sources_path = models_path / SOURCES_FILE_NAME
    if not sources_path.exists():
        return changed_files
    content = sources_path.read_text(encoding="utf-8")
    repaired_content, removed_tables = _remove_misplaced_tables_from_content(
        content,
        desired_schemas,
        ignored_table_names,
    )
    if not removed_tables:
        return changed_files
    sources_path.write_text(repaired_content, encoding="utf-8")
    changed_files.append(sources_path)
    for source_schema, table_name, desired_schema in removed_tables:
        if desired_schema:
            print(
                f"Removed misplaced dbt source table: {source_schema}.{table_name} "
                f"expected_schema={desired_schema} file={sources_path}"
            )
        else:
            print(
                f"Removed stale dbt source table: {source_schema}.{table_name} "
                f"table is no longer declared in layers/**/config.yaml "
                f"or datasets/**/config.yaml file={sources_path}"
            )
    return changed_files


def _remove_misplaced_tables_from_content(
    content: str,
    desired_schemas: dict[str, str],
    ignored_table_names: Optional[set[str]] = None,
) -> tuple[str, list[tuple[str, str, Optional[str]]]]:
    lines = content.splitlines()
    kept_lines: list[str] = []
    removed_tables: list[tuple[str, str, Optional[str]]] = []
    ignored_table_names = ignored_table_names or set()
    current_schema = ""
    current_source_is_managed = False
    in_tables_block = False
    tables_indent = -1
    index = 0

    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))

        source_match = NAME_LINE_PATTERN.match(stripped)
        if indent == 2 and source_match:
            current_source_is_managed = source_match.group("name").startswith(
                "ml_feature_platform_"
            )
            current_schema = ""
            in_tables_block = False

        schema_match = SCHEMA_LINE_PATTERN.match(stripped)
        if schema_match:
            current_schema = schema_match.group("schema")
        if stripped == "tables:":
            in_tables_block = True
            tables_indent = indent
        elif in_tables_block and stripped and indent <= tables_indent:
            in_tables_block = False

        table_match = NAME_LINE_PATTERN.match(stripped)
        if (
            in_tables_block
            and indent == tables_indent + 2
            and table_match
            and current_schema
            and current_source_is_managed
        ):
            table_name = table_match.group("name")
            if table_name in ignored_table_names:
                kept_lines.append(line)
                index += 1
                continue
            desired_schema = desired_schemas.get(table_name)
            if desired_schema != current_schema:
                removed_tables.append((current_schema, table_name, desired_schema))
                index += 1
                while index < len(lines):
                    next_line = lines[index]
                    next_stripped = next_line.strip()
                    next_indent = len(next_line) - len(next_line.lstrip(" "))
                    if next_stripped and next_indent <= indent:
                        break
                    index += 1
                continue

        kept_lines.append(line)
        index += 1

    suffix = "\n" if content.endswith("\n") else ""
    return "\n".join(kept_lines).rstrip() + suffix, removed_tables


def _ensure_source_descriptions(content: str) -> tuple[str, bool]:
    lines = content.splitlines()
    updated_lines: list[str] = []
    changed = False
    index = 0

    while index < len(lines):
        line = lines[index]
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        source_match = NAME_LINE_PATTERN.match(stripped)
        if (
            indent == 2
            and source_match
            and source_match.group("name").startswith("ml_feature_platform_")
        ):
            source_schema = source_match.group("name").removeprefix(
                "ml_feature_platform_"
            )
            description_line = (
                f'    description: "{_source_description(source_schema)}"'
            )
            updated_lines.append(line)
            if index + 1 < len(lines) and lines[index + 1].strip().startswith(
                "description:"
            ):
                if lines[index + 1] != description_line:
                    changed = True
                updated_lines.append(description_line)
                index += 2
                continue
            updated_lines.append(description_line)
            changed = True
            index += 1
            continue

        updated_lines.append(line)
        index += 1

    suffix = "\n" if content.endswith("\n") else ""
    return "\n".join(updated_lines) + suffix, changed


def _existing_tables(models_path: Path) -> set[tuple[str, str]]:
    source_tables: set[tuple[str, str]] = set()
    sources_path = models_path / SOURCES_FILE_NAME
    if not sources_path.exists():
        print(f"No {SOURCES_FILE_NAME} file found under {models_path}")
        return source_tables
    print(f"scan_yaml={sources_path}")
    source_tables.update(_extract_source_tables(sources_path.read_text(encoding="utf-8")))
    return source_tables


def _extract_source_schemas(content: str) -> set[str]:
    schemas = set()
    for raw_line in content.splitlines():
        line = _strip_diff_prefix(raw_line.rstrip())
        schema_match = SCHEMA_LINE_PATTERN.match(line.strip())
        if schema_match:
            schemas.add(schema_match.group("schema"))
    return schemas


def _extract_source_tables(content: str) -> set[tuple[str, str]]:
    source_tables: set[tuple[str, str]] = set()
    in_tables_block = False
    tables_indent = -1
    current_schema = ""

    for raw_line in content.splitlines():
        line = _strip_diff_prefix(raw_line.rstrip())
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        schema_match = SCHEMA_LINE_PATTERN.match(stripped)
        if schema_match:
            current_schema = schema_match.group("schema")
            continue
        if stripped == "tables:":
            in_tables_block = True
            tables_indent = indent
            continue

        if in_tables_block and indent <= tables_indent:
            in_tables_block = False

        if not in_tables_block or indent != tables_indent + 2:
            continue

        match = NAME_LINE_PATTERN.match(stripped)
        if match and current_schema:
            source_tables.add((current_schema, match.group("name")))

    return source_tables


def _strip_diff_prefix(line: str) -> str:
    if line.startswith(("+", "-", " ")) and not line.startswith(("+++", "---")):
        return line[1:]
    return line


def print_table_list(prefix: str, source_tables: set[tuple[str, str]]) -> None:
    if not source_tables:
        print(f"{prefix}=none")
        return
    for source_schema, table_name in sorted(source_tables):
        print(f"{prefix}={source_schema}.{table_name}")


def _open_pr_tables(
    runtime: RuntimeConfig,
    repo_slug: str,
    dbt_config: dict[str, Any],
) -> dict[tuple[str, str], str]:
    result = _run(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            repo_slug,
            "--state",
            "open",
            "--limit",
            "100",
            "--json",
            "number,url,title,headRefName,headRefOid",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print("No open PR metadata returned")
        return {}

    try:
        pull_requests = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"Cannot parse gh pr list output: {exc}")
        return {}

    pending_tables: dict[tuple[str, str], str] = {}
    models_path = dbt_config["models_path"].rstrip("/")
    for pull_request in pull_requests:
        pr_number = str(pull_request["number"])
        pr_url = str(pull_request["url"])
        print(
            f"scan_open_pr=number:{pr_number} "
            f"head:{pull_request.get('headRefName')} title:{pull_request.get('title')}"
        )
        branch_tables = _open_pr_branch_tables(runtime, pr_number, models_path)
        if branch_tables:
            for table_key in branch_tables:
                pending_tables[table_key] = pr_url
            continue

        head_tables = _open_pr_head_tables(
            runtime,
            repo_slug,
            pr_number,
            str(pull_request.get("headRefOid") or ""),
            models_path,
        )
        if head_tables:
            for table_key in head_tables:
                pending_tables[table_key] = pr_url
            continue

        diff_result = _run(
            ["gh", "pr", "diff", pr_number, "--repo", repo_slug, "--patch"],
            env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
            check=False,
            capture_output=True,
        )
        if diff_result.returncode != 0:
            print(f"Cannot read diff for open PR #{pr_number}")
            continue

        for table_key in _extract_source_tables(diff_result.stdout or ""):
            pending_tables[table_key] = pr_url
    return pending_tables


def _open_pr_branch_tables(
    runtime: RuntimeConfig,
    pr_number: str,
    models_path: str,
) -> set[tuple[str, str]]:
    pr_ref = f"refs/remotes/origin/pr/{pr_number}"
    fetch_result = _run(
        ["git", "fetch", "origin", f"+pull/{pr_number}/head:{pr_ref}"],
        cwd=runtime.workspace,
        check=False,
        capture_output=True,
    )
    if fetch_result.returncode != 0:
        print(f"Cannot fetch head branch for open PR #{pr_number}, fallback to API")
        return set()

    tree_result = _run(
        ["git", "ls-tree", "-r", "--name-only", pr_ref, models_path],
        cwd=runtime.workspace,
        check=False,
        capture_output=True,
        log_output=False,
    )
    if tree_result.returncode != 0:
        print(f"Cannot list {models_path} from open PR #{pr_number}, fallback to API")
        return set()

    source_tables: set[tuple[str, str]] = set()
    for source_path in (tree_result.stdout or "").splitlines():
        if Path(source_path).name != SOURCES_FILE_NAME:
            continue
        show_result = _run(
            ["git", "show", f"{pr_ref}:{source_path}"],
            cwd=runtime.workspace,
            check=False,
            capture_output=True,
            log_output=False,
        )
        if show_result.returncode == 0:
            source_tables.update(_extract_source_tables(show_result.stdout or ""))
    print(f"open_pr_branch_tables_count={len(source_tables)} pr={pr_number}")
    return source_tables


def _open_pr_head_tables(
    runtime: RuntimeConfig,
    repo_slug: str,
    pr_number: str,
    head_ref_oid: str,
    models_path: str,
) -> set[tuple[str, str]]:
    if not head_ref_oid:
        print(f"No head sha for open PR #{pr_number}, fallback to diff")
        return set()

    source_path = f"{models_path.rstrip('/')}/{SOURCES_FILE_NAME}"
    api_result = _run(
        ["gh", "api", f"repos/{repo_slug}/contents/{source_path}?ref={head_ref_oid}"],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
        log_output=False,
    )
    if api_result.returncode != 0 or not api_result.stdout.strip():
        print(f"Cannot read {source_path} from open PR #{pr_number}, fallback to diff")
        return set()

    try:
        payload = json.loads(api_result.stdout)
    except json.JSONDecodeError as exc:
        print(f"Cannot parse {source_path} from open PR #{pr_number}: {exc}")
        return set()

    raw_content = str(payload.get("content") or "")
    if not raw_content:
        print(f"No {source_path} content returned for open PR #{pr_number}")
        return set()

    encoding = str(payload.get("encoding") or "")
    if encoding == "base64":
        try:
            content = base64.b64decode(raw_content).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            print(f"Cannot decode {source_path} from open PR #{pr_number}: {exc}")
            return set()
    else:
        content = raw_content

    source_tables = _extract_source_tables(content)
    print(f"open_pr_head_tables_count={len(source_tables)} pr={pr_number}")
    return source_tables


def _append_to_sources_file(sources_path: Path, source_yaml: str) -> None:
    if not sources_path.exists() or sources_path.stat().st_size == 0:
        sources_path.write_text(source_yaml, encoding="utf-8")
        return

    existing_content = sources_path.read_text(encoding="utf-8")
    sources_path.write_text(
        existing_content.rstrip() + "\n\n" + source_yaml,
        encoding="utf-8",
    )


def _append_table_to_source_block(
    sources_path: Path,
    source_schema: str,
    table_yaml: str,
) -> None:
    content = sources_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    source_name = f"ml_feature_platform_{source_schema}"
    source_start = None
    insert_at = len(lines)

    for index, line in enumerate(lines):
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        match = NAME_LINE_PATTERN.match(stripped)
        if indent != 2 or not match:
            continue
        if match.group("name") == source_name:
            source_start = index
            continue
        if source_start is not None:
            insert_at = index
            break

    if source_start is None:
        raise ValueError(
            f"Cannot find dbt source block {source_name} in {sources_path}"
        )

    table_lines = table_yaml.rstrip().splitlines()
    prefix = lines[:insert_at]
    suffix = lines[insert_at:]
    while prefix and not prefix[-1].strip():
        prefix.pop()
    updated_lines = prefix + table_lines
    if suffix:
        updated_lines.extend([""] + suffix)
    sources_path.write_text("\n".join(updated_lines).rstrip() + "\n", encoding="utf-8")


def _checkout_branch(runtime: RuntimeConfig, base_branch: str) -> str:
    branch_name = f"automation/ml-feature-platform-sources-{runtime.branch}-{_branch_suffix()}"
    _run(["git", "checkout", base_branch], cwd=runtime.workspace)
    _run(["git", "checkout", "-B", branch_name], cwd=runtime.workspace)
    return branch_name


def _branch_suffix() -> str:
    commit_sha = os.getenv("DRONE_COMMIT_SHA", "")
    if commit_sha:
        return commit_sha[:8]

    build_number = os.getenv("DRONE_BUILD_NUMBER", "")
    if build_number:
        return f"build-{build_number}"

    return "local"


def _skip_dbt_pr_creation(runtime: RuntimeConfig) -> bool:
    return runtime.branch == "dev"


def _publish_changes(
    runtime: RuntimeConfig,
    branch_name: str,
    base_branch: str,
    created_files: list[Path],
) -> None:
    print("created_files=" + ", ".join(path.relative_to(runtime.workspace).as_posix() for path in created_files))
    _run(["git", "config", "user.email", "ci@ml-feature-platform.local"], cwd=runtime.workspace)
    _run(["git", "config", "user.name", "ml-feature-platform-ci"], cwd=runtime.workspace)
    _run(["git", "add", *[path.relative_to(runtime.workspace).as_posix() for path in created_files]], cwd=runtime.workspace)
    _run(["git", "status", "--short"], cwd=runtime.workspace)
    _run(["git", "commit", "-m", "Add ml-feature-platform dbt sources"], cwd=runtime.workspace)
    _run(["git", "push", "--force", "origin", branch_name], cwd=runtime.workspace)

    repo_slug = _github_slug(runtime.dbt_repo_url)
    if repo_slug:
        pr_result = _run(
            [
                "gh",
                "pr",
                "create",
                "--repo",
                repo_slug,
                "--base",
                base_branch,
                "--head",
                branch_name,
                "--title",
                "Add ml-feature-platform dbt sources",
                "--body",
                _pr_body(runtime),
            ],
            cwd=runtime.workspace,
            env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
            check=False,
            capture_output=True,
        )
        pr_url = (pr_result.stdout or "").strip()
        if pr_result.returncode == 0 and pr_url:
            print(f"created_pr_url={pr_url}")
            _write_created_pr_url(runtime, pr_url)
            _comment_source_pr_if_possible(runtime, pr_url)
        else:
            print(f"gh pr create finished with code={pr_result.returncode}")
            if pr_result.stdout:
                print(pr_result.stdout.strip())
            if pr_result.stderr:
                print(pr_result.stderr.strip())
    else:
        print("Cannot derive GitHub repo slug from DBT_REPO_URL, skip PR creation")


def _pr_body(runtime: RuntimeConfig) -> str:
    lines = ["Generated by ml-feature-platform CI."]
    if runtime.build_url:
        lines.append("")
        lines.append(f"Drone build: {runtime.build_url}")
    return "\n".join(lines)


def _write_created_pr_url(runtime: RuntimeConfig, pr_url: str) -> None:
    output_path = Path.cwd() / "created_dbt_pr_url.txt"
    output_path.write_text(pr_url + "\n", encoding="utf-8")
    print(f"created_pr_url_file={output_path}")


def _comment_source_pr_if_possible(runtime: RuntimeConfig, pr_url: str) -> None:
    source_pr = os.getenv("DRONE_PULL_REQUEST")
    source_repo = os.getenv("DRONE_REPO")
    if not source_repo:
        print("No source repo context in Drone, skip source PR comment")
        return

    if not source_pr:
        source_pr = _find_merged_source_pr(runtime, source_repo)

    if not source_pr:
        print("No source PR number found, skip source PR comment")
        return

    print(f"Comment source PR #{source_pr} in {source_repo}")
    _run(
        [
            "gh",
            "pr",
            "comment",
            source_pr,
            "--repo",
            source_repo,
            "--body",
            f"Created dbt source PR: {pr_url}",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
    )


def _find_merged_source_pr(runtime: RuntimeConfig, source_repo: str) -> str:
    commit_sha = os.getenv("DRONE_COMMIT_SHA")
    if not commit_sha:
        print("No DRONE_COMMIT_SHA, cannot search merged source PR")
        return ""

    print(f"Search merged source PR by commit sha={commit_sha}")
    result = _run(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            source_repo,
            "--state",
            "merged",
            "--search",
            commit_sha,
            "--json",
            "number",
            "--jq",
            ".[0].number",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
    )
    source_pr = (result.stdout or "").strip()
    if source_pr:
        print(f"found_source_pr={source_pr}")
    return source_pr


def _with_token(repo_url: str, token: str) -> str:
    repo_url = _normalize_repo_url(repo_url)
    if repo_url.startswith("https://"):
        return repo_url.replace("https://", f"https://{token}@", 1)
    return repo_url


def _normalize_repo_url(repo_url: str) -> str:
    repo_url = repo_url.strip()
    if repo_url.startswith("github.com/"):
        repo_url = f"https://{repo_url}"
    if repo_url.startswith("https://github.com/") and not repo_url.endswith(".git"):
        repo_url = f"{repo_url}.git"
    return repo_url


def _github_slug(repo_url: str) -> Optional[str]:
    repo_url = _normalize_repo_url(repo_url)
    match = re.search(r"github\.com[:/](?P<slug>[^/]+/[^/.]+)(?:\.git)?$", repo_url)
    if not match:
        return None
    return match.group("slug")


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is required")
    return value


def _run(
    command: list[str],
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    check: bool = True,
    capture_output: bool = False,
    log_output: bool = True,
) -> subprocess.CompletedProcess:
    printable = " ".join(_mask_token(part) for part in command)
    cwd_text = f" cwd={cwd}" if cwd else ""
    print(f"+{cwd_text} {printable}")
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        capture_output=True,
    )
    if log_output and result.stdout:
        print(_mask_token(result.stdout.rstrip()))
    if log_output and result.stderr:
        print(_mask_token(result.stderr.rstrip()))
    if check and result.returncode != 0:
        print(f"command_failed_returncode={result.returncode}")
        print_clone_failure_hint(command)
        raise subprocess.CalledProcessError(
            result.returncode,
            [_mask_token(part) for part in command],
            output=_mask_token(result.stdout or ""),
            stderr=_mask_token(result.stderr or ""),
        )
    return result


def print_clone_failure_hint(command: list[str]) -> None:
    if len(command) >= 2 and command[0] == "git" and command[1] in {"clone", "ls-remote"}:
        print("git_auth_hint=Check DBT_REPO_URL, GIT_TOKEN repo access, org SSO approval, and contents read permission.")


def _mask_token(value: str) -> str:
    value = re.sub(r"x-access-token:[^@]+@", "x-access-token:***@", value)
    value = re.sub(r"https://github_pat_[^@]+@", "https://github_pat_***@", value)
    value = re.sub(r"https://ghp_[^@]+@", "https://ghp_***@", value)
    return value


def _log_section(title: str) -> None:
    print("")
    print(f"=== {title} ===")


if __name__ == "__main__":
    raise SystemExit(main())
