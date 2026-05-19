import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


NAME_LINE_PATTERN = re.compile(r"^-\s+name:\s+['\"]?(?P<name>[A-Za-z0-9_]+)['\"]?\s*$")
SOURCES_FILE_NAME = "sources.yaml"


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
    sources_path = models_path / SOURCES_FILE_NAME
    print(f"sources_path={sources_path}")

    _log_section("scan existing dbt source tables")
    existing_tables = _existing_tables(sources_path)
    print(f"sources_file_exists={sources_path.exists()}")
    print(f"existing_tables_count={len(existing_tables)}")
    print_table_list("existing_table", existing_tables)

    repo_slug = _github_slug(runtime.dbt_repo_url)
    pending_pr_tables = {}
    if repo_slug:
        _log_section("scan open dbt source PRs")
        pending_pr_tables = _open_pr_tables(runtime, repo_slug)
        print(f"open_pr_tables_count={len(pending_pr_tables)}")
        for pending_table, pr_url in sorted(pending_pr_tables.items()):
            print(f"open_pr_table={pending_table} pr={pr_url}")

    created_files = []
    _log_section("render missing sources")
    for table_config in table_configs:
        table_name = str(table_config["name"])
        source_schema = _source_schema(dbt_config, table_config, runtime.branch)
        print(
            f"table={table_name} catalog={table_config['catalog']} "
            f"schema={source_schema} team={table_config['team']} "
            f"primary_key={table_config['primary_key']}"
        )
        if table_name in existing_tables:
            print(f"Skip existing dbt source table: {table_name}")
            continue
        if table_name in pending_pr_tables:
            print(
                f"Skip table already present in open dbt PR: "
                f"{table_name} pr={pending_pr_tables[table_name]}"
            )
            continue

        source_yaml = render_source_yaml(
            dbt_config,
            table_config,
            runtime.branch,
            include_header=not sources_path.exists() and not created_files,
        )
        _append_to_sources_file(sources_path, source_yaml)
        if sources_path not in created_files:
            created_files.append(sources_path)
        existing_tables.add(table_name)
        print(f"Added dbt source table: {table_name} -> {sources_path.relative_to(runtime.workspace)}")
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

    _log_section("publish changes")
    _publish_changes(runtime, target_branch, dbt_config["base_branch"], created_files)
    return 0


def discover_table_configs(repo_root: Path) -> list[dict[str, Any]]:
    table_configs = []
    for config_path in sorted(repo_root.glob("layers/**/config.yaml")):
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
    include_header: bool = True,
) -> str:
    source_schema = _source_schema(dbt_config, table_config, branch)
    source_name = f"ml_feature_platform_{source_schema}"
    database = dbt_config["database_mapping"][table_config["catalog"]]
    team = table_config["team"]

    if include_header:
        lines = [
            "version: 2",
            "",
            "sources:",
            f"  - name: {source_name}",
            f"    database: {database}",
            f"    schema: {source_schema}",
            "    meta:",
            f'      owner: "{team}"',
            "    tables:",
        ]
    else:
        lines = []

    lines.extend(
        [
            f"      - name: {table_config['name']}",
            "        meta:",
            f'          owner: "{team}"',
            "        tests:",
            "          - dbt_utils.unique_combination_of_columns:",
            "              combination_of_columns:",
        ]
    )
    for column_name in table_config["primary_key"]:
        lines.append(f"                - {column_name}")
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


def _source_schema(
    dbt_config: dict[str, Any],
    table_config: dict[str, Any],
    branch: str,
) -> str:
    return str(dbt_config.get("schema_overrides", {}).get(branch, table_config["schema"]))


def _existing_tables(sources_path: Path) -> set[str]:
    if not sources_path.exists():
        print("sources.yaml does not exist yet")
        return set()
    print(f"scan_yaml={sources_path}")
    content = sources_path.read_text(encoding="utf-8")
    return _extract_source_table_names(content)


def _extract_source_table_names(content: str) -> set[str]:
    table_names = set()
    in_tables_block = False
    tables_indent = -1

    for raw_line in content.splitlines():
        line = _strip_diff_prefix(raw_line.rstrip())
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        if stripped == "tables:":
            in_tables_block = True
            tables_indent = indent
            continue

        if in_tables_block and indent <= tables_indent:
            in_tables_block = False

        if not in_tables_block or indent != tables_indent + 2:
            continue

        match = NAME_LINE_PATTERN.match(stripped)
        if match:
            table_names.add(match.group("name"))

    return table_names


def _strip_diff_prefix(line: str) -> str:
    if line.startswith(("+", "-", " ")) and not line.startswith(("+++", "---")):
        return line[1:]
    return line


def print_table_list(prefix: str, table_names: set[str]) -> None:
    if not table_names:
        print(f"{prefix}=none")
        return
    for table_name in sorted(table_names):
        print(f"{prefix}={table_name}")


def _open_pr_tables(runtime: RuntimeConfig, repo_slug: str) -> dict[str, str]:
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
            "number,url,title,headRefName",
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

    pending_tables = {}
    for pull_request in pull_requests:
        pr_number = str(pull_request["number"])
        pr_url = str(pull_request["url"])
        print(
            f"scan_open_pr=number:{pr_number} "
            f"head:{pull_request.get('headRefName')} title:{pull_request.get('title')}"
        )
        diff_result = _run(
            ["gh", "pr", "diff", pr_number, "--repo", repo_slug, "--patch"],
            env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
            check=False,
            capture_output=True,
        )
        if diff_result.returncode != 0:
            print(f"Cannot read diff for open PR #{pr_number}")
            continue

        for table_name in _extract_source_table_names(diff_result.stdout or ""):
            pending_tables[table_name] = pr_url
    return pending_tables


def _append_to_sources_file(sources_path: Path, source_yaml: str) -> None:
    if not sources_path.exists() or sources_path.stat().st_size == 0:
        sources_path.write_text(source_yaml, encoding="utf-8")
        return

    existing_content = sources_path.read_text(encoding="utf-8")
    sources_path.write_text(
        existing_content.rstrip() + "\n\n" + source_yaml,
        encoding="utf-8",
    )


def _checkout_branch(runtime: RuntimeConfig, base_branch: str) -> str:
    branch_name = f"automation/ml-feature-platform-sources-{runtime.branch}"
    _run(["git", "checkout", base_branch], cwd=runtime.workspace)
    _run(["git", "checkout", "-B", branch_name], cwd=runtime.workspace)
    return branch_name


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
    if result.stdout:
        print(_mask_token(result.stdout.rstrip()))
    if result.stderr:
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
