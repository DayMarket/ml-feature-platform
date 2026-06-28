import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


MAINTENANCE_REPO_DEFAULT_URL = "https://github.com/DayMarket/pyspark-etl.git"
MAINTENANCE_CONFIG_PATH = Path(
    "dags_v3/maintenance_generator/feature_platform_config.yaml"
)
MAINTENANCE_DAG_PATH = Path("dags_v3/maintenance_generator/dag.py")
TABLE_CONFIG_ROOTS = ("layers", "datasets")


@dataclass(frozen=True)
class RuntimeConfig:
    branch: str
    repo_url: str
    git_token: str
    workspace: Path
    dry_run: bool
    build_url: str


def main() -> int:
    repo_root = Path.cwd()
    runtime = RuntimeConfig(
        branch=os.getenv("DRONE_COMMIT_BRANCH", "local"),
        repo_url=os.getenv("MAINTENANCE_REPO_URL", MAINTENANCE_REPO_DEFAULT_URL),
        git_token=_required_env("GIT_TOKEN"),
        workspace=repo_root / ".tmp_maintenance_repo",
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true",
        build_url=os.getenv("DRONE_BUILD_LINK", ""),
    )
    table_config = discover_iceberg_tables(repo_root)
    print(f"branch={runtime.branch}")
    print(f"dry_run={runtime.dry_run}")
    print(f"maintenance_repo={_mask_token(runtime.repo_url)}")
    print(f"discovered_maintenance_tables={sum(len(tables) for tables in table_config.values())}")
    for schema, tables in sorted(table_config.items()):
        print(f"discovered_schema={schema} tables={len(tables)}")
        for table in tables:
            print(f"discovered_table={schema}.{table}")

    if runtime.workspace.exists():
        shutil.rmtree(runtime.workspace)

    _run(["git", "clone", _with_token(runtime.repo_url, runtime.git_token), runtime.workspace.as_posix()])
    base_branch = _base_branch(runtime.branch)
    target_branch = _checkout_branch(runtime, base_branch)

    repo_slug = github_slug(runtime.repo_url)
    pending_pr_tables: dict[tuple[str, str], str] = {}
    if repo_slug:
        pending_pr_tables = open_pr_tables(runtime, repo_slug)
        print(f"open_pr_tables_count={len(pending_pr_tables)}")
        for pending_table, pr_url in sorted(pending_pr_tables.items()):
            print(f"open_pr_table={pending_table[0]}.{pending_table[1]} pr={pr_url}")

    changed_files = sync_maintenance_files(
        runtime.workspace,
        table_config,
        pending_pr_tables,
    )
    if not changed_files:
        print("No Iceberg maintenance changes to publish")
        return 0

    if runtime.dry_run:
        _run(["git", "status", "--short"], cwd=runtime.workspace)
        print("DRY_RUN=true, skip commit/push/PR")
        return 0

    publish_changes(runtime, target_branch, base_branch, changed_files)
    return 0


def discover_iceberg_tables(repo_root: Path) -> dict[str, list[str]]:
    schemas: dict[str, set[str]] = {}
    for config_root in TABLE_CONFIG_ROOTS:
        for config_path in sorted(repo_root.glob(f"{config_root}/**/config.yaml")):
            config = read_simple_nested_config(config_path)
            table = config.get("table", {})
            if table.get("catalog") != "iceberg":
                continue
            schema = str(table.get("schema", "")).strip()
            table_name = str(table.get("name", "")).strip()
            if not schema or not table_name:
                raise ValueError(f"{config_path}: table.schema and table.name are required")
            schemas.setdefault(schema, set()).add(table_name)
    return {
        schema: sorted(tables)
        for schema, tables in sorted(schemas.items())
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


def sync_maintenance_files(
    maintenance_repo: Path,
    discovered_tables: dict[str, list[str]],
    pending_pr_tables: Optional[dict[tuple[str, str], str]] = None,
) -> list[Path]:
    changed_files: list[Path] = []
    pending_pr_tables = pending_pr_tables or {}

    config_path = maintenance_repo / MAINTENANCE_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing_tables = (
        parse_schema_table_config(config_path.read_text(encoding="utf-8"))
        if config_path.exists()
        else {}
    )
    missing_tables = missing_schema_tables(
        discovered_tables,
        existing_tables,
        pending_pr_tables,
    )
    if not config_path.exists() and not any(missing_tables.values()):
        print("No missing maintenance tables to add")
        return changed_files

    merged_tables = merge_schema_tables(existing_tables, missing_tables)
    rendered_config = render_feature_platform_config(merged_tables)
    if not config_path.exists() or config_path.read_text(encoding="utf-8") != rendered_config:
        config_path.write_text(rendered_config, encoding="utf-8")
        changed_files.append(config_path)
        print(f"Updated maintenance config: {config_path}")

    dag_path = maintenance_repo / MAINTENANCE_DAG_PATH
    dag_content = dag_path.read_text(encoding="utf-8")
    updated_dag = ensure_feature_platform_dag(dag_content)
    if updated_dag != dag_content:
        dag_path.write_text(updated_dag, encoding="utf-8")
        changed_files.append(dag_path)
        print(f"Updated maintenance DAG: {dag_path}")

    return changed_files


def missing_schema_tables(
    discovered_tables: dict[str, list[str]],
    existing_tables: dict[str, list[str]],
    pending_pr_tables: dict[tuple[str, str], str],
) -> dict[str, list[str]]:
    existing_keys = {
        (schema, table)
        for schema, tables in existing_tables.items()
        for table in tables
    }
    missing: dict[str, list[str]] = {}
    for schema, tables in sorted(discovered_tables.items()):
        for table in sorted(dict.fromkeys(tables)):
            table_key = (schema, table)
            if table_key in existing_keys:
                print(f"Skip existing maintenance table: {schema}.{table}")
                continue
            if table_key in pending_pr_tables:
                print(
                    f"Skip table already present in open maintenance PR: "
                    f"{schema}.{table} pr={pending_pr_tables[table_key]}"
                )
                continue
            missing.setdefault(schema, []).append(table)
    return missing


def merge_schema_tables(
    existing_tables: dict[str, list[str]],
    discovered_tables: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = {
        schema: set(tables)
        for schema, tables in existing_tables.items()
    }
    for schema, tables in discovered_tables.items():
        merged.setdefault(schema, set()).update(tables)
    return {
        schema: sorted(tables)
        for schema, tables in sorted(merged.items())
        if tables
    }


def parse_schema_table_config(content: str) -> dict[str, list[str]]:
    schemas: dict[str, list[str]] = {}
    in_schemas = False
    current_schema: Optional[str] = None

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped == "schemas:":
            in_schemas = True
            current_schema = None
            continue
        if not in_schemas:
            continue

        indent = len(line) - len(line.lstrip(" "))
        if indent == 2 and stripped.endswith(":"):
            current_schema = stripped[:-1].strip()
            schemas.setdefault(current_schema, [])
            continue
        if indent == 4 and stripped.startswith("- ") and current_schema:
            schemas.setdefault(current_schema, []).append(stripped[2:].strip())

    return schemas


def parse_schema_table_keys(
    content: str,
    strip_diff: bool = False,
) -> set[tuple[str, str]]:
    if strip_diff:
        content = "\n".join(strip_diff_prefix(line) for line in content.splitlines())
    return {
        (schema, table)
        for schema, tables in parse_schema_table_config(content).items()
        for table in tables
    }


def strip_diff_prefix(line: str) -> str:
    if line.startswith(("+", "-", " ")) and not line.startswith(("+++", "---")):
        return line[1:]
    return line


def render_feature_platform_config(schema_tables: dict[str, list[str]]) -> str:
    lines = [
        "# Auto-generated by ml-feature-platform CI.",
        "# Manual additions are preserved by sync; removals require manual review.",
        "schemas:",
    ]
    for schema, tables in sorted(schema_tables.items()):
        lines.append(f"  {schema}:")
        for table in sorted(dict.fromkeys(tables)):
            lines.append(f"    - {table}")
    return "\n".join(lines) + "\n"


def ensure_feature_platform_dag(content: str) -> str:
    updated = ensure_feature_platform_loader(content)
    updated = ensure_feature_platform_loader_mapping(updated)
    updated = ensure_feature_platform_create_dag(updated)
    return updated


def ensure_feature_platform_loader(content: str) -> str:
    if "def feature_platform_config()" in content:
        return content

    loader = '''

def feature_platform_config() -> dict:
    """Read maintenance table list generated by ml-feature-platform."""
    config_path = Path(DAG_DIR) / "feature_platform_config.yaml"
    if not config_path.exists():
        logging.warning("Feature platform maintenance config not found: %s", config_path)
        return {"schemas": {}}
    try:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        if not data or "schemas" not in data:
            return {"schemas": {}}
        return data
    except Exception as e:
        logging.error("Error reading feature platform config: %s", e)
        return {"schemas": {}}
'''
    match = re.search(r"\ndef dpa_streamer_config\(\) -> dict:", content)
    if not match:
        raise ValueError("Cannot find dpa_streamer_config marker in maintenance dag.py")
    return content[:match.start()] + loader + content[match.start():]


def ensure_feature_platform_loader_mapping(content: str) -> str:
    if '"feature_platform": feature_platform_config,' in content:
        return content
    marker = '    "dpa": dpa_streamer_config,\n'
    if marker not in content:
        raise ValueError("Cannot find _CONFIG_LOADERS dpa marker in maintenance dag.py")
    return content.replace(marker, '    "feature_platform": feature_platform_config,\n' + marker, 1)


def ensure_feature_platform_create_dag(content: str) -> str:
    create_call = 'create_dag(config_name="feature_platform", dag_suffix="_fp")'
    if create_call in content:
        return content

    marker = 'create_dag(config_name="dpa",     dag_suffix="_dpa")'
    if marker not in content:
        raise ValueError("Cannot find dpa create_dag marker in maintenance dag.py")
    return content.replace(marker, marker + "\n" + create_call, 1)


def publish_changes(
    runtime: RuntimeConfig,
    branch_name: str,
    base_branch: str,
    changed_files: list[Path],
) -> None:
    _run(["git", "config", "user.email", "ci@ml-feature-platform.local"], cwd=runtime.workspace)
    _run(["git", "config", "user.name", "ml-feature-platform-ci"], cwd=runtime.workspace)
    _run(
        ["git", "add", *[path.relative_to(runtime.workspace).as_posix() for path in changed_files]],
        cwd=runtime.workspace,
    )
    _run(["git", "status", "--short"], cwd=runtime.workspace)
    _run(["git", "commit", "-m", "Add ml-feature-platform tables to Iceberg maintenance"], cwd=runtime.workspace)
    _run(["git", "push", "--force", "origin", branch_name], cwd=runtime.workspace)

    repo_slug = github_slug(runtime.repo_url)
    if not repo_slug:
        print("Cannot derive GitHub repo slug from MAINTENANCE_REPO_URL, skip PR creation")
        return

    pr_url = find_open_pr_url(runtime, repo_slug, branch_name, base_branch)
    if not pr_url:
        pr_url = create_pull_request(runtime, repo_slug, branch_name, base_branch)
    if pr_url:
        write_created_pr_url(pr_url)
        comment_source_pr_if_possible(runtime, pr_url)


def create_pull_request(
    runtime: RuntimeConfig,
    repo_slug: str,
    branch_name: str,
    base_branch: str,
) -> str:
    result = _run(
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
            "Add ml-feature-platform tables to Iceberg maintenance",
            "--body",
            pr_body(runtime),
        ],
        cwd=runtime.workspace,
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
    )
    pr_url = (result.stdout or "").strip()
    if result.returncode == 0 and pr_url:
        print(f"created_maintenance_pr_url={pr_url}")
        return pr_url

    print(f"gh pr create finished with code={result.returncode}")
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return ""


def find_open_pr_url(
    runtime: RuntimeConfig,
    repo_slug: str,
    branch_name: str,
    base_branch: str,
) -> str:
    result = _run(
        [
            "gh",
            "pr",
            "list",
            "--repo",
            repo_slug,
            "--state",
            "open",
            "--head",
            branch_name,
            "--base",
            base_branch,
            "--json",
            "url",
            "--limit",
            "1",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    try:
        pull_requests = json.loads(result.stdout)
    except json.JSONDecodeError:
        return ""
    if not pull_requests:
        return ""
    pr_url = str(pull_requests[0].get("url", ""))
    if pr_url:
        print(f"reuse_open_maintenance_pr_url={pr_url}")
    return pr_url


def open_pr_tables(
    runtime: RuntimeConfig,
    repo_slug: str,
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

    pending_tables: dict[tuple[str, str], str] = {}
    for pull_request in pull_requests:
        pr_number = str(pull_request["number"])
        pr_url = str(pull_request["url"])
        print(
            f"scan_open_pr=number:{pr_number} "
            f"head:{pull_request.get('headRefName')} title:{pull_request.get('title')}"
        )
        branch_tables = open_pr_branch_tables(runtime, pr_number)
        if branch_tables:
            for table_key in branch_tables:
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

        diff_tables = parse_schema_table_keys(
            diff_result.stdout or "",
            strip_diff=True,
        )
        for table_key in diff_tables:
            pending_tables[table_key] = pr_url
    return pending_tables


def open_pr_branch_tables(
    runtime: RuntimeConfig,
    pr_number: str,
) -> set[tuple[str, str]]:
    pr_ref = f"refs/remotes/origin/pr/{pr_number}"
    fetch_result = _run(
        ["git", "fetch", "origin", f"+pull/{pr_number}/head:{pr_ref}"],
        cwd=runtime.workspace,
        check=False,
        capture_output=True,
        log_output=False,
    )
    if fetch_result.returncode != 0:
        print(f"Cannot fetch head branch for open PR #{pr_number}, fallback to diff")
        return set()

    show_result = _run(
        ["git", "show", f"{pr_ref}:{MAINTENANCE_CONFIG_PATH.as_posix()}"],
        cwd=runtime.workspace,
        check=False,
        capture_output=True,
        log_output=False,
    )
    if show_result.returncode != 0:
        print(f"Cannot read maintenance config from open PR #{pr_number}, fallback to diff")
        return set()

    source_tables = parse_schema_table_keys(show_result.stdout or "")
    print(f"open_pr_branch_tables_count={len(source_tables)} pr={pr_number}")
    return source_tables


def pr_body(runtime: RuntimeConfig) -> str:
    lines = [
        "Generated by ml-feature-platform CI.",
        "",
        "This PR adds repository-managed ml-feature-platform Iceberg tables to the dedicated maintenance DAG:",
        "`spark.iceberg_maintenance_fp`.",
    ]
    if runtime.build_url:
        lines.extend(["", f"Drone build: {runtime.build_url}"])
    return "\n".join(lines)


def write_created_pr_url(pr_url: str) -> None:
    output_path = Path.cwd() / "created_maintenance_pr_url.txt"
    output_path.write_text(pr_url + "\n", encoding="utf-8")
    print(f"created_maintenance_pr_url_file={output_path}")


def comment_source_pr_if_possible(runtime: RuntimeConfig, pr_url: str) -> None:
    source_pr = os.getenv("DRONE_PULL_REQUEST")
    source_repo = os.getenv("DRONE_REPO")
    if not source_repo:
        print("No source repo context in Drone, skip source PR comment")
        return

    if not source_pr:
        source_pr = find_merged_source_pr(runtime, source_repo)
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
            f"Created Iceberg maintenance PR: {pr_url}",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
    )


def find_merged_source_pr(runtime: RuntimeConfig, source_repo: str) -> str:
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
            "--limit",
            "1",
        ],
        env={**os.environ, "GITHUB_TOKEN": runtime.git_token},
        check=False,
        capture_output=True,
    )
    source_pr = (result.stdout or "").strip()
    if source_pr:
        print(f"found_source_pr={source_pr}")
    return source_pr


def _checkout_branch(runtime: RuntimeConfig, base_branch: str) -> str:
    branch_name = f"automation/ml-feature-platform-maintenance-{runtime.branch}-{branch_suffix()}"
    _run(["git", "checkout", base_branch], cwd=runtime.workspace)
    _run(["git", "checkout", "-B", branch_name], cwd=runtime.workspace)
    return branch_name


def _base_branch(source_branch: str) -> str:
    if source_branch in {"dev", "master"}:
        return source_branch
    return "master"


def branch_suffix() -> str:
    commit_sha = os.getenv("DRONE_COMMIT_SHA", "")
    if commit_sha:
        return commit_sha[:8]
    build_number = os.getenv("DRONE_BUILD_NUMBER", "")
    if build_number:
        return f"build-{build_number}"
    return "local"


def _with_token(repo_url: str, token: str) -> str:
    if repo_url.startswith("https://"):
        return repo_url.replace("https://", f"https://{token}@", 1)
    return repo_url


def github_slug(repo_url: str) -> str:
    cleaned = repo_url.removesuffix(".git").rstrip("/")
    match = re.search(r"github\.com[:/](?P<slug>[^/]+/[^/]+)$", cleaned)
    if not match:
        return ""
    return match.group("slug")


def _mask_token(value: str) -> str:
    token = os.getenv("GIT_TOKEN")
    if token:
        return value.replace(token, "***")
    return value


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
) -> subprocess.CompletedProcess[str]:
    print("$ " + " ".join(command))
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        capture_output=capture_output,
    )
    if log_output and result.stdout:
        print(result.stdout, end="")
    if log_output and result.stderr:
        print(result.stderr, end="")
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


if __name__ == "__main__":
    raise SystemExit(main())
