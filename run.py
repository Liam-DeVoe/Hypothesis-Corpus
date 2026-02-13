"""
Unified CLI for PBT corpus analysis system.
"""

import json
import logging
import subprocess
import sys
import traceback
from collections import defaultdict

import click
from rich.console import Console

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="",
    handlers=[logging.StreamHandler()],
    force=True,
)

# Ensure all loggers use the same format
for name in ["analysis.test_runner", "analysis.worker", "analysis.database"]:
    logging.getLogger(name).handlers = []
    logging.getLogger(name).propagate = True

logger = logging.getLogger(__name__)
console = Console()


@click.group()
def cli():
    pass


# ==============================================================================
# COLLECT COMMAND
# ==============================================================================


@cli.command()
@click.option("--db-path", help="Path to database file", default="analysis/data.db")
def collect(db_path: str):
    """Collect repositories from GitHub and store in database."""
    from analysis.collect.run import run_collection

    run_collection(db_path)


# ==============================================================================
# EXPERIMENT COMMAND
# ==============================================================================


@cli.command()
@click.option("--db-path", help="Path to database file", default="analysis/data.db")
@click.option("--workers", "-w", type=int, default=4, help="Number of worker processes")
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option(
    "--repo",
    "repo_name",
    help="Run experiment on specific repository (e.g., owner/repo)",
)
@click.option(
    "--docker-image", default="pbt-analysis:latest", help="Docker image to use"
)
@click.option(
    "--experiments", "-e", multiple=True, help="Experiments to run (default: all)"
)
@click.option("--debug", is_flag=True, help="Enable debug mode with verbose logging")
@click.option(
    "--overwrite", is_flag=True, help="Re-run experiments even if already completed"
)
def experiment(
    db_path: str,
    workers: int,
    limit: int,
    repo_name: str,
    docker_image: str,
    experiments: tuple[str, ...],
    debug: bool,
    overwrite: bool,
):
    """Run experiments on repositories in the database."""
    from analysis.database import Database
    from analysis.experiments import Experiment
    from analysis.worker import WorkerPool, WorkItem

    for experiment_name in experiments:
        assert (
            experiment_name in Experiment.experiments
        ), f"Unrecognized experiment {experiment_name}. Options: {list(Experiment.experiments.keys())}"

    experiments = (
        list(experiments) if experiments else list(Experiment.experiments.keys())
    )
    console.print(f"[bold]Experiments:[/bold] [green]{', '.join(experiments)}[/green]")
    console.print()

    db = Database(db_path=db_path)

    repos = db.fetchall(
        """
        SELECT
            core_repository.id,
            core_repository.full_name,
            core_repository.requirements,
            core_repository.commit_hash
        FROM core_repository
        WHERE core_repository.status = 'valid'
    """
    )

    if not overwrite:
        complete_repo_ids = set.intersection(
            *(
                Experiment.experiments[name].get_complete_repo_ids(db)
                for name in experiments
            )
        )
        repos = [repo for repo in repos if repo["id"] not in complete_repo_ids]
    if limit:
        repos = repos[:limit]

    if repo_name is not None:
        assert limit is None
        repos = [repo for repo in repos if repo["full_name"] == repo_name]
        # if you hit this, then maybe you forgot to pass --overwrite?
        assert len(repos) == 1, repos

    work_items = []
    for repo in repos:
        nodes = db.fetchall(
            "SELECT node_id, canonical_parametrization FROM core_node WHERE repo_id = ?",
            (repo["id"],),
        )
        node_ids = [node["node_id"] for node in nodes]
        canonical_node_ids = [
            node["node_id"] for node in nodes if node["canonical_parametrization"]
        ]

        # should have been marked invalid if there are no node ids
        assert node_ids

        work_item = WorkItem(
            repo_name=repo["full_name"],
            node_ids=node_ids,
            canonical_node_ids=canonical_node_ids,
            requirements=repo["requirements"],
            repo_id=repo["id"],
            commit_hash=repo["commit_hash"],
        )
        work_items.append(work_item)

    console.print(f"Repositories loaded: [green]{len(work_items)}[/green]")
    console.print(f"Workers: [green]{workers}[/green]")
    console.print(f"Docker image: [green]{docker_image}[/green]")
    console.print()

    if overwrite:
        console.print(
            "[yellow]Overwrite mode: deleting existing experiment data...[/yellow]"
        )
        for repo in repos:
            for experiment_name in experiments:
                Experiment.experiments[experiment_name].delete_data(db, repo["id"])

    # Create worker pool
    console.print("[bold]Starting analysis...[/bold]")

    with WorkerPool(
        # if there are less work items than workers, only launch as many workers
        # as work items
        num_workers=min(workers, len(work_items)),
        db_path=db_path,
        docker_image=docker_image,
        experiments=experiments,
        debug=debug,
    ) as pool:
        for item in work_items:
            pool.submit(item)

        successful = 0
        failed = 0

        for _ in range(len(work_items)):
            result = pool.get_result(timeout=None)
            if result:
                if result["success"]:
                    successful += 1
                    console.print(
                        f"[w{result['worker_id']}] {result['repo_name']}: [green]Success[/green]"
                    )
                else:
                    failed += 1
                    console.print(
                        f"[w{result['worker_id']}] {result['repo_name']}: [red]{result['error']}[/red]"
                    )

    # Print summary
    console.print()
    console.print("[bold]Analysis Complete![/bold]")
    console.print(f"Successful: [green]{successful}[/green]")
    console.print(f"Failed: [red]{failed}[/red]")
    console.print(f"Total: {len(work_items)}")

    console.print()
    console.print(
        "[green]To view results in the dashboard: streamlit run dashboard/Overview.py[/green]"
    )


# ==============================================================================
# INSTALL COMMAND
# ==============================================================================


def _populate_nodes_for_repo(
    db, repo_id: int, node_ids: list[str], source_codes: dict[str, str | None]
):
    # replace any existing nodes for this repository
    db.execute("DELETE FROM core_node WHERE repo_id = ?", (repo_id,))

    # mark first node in each parametrization group as canonical
    node_groups = defaultdict(list)
    for node_id in node_ids:
        base_name = node_id.split("[")[0]
        node_groups[base_name].append(node_id)

    for node_ids_in_group in node_groups.values():
        for i, node_id in enumerate(node_ids_in_group):
            is_canonical = i == 0
            db.execute(
                "INSERT INTO core_node (repo_id, node_id, canonical_parametrization, source_code) VALUES (?, ?, ?, ?)",
                (repo_id, node_id, is_canonical, source_codes.get(node_id)),
            )

    db.commit()


def _install(*, db_path, limit, debug, overwrite, repo_name):
    from analysis.collect.install_repos import install_repository
    from analysis.database import Database

    db = Database(db_path=db_path)

    repos = db.fetchall("SELECT full_name, status FROM core_repository")
    if not overwrite:
        repos = [repo for repo in repos if repo["status"] is None]

    if limit is not None:
        repos = repos[:limit]

    if repo_name is not None:
        repos = [repo for repo in repos if repo["full_name"] == repo_name]
        assert len(repos) == 1, repos

    console.print(f"Found [green]{len(repos)}[/green] repositories to process\n")

    successful = 0
    failed = 0
    # safeguard against fatal docker errors, host system running out of storage,
    # etc.
    consecutive_failed_repos = []

    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        console.print(f"[{i}/{len(repos)}] Processing [cyan]{repo_name}[/cyan] ...")

        try:
            result = install_repository(repo_name, debug=debug)
            consecutive_failed_repos = []
        except Exception as e:
            console.print(f"  ✗ Failed: [red]{traceback.format_exception(e)}[/red]\n")
            failed += 1
            consecutive_failed_repos.append(repo_name)

            if len(consecutive_failed_repos) >= 7:
                console.print(
                    f"\n[bold red]ABORTING: {len(consecutive_failed_repos)} fatal "
                    "errors in a row.[/bold red]"
                )
                console.print(
                    "[yellow]Failed repositories that were marked as install_error:[/yellow]"
                )
                for failed_repo in consecutive_failed_repos:
                    console.print(f"  - {failed_repo}")
                console.print(
                    f"\n[yellow]To reset these repos, run:[/yellow]\n"
                    f"  sqlite3 {db_path or 'analysis/data.db'} \"UPDATE core_repository SET status = NULL, status_reason = NULL "
                    f"WHERE full_name IN ({', '.join(repr(r) for r in consecutive_failed_repos)});\""
                )
                break

            db.execute(
                "UPDATE core_repository SET status = ?, status_reason = ? WHERE full_name = ?",
                ("invalid", "install_error", repo_name),
            )
            db.commit()
            continue

        if result["timed_out"]:
            status, status_reason = "invalid", "invalid_install (timed_out)"
        elif len(result["node_ids"]) == 0:
            status, status_reason = "invalid", "invalid_install (no_hypothesis_tests)"
        else:
            status, status_reason = "valid", None

        db.execute(
            """UPDATE core_repository
               SET status = ?, status_reason = ?, requirements = ?, node_ids = ?,
                   other_node_ids = ?, commit_hash = ?, collection_returncode = ?, collection_output = ?
               WHERE full_name = ?""",
            (
                status,
                status_reason,
                result["requirements"],
                json.dumps(result["node_ids"]),
                json.dumps(result["other_node_ids"]),
                result["commit_hash"],
                result["collection_returncode"],
                result["collection_output"],
                repo_name,
            ),
        )
        db.commit()

        if status == "valid":
            repo_row = db.fetchone(
                "SELECT id FROM core_repository WHERE full_name = ?", (repo_name,)
            )
            _populate_nodes_for_repo(
                db,
                repo_row["id"],
                result["node_ids"],
                result["source_codes"],
            )

        count = len(result["node_ids"])
        other_count = len(result["other_node_ids"])

        if status == "valid":
            console.print(
                f"  ✓ Successfully processed ([green]{count} hypothesis nodes, "
                f"{other_count} other nodes[/green], returncode: "
                f"[green]{result['collection_returncode']}[/green], commit: "
                f"[cyan]{result['commit_hash'][:7]}[/cyan])\n"
            )
            successful += 1
        else:
            console.print(
                f"  ✗ Rejected: [red]{status_reason}[/red] ([yellow]{count} "
                f"hypothesis nodes, {other_count} other nodes, returncode: "
                f"{result['collection_returncode']}[/yellow])\n"
            )
            failed += 1

    console.print("\n[bold]Installation Complete![/bold]")
    console.print(f"Successful: [green]{successful}[/green]")
    console.print(f"Failed: [red]{failed}[/red]")
    console.print(f"Total: {len(repos)}")



@cli.command()
@click.option("--db-path", help="Path to database file", default="analysis/data.db")
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option("--debug", is_flag=True, help="Enable debug mode with container logs")
@click.option(
    "--overwrite", is_flag=True, help="Re-run installation even if already completed"
)
@click.option("--repo", "repo_name", help="Process just this repository")
def install(db_path: str, limit: int, debug: bool, overwrite: bool, repo_name: str):
    _install(
        db_path=db_path,
        limit=limit,
        debug=debug,
        overwrite=overwrite,
        repo_name=repo_name,
    )


# ==============================================================================
# TASK COMMANDS
# ==============================================================================


@cli.group()
def task():
    """Task management commands."""


@task.command()
@click.argument("task_name")
@click.option("--db-path", help="Path to database", default="analysis/data.db")
def run(task_name: str, db_path: str):
    from analysis.database import Database
    from analysis.tasks import run_task

    db = Database(db_path=db_path)
    run_task(task_name, db=db)


@task.command()
@click.option("--db-path", help="Path to database", default="analysis/data.db")
@click.option("--task-name", help="Specific task to clear (default: all)")
def clear(db_path: str, task_name: str):
    """Clear task data from the database."""
    from analysis.database import Database
    from analysis.tasks import Task

    db = Database(db_path=db_path)

    if task_name:
        if task_name not in Task.tasks:
            console.print(f"[red]Task '{task_name}' not found[/red]")
            sys.exit(1)

        console.print(f"Clearing data for task: [yellow]{task_name}[/yellow]")
        Task.tasks[task_name].delete_data(db)
        console.print("[green]Data cleared successfully[/green]")
    else:
        console.print("[yellow]Clearing data for all tasks...[/yellow]")
        for name, task_class in Task.tasks.items():
            console.print(f"  Clearing {name}...")
            task_class.delete_data(db)
        console.print("[green]All task data cleared[/green]")


# ==============================================================================
# MARK_INVALID COMMAND
# ==============================================================================


@cli.command()
@click.argument("repo_name")
@click.argument("reason", type=click.Choice(["autogenerated_tests"]))
@click.option("--db-path", default="analysis/data.db")
def mark_invalid(repo_name: str, reason: str, db_path: str):
    """Mark a repository as invalid with the given reason."""
    from analysis.database import Database

    db = Database(db_path=db_path)
    repo = db.fetchone(
        "SELECT id, full_name, status FROM core_repository WHERE full_name = ?",
        (repo_name,),
    )

    if not repo:
        console.print(f"[red]Repository '{repo_name}' not found in database[/red]")
        sys.exit(1)

    db.execute(
        "UPDATE core_repository SET status = ?, status_reason = ? WHERE full_name = ?",
        ("invalid", f"manual ({reason})", repo_name),
    )
    db.commit()

    console.print(
        f"[green]✓[/green] Marked [cyan]{repo_name}[/cyan] as [red]invalid[/red] with reason: [yellow]{reason}[/yellow]"
    )


# ==============================================================================
# DASHBOARD COMMAND
# ==============================================================================


@cli.command()
@click.option("--db-path", help="Path to database file", default="analysis/data.db")
@click.option("--port", default=8501, help="Port to run dashboard on")
def dashboard(db_path: str, port: int):
    """Start the Streamlit dashboard."""

    console.print(f"[bold]Starting dashboard on port {port}...[/bold]")
    console.print(f"Database: [green]{db_path}[/green]")
    console.print(f"URL: [blue]http://localhost:{port}[/blue]")
    console.print()

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "dashboard/Overview.py",
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--",
        "--db-path",
        db_path,
    ]

    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped[/yellow]")


if __name__ == "__main__":
    cli()
