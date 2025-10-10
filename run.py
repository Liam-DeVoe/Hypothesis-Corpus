"""
Unified CLI for PBT corpus analysis system.
"""

import json
import logging
import sys
import traceback

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
@click.option("--db-path", default="analysis/data.db", help="Path to database file")
def collect(db_path: str):
    """Collect repositories from GitHub and store in database."""
    from analysis.collect.run import run_collection

    run_collection(db_path)


# ==============================================================================
# EXPERIMENT COMMAND
# ==============================================================================


@cli.command()
@click.option("--db-path", default="analysis/data.db", help="Path to database file")
@click.option("--workers", "-w", type=int, default=4, help="Number of worker processes")
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option(
    "--docker-image", default="pbt-analysis:latest", help="Docker image to use"
)
@click.option(
    "--experiment", "-e", multiple=True, help="Experiments to run (default: all)"
)
@click.option("--debug", is_flag=True, help="Enable debug mode with verbose logging")
def experiment(
    db_path: str,
    workers: int,
    limit: int,
    docker_image: str,
    experiment: tuple[str, ...],
    debug: bool,
):
    """Run experiments on repositories in the database."""
    from analysis.database import Database
    from analysis.experiments import Experiment
    from analysis.worker import WorkerPool, WorkItem

    experiments = (
        list(experiment) if experiment else list(Experiment.experiments.keys())
    )
    console.print(f"[bold]Experiments:[/bold] [green]{', '.join(experiments)}[/green]")
    console.print()

    db = Database(db_path=db_path)

    # Load work items directly from database
    query = "SELECT full_name, requirements, node_ids FROM core_repository WHERE status = 'valid'"
    if limit:
        query += f" LIMIT {limit}"

    repos = db.fetchall(query)

    work_items = []
    for repo in repos:
        work_item = WorkItem(
            repo_name=repo["full_name"],
            node_ids=json.loads(repo["node_ids"]),
            requirements=repo["requirements"] or "",
        )
        work_items.append(work_item)

    console.print(f"Repositories loaded: [green]{len(work_items)}[/green]")
    console.print(f"Workers: [green]{workers}[/green]")
    console.print(f"Docker image: [green]{docker_image}[/green]")
    console.print()

    # Create worker pool
    console.print("[bold]Starting analysis...[/bold]")

    with WorkerPool(
        num_workers=workers,
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


@cli.command()
@click.option("--db-path", default="analysis/data.db", help="Path to database file")
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option("--debug", is_flag=True, help="Enable debug mode with container logs")
def install(db_path: str, limit: int, debug: bool):
    """Install repositories and collect test node IDs."""
    from analysis.collect.install_repos import install_repository
    from analysis.database import Database

    db = Database(db_path=db_path)

    # Get repositories that need processing
    query = "SELECT full_name FROM core_repository WHERE status IS NULL"
    if limit:
        query += f" LIMIT {limit}"

    repos = db.fetchall(query)

    console.print(f"Found [green]{len(repos)}[/green] repositories to process\n")

    successful = 0
    failed = 0

    def _reject(repo_name: str):
        nonlocal failed
        failed += 1
        db.execute(
            "UPDATE core_repository SET status = ? WHERE full_name = ?",
            ("invalid", repo_name),
        )
        db.commit()

    def is_clean_install(result):
        return result["collection_returncode"] == 0 and len(result["node_ids"]) > 0

    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        console.print(f"[{i}/{len(repos)}] Processing [cyan]{repo_name}[/cyan]...")

        try:
            result = install_repository(repo_name, debug=debug)
        except Exception as e:
            console.print(f"  ✗ Failed: [red]{traceback.format_exception(e)}[/red]\n")
            _reject(repo_name)
            continue

        if not is_clean_install(result):
            console.print(f"  ✗ Failed: [red]not a clean install ({result})[/red]\n")
            _reject(repo_name)
            continue

        db.execute(
            "UPDATE core_repository SET status = ?, requirements = ?, node_ids = ?, commit_hash = ? WHERE full_name = ?",
            (
                "valid",
                result["requirements"],
                json.dumps(result["node_ids"]),
                result.get("commit_hash"),
                repo_name,
            ),
        )
        db.commit()

        console.print(
            f"  ✓ Successfully processed ([green]{len(result['node_ids'])} nodes[/green], commit: [cyan]{result.get('commit_hash', 'unknown')[:7]}[/cyan])\n"
        )
        successful += 1

    # Print summary
    console.print("\n[bold]Installation Complete![/bold]")
    console.print(f"Successful: [green]{successful}[/green]")
    console.print(f"Failed: [red]{failed}[/red]")
    console.print(f"Total: {len(repos)}")


# ==============================================================================
# TASK COMMANDS
# ==============================================================================


@cli.group()
def task():
    """Task management commands."""
    pass


@task.command()
@click.argument("task_name")
@click.option("--db-path", default="analysis/data.db", help="Path to database")
def run(task_name: str, db_path: str):
    from analysis.database import Database
    from analysis.tasks import run_task

    db = Database(db_path=db_path)
    run_task(task_name, db=db)


@task.command()
@click.option("--db-path", default="analysis/data.db", help="Path to database")
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
# DASHBOARD COMMAND
# ==============================================================================


@cli.command()
@click.option("--db-path", default="analysis/data.db", help="Path to database file")
@click.option("--port", default=8501, help="Port to run dashboard on")
def dashboard(db_path: str, port: int):
    """Start the Streamlit dashboard."""
    import os
    import subprocess

    # Set environment variable for dashboard to use
    os.environ["PBT_DB_PATH"] = db_path

    console.print(f"[bold]Starting dashboard on port {port}...[/bold]")
    console.print(f"Database: [green]{db_path}[/green]")
    console.print(f"URL: [blue]http://localhost:{port}[/blue]")
    console.print()

    try:
        subprocess.run(
            ["streamlit", "run", "dashboard/Overview.py", "--server.port", str(port)],
            check=True,
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped[/yellow]")


if __name__ == "__main__":
    cli()
