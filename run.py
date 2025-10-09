"""
Unified CLI for PBT corpus analysis system.
"""

import logging
import sys
from typing import Any

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
# ANALYSIS COMMAND
# ==============================================================================


def prepare_work_items(dataset: dict[str, Any]):
    """Convert dataset to work items."""
    from analysis.worker import WorkItem

    work_items = []
    for repo_name, repo_data in dataset.items():
        work_item = WorkItem(
            repo_name=repo_name,
            node_ids=repo_data.get("node_ids", []),
            requirements=repo_data.get("requirements.txt", ""),
        )
        work_items.append(work_item)

    return work_items


@cli.command()
@click.option("--db-path", default="analysis/data.db", help="Path to database file")
@click.option("--workers", "-w", type=int, default=4, help="Number of worker processes")
@click.option(
    "--sample", "-s", is_flag=True, help="Run sample test with MarkCBell/bigger"
)
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option(
    "--docker-image", default="pbt-analysis:latest", help="Docker image to use"
)
@click.option(
    "--experiment", "-e", multiple=True, help="Experiments to run (default: all)"
)
@click.option("--debug", is_flag=True, help="Enable debug mode with verbose logging")
def analysis(
    db_path: str,
    workers: int,
    sample: bool,
    limit: int,
    docker_image: str,
    experiment: tuple[str, ...],
    debug: bool,
):
    """Run analysis on repositories in the database."""
    from analysis.database import Database
    from analysis.experiments import Experiment
    from analysis.worker import WorkerPool

    experiments = (
        list(experiment) if experiment else list(Experiment.experiments.keys())
    )
    console.print(f"[bold]Experiments:[/bold] [green]{', '.join(experiments)}[/green]")
    console.print()

    # Handle sample mode
    if sample:
        console.print("[yellow]Running in sample mode with MarkCBell/bigger[/yellow]")
        dataset_data = {
            "MarkCBell/bigger": {
                "node_ids": ["tests/structures.py::TestUnionFind::runTest"],
                "requirements.txt": "attrs==24.2.0\nexceptiongroup==1.2.2\nhypothesis==6.112.5\niniconfig==2.0.0\npackaging==24.1\npillow==11.0.0\npluggy==1.5.0\npytest==8.2.2\nsortedcontainers==2.4.0\ntomli==2.0.2",
            }
        }
        workers = 1
        experiments = ["runtime"]
    else:
        dataset_data = load_dataset_from_db(db_path, limit=limit)

    # Prepare work items
    work_items = prepare_work_items(dataset_data)

    console.print(f"Dataset loaded: [green]{len(work_items)} repositories[/green]")
    console.print(f"Workers: [green]{workers}[/green]")
    console.print(f"Docker image: [green]{docker_image}[/green]")
    console.print()

    # Initialize database
    Database(db_path=db_path)

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
    from analysis.tasks import run_task

    run_task(task_name, db_path=db_path)


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


if __name__ == "__main__":
    cli()
