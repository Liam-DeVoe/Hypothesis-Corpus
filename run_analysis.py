"""
Main script to run the PBT corpus analysis.
"""

import logging
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from analysis.database import Database
from analysis.experiments import Experiment
from analysis.worker import WorkerPool, WorkItem

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="",
    handlers=[logging.StreamHandler()],
    force=True,  # Override any existing configuration
)

# Ensure all loggers use the same format
for name in ["analysis.test_runner", "analysis.worker", "analysis.database"]:
    logging.getLogger(name).handlers = []
    logging.getLogger(name).propagate = True

logger = logging.getLogger(__name__)
console = Console()


def load_dataset_from_db(db_path: str, limit: int | None = None) -> dict[str, Any]:
    """Load dataset from database."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    query = "SELECT full_name, requirements FROM core_repositories"
    if limit:
        query += f" LIMIT {limit}"

    repos = conn.execute(query).fetchall()
    conn.close()

    # Convert to dataset format
    dataset = {}
    for repo in repos:
        # TODO: We need to get node_ids from somewhere. For now, use empty list
        # which will trigger test discovery
        dataset[repo["full_name"]] = {
            "node_ids": [],
            "requirements.txt": repo["requirements"] or "",
        }

    return dataset




def prepare_work_items(dataset: dict[str, Any]) -> list[WorkItem]:
    """Convert dataset to work items."""
    work_items = []

    for repo_name, repo_data in dataset.items():
        work_item = WorkItem(
            repo_name=repo_name,
            node_ids=repo_data.get("node_ids", []),
            requirements=repo_data.get("requirements.txt", ""),
        )
        work_items.append(work_item)

    return work_items


@click.command()
@click.option(
    "--db-path", default="analysis/data.db", help="Path to database file"
)
@click.option("--workers", "-w", type=int, default=4, help="Number of worker processes")
@click.option(
    "--sample", "-s", is_flag=True, help="Run sample test with MarkCBell/bigger"
)
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option("--docker-image", default="pbt-analysis:latest", help="Docker image to use")
@click.option(
    "--experiment",
    "-e",
    multiple=True,
    help="Experiments to run (default: all)",
)
@click.option("--debug", is_flag=True, help="Enable debug mode with verbose logging")
def main(
    db_path: str,
    workers: int,
    sample: bool,
    limit: int,
    docker_image: str,
    experiment: tuple[str, ...],
    debug: bool,
):
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
        # Only run runtime experiment in sample mode
        experiments = ["runtime"]
    else:
        # Load from database
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


if __name__ == "__main__":
    main()
