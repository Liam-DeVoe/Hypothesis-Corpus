"""
Main script to run the PBT corpus analysis.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from analyzer.database import Database
from analyzer.experiments import Experiment
from analyzer.worker import WorkerPool, WorkItem

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="",
    handlers=[logging.StreamHandler()],
    force=True,  # Override any existing configuration
)

# Ensure all loggers use the same format
for name in ["analyzer.test_runner", "analyzer.worker", "analyzer.database"]:
    logging.getLogger(name).handlers = []
    logging.getLogger(name).propagate = True

logger = logging.getLogger(__name__)
console = Console()


def load_dataset(dataset_path: str) -> dict[str, Any]:
    """Load dataset from JSON file."""
    path = Path(dataset_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {dataset_path}")

    with open(path) as f:
        return json.load(f)


def load_config(config_path: str = "analyzer/config.yaml") -> dict[str, Any]:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        console.print(
            f"[yellow]Warning: Config file not found at {config_path}, using defaults[/yellow]"
        )
        return {
            "database": {"path": "data/analysis.db"},
            "docker": {"image": "pbt-analyzer:latest"},
            "workers": {"max_workers": 4},
        }

    with open(path) as f:
        return yaml.safe_load(f)


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
@click.option("--dataset", "-d", type=str, help="Path to dataset JSON file")
@click.option(
    "--config", "-c", default="analyzer/config.yaml", help="Path to configuration file"
)
@click.option("--workers", "-w", type=int, help="Number of worker processes")
@click.option(
    "--sample", "-s", is_flag=True, help="Run sample test with MarkCBell/bigger"
)
@click.option("--limit", "-l", type=int, help="Limit number of repositories to process")
@click.option("--docker-image", type=str, help="Docker image to use")
@click.option(
    "--experiment",
    "-e",
    multiple=True,
    help="Experiments to run (default: all)",
)
@click.option("--debug", is_flag=True, help="Enable debug mode with verbose logging")
def main(
    dataset: str,
    config: str,
    workers: int,
    sample: bool,
    limit: int,
    docker_image: str,
    experiment: tuple[str, ...],
    debug: bool,
):
    """Run PBT corpus analysis."""

    console.print()

    experiments = (
        list(experiment) if experiment else list(Experiment.experiments.keys())
    )
    console.print(f"[bold]Experiments:[/bold] [green]{', '.join(experiments)}[/green]")
    console.print()

    # Load configuration
    cfg = load_config(config)

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
        # Only run coverage experiment in sample mode
        experiments = ["coverage"]
    elif dataset:
        dataset_data = load_dataset(dataset)
    else:
        console.print("[red]Error: Please provide --dataset or use --sample flag[/red]")
        sys.exit(1)

    # Override with command line options
    if workers:
        cfg["workers"]["max_workers"] = workers
    if docker_image:
        cfg["docker"]["image"] = docker_image

    # Prepare work items
    work_items = prepare_work_items(dataset_data)

    if limit and limit < len(work_items):
        work_items = work_items[:limit]
        console.print(f"[yellow]Limited to {limit} repositories[/yellow]")

    console.print(f"Dataset loaded: [green]{len(work_items)} repositories[/green]")
    console.print(f"Workers: [green]{cfg['workers']['max_workers']}[/green]")
    console.print(f"Docker image: [green]{cfg['docker']['image']}[/green]")
    console.print()

    # Initialize database
    db = Database(cfg["database"]["path"])

    # Start analysis run
    with db.connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO analysis_runs (configuration, total_repos, experiment_name)
            VALUES (?, ?, ?)
        """,
            (json.dumps(cfg), len(work_items), ",".join(experiments)),
        )
        run_id = cursor.lastrowid
        conn.commit()

    # Create worker pool
    console.print("[bold]Starting analysis...[/bold]")

    with WorkerPool(
        num_workers=cfg["workers"]["max_workers"],
        db_path=cfg["database"]["path"],
        docker_image=cfg["docker"]["image"],
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

                console.print(
                    f"[w{result['worker_id']}] Finished repository {result['repo_name']}"
                )

    # Update analysis run
    with db.connection() as conn:
        conn.execute(
            """
            UPDATE analysis_runs
            SET end_time = CURRENT_TIMESTAMP,
                successful_repos = ?,
                failed_repos = ?
            WHERE id = ?
        """,
            (successful, failed, run_id),
        )
        conn.commit()

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
