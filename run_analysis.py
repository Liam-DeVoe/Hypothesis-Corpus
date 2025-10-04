"""
Main script to run the PBT corpus analysis.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any

import click
import yaml
from rich.console import Console

from analyzer.database import Database
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


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
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
    "--config", "-c", default="config.yaml", help="Path to configuration file"
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
    type=str,
    default="all",
    help="Experiment to run (static, coverage, ast, all)",
)
@click.option(
    "--list-experiments",
    is_flag=True,
    help="List available experiments and exit",
)
def main(
    dataset: str,
    config: str,
    workers: int,
    sample: bool,
    limit: int,
    docker_image: str,
    experiment: str,
    list_experiments: bool,
):
    """Run PBT corpus analysis."""

    # Handle --list-experiments first
    if list_experiments:
        from analyzer.experiments import list_experiments as get_experiments_list

        console.print("[bold]Available Experiments:[/bold]")
        console.print()
        for exp_name in get_experiments_list():
            console.print(f"  • [cyan]{exp_name}[/cyan]")
        console.print()
        return

    console.print("[bold blue]🔬 Property-Based Testing Corpus Analysis[/bold blue]")
    console.print()

    # Validate experiment
    from analyzer.experiments import Experiment

    exp = Experiment.get_experiment(experiment)
    console.print(f"[bold]Experiment:[/bold] [green]{exp.name}[/green]")
    console.print()

    # Load configuration
    cfg = load_config(config)

    # Override with command line options
    if workers:
        cfg["workers"]["max_workers"] = workers
    if docker_image:
        cfg["docker"]["image"] = docker_image

    # Handle sample mode
    if sample:
        console.print("[yellow]Running in sample mode with MarkCBell/bigger[/yellow]")
        dataset_data = {
            "MarkCBell/bigger": {
                "node_ids": ["tests/structures.py::TestUnionFind::runTest"],
                "requirements.txt": "attrs==24.2.0\nexceptiongroup==1.2.2\nhypothesis==6.112.5\niniconfig==2.0.0\npackaging==24.1\npillow==11.0.0\npluggy==1.5.0\npytest==8.2.2\nsortedcontainers==2.4.0\ntomli==2.0.2",
            }
        }
    elif dataset:
        dataset_data = load_dataset(dataset)
    else:
        console.print("[red]Error: Please provide --dataset or use --sample flag[/red]")
        sys.exit(1)

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
            (json.dumps(cfg), len(work_items), experiment),
        )
        run_id = cursor.lastrowid
        conn.commit()

    # Create worker pool
    console.print("[bold]Starting analysis...[/bold]")

    with WorkerPool(
        num_workers=cfg["workers"]["max_workers"],
        db_path=cfg["database"]["path"],
        docker_image=cfg["docker"]["image"],
        experiment_name=experiment,
    ) as pool:
        pool.submit_batch(work_items)
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
                    error = result.get("error", "Unknown error")
                    console.print(
                        f"[w{result['worker_id']}] {result['repo_name']}: [red]{error}[/red]"
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

    # Get and display statistics
    stats = db.get_analysis_stats()

    console.print()
    console.print("[bold]Top Generators:[/bold]")
    for gen in stats["top_generators"][:5]:
        console.print(f"  • {gen['generator_name']}: {gen['total_uses']} uses")

    console.print()
    console.print("[bold]Property Types:[/bold]")
    for prop in stats["property_types"]:
        console.print(f"  • {prop['property_type']}: {prop['count']} tests")

    console.print()
    console.print("[bold]Feature Usage:[/bold]")
    for feature in stats["feature_usage"]:
        console.print(
            f"  • {feature['feature_name']}: {feature['total_uses']} uses in {feature['test_count']} tests"
        )

    console.print()
    console.print(
        "[green]✨ View results in the dashboard: streamlit run dashboard.py[/green]"
    )


if __name__ == "__main__":
    main()
