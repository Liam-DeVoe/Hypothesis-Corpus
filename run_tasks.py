"""CLI script to run tasks manually."""

import logging
import sys

import click
from rich.console import Console

from analyzer.database import Database
from analyzer.tasks import (
    run_task,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="",
    handlers=[logging.StreamHandler()],
    force=True,
)

logger = logging.getLogger(__name__)
console = Console()


@click.group()
def cli():
    """Task management commands."""
    pass


@cli.command()
@click.argument("task")
@click.option("--db-path", default="data/analysis.db", help="Path to database")
def run(task: str, db_path: str):
    """Run a specific task.

    TASK is the name of the task to run.
    """
    console.print(f"\n[bold]Running task: {task}[/bold]\n")

    try:
        results = run_task(task, db_path)

        console.print("\n[green]Task completed successfully![/green]\n")

        # Display results
        if "num_pattern_clusters" in results:
            console.print(
                f"Pattern clusters created: [cyan]{results['num_pattern_clusters']}[/cyan]"
            )
        if "num_domain_clusters" in results:
            console.print(
                f"Domain clusters created: [cyan]{results['num_domain_clusters']}[/cyan]"
            )

        console.print()

    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]\n")
        sys.exit(1)


@cli.command()
@click.option("--db-path", default="data/analysis.db", help="Path to database")
@click.option("--task", help="Specific task to clear (default: all)")
def clear(db_path: str, task: str):
    """Clear task data from the database."""
    from analyzer.tasks import Task

    db = Database(db_path)

    if task:
        if task not in Task.tasks:
            console.print(f"[red]Task '{task}' not found[/red]")
            sys.exit(1)

        console.print(f"Clearing data for task: [yellow]{task}[/yellow]")
        Task.tasks[task].delete_data(db)
        console.print("[green]Data cleared successfully[/green]")
    else:
        console.print("[yellow]Clearing data for all tasks...[/yellow]")
        for task_name, task_class in Task.tasks.items():
            console.print(f"  Clearing {task_name}...")
            task_class.delete_data(db)
        console.print("[green]All task data cleared[/green]")


if __name__ == "__main__":
    cli()
