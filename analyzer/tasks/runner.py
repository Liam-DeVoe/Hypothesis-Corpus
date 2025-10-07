"""Runner for tasks."""

import logging
from typing import Any

from ..database import Database
from .task import Task

logger = logging.getLogger(__name__)


def run_task(
    task_name: str,
    db_path: str = "data/analysis.db",
) -> dict[str, Any]:
    """Run a specific task.

    Args:
        task_name: Name of the task to run
        db_path: Path to the database

    Returns:
        Results from the task

    Raises:
        ValueError: If task doesn't exist
    """
    if task_name not in Task.tasks:
        available = ", ".join(Task.tasks.keys())
        raise ValueError(f"Task '{task_name}' not found. Available: {available}")

    logger.info(f"Running task: {task_name}")

    task_class = Task.tasks[task_name]
    db = Database(db_path)

    # Check dependencies
    for dep in task_class.follows:
        logger.info(f"Task follows experiment: {dep}")

    # Run the task
    logger.info("Executing task...")
    results = task_class.run(db)

    # Store results
    logger.info("Storing results...")
    task_class.store_to_database(db, results)

    logger.info(f"Task '{task_name}' completed successfully")
    return results


def run_tasks_for_experiment(
    experiment_name: str,
    db_path: str = "data/analysis.db",
) -> list[dict[str, Any]]:
    """Run all tasks that follow a given experiment.

    Args:
        experiment_name: Name of the experiment that just completed
        db_path: Path to the database

    Returns:
        List of results from each task
    """
    results = []

    for task_name, task_class in Task.tasks.items():
        if experiment_name in task_class.follows:
            logger.info(
                f"Running followup task '{task_name}' for experiment '{experiment_name}'"
            )
            try:
                result = run_task(task_name, db_path)
                results.append(
                    {
                        "task": task_name,
                        "success": True,
                        "data": result,
                    }
                )
            except Exception as e:
                logger.error(f"Task '{task_name}' failed: {e}", exc_info=True)
                results.append(
                    {
                        "task": task_name,
                        "success": False,
                        "error": str(e),
                    }
                )

    return results
