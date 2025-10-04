"""
Experiment registry utilities.

Note: Experiments are automatically registered via __init_subclass__ in base.Experiment.
This module provides convenience functions for accessing registered experiments.
"""

from .base import Experiment


def get_experiment(name: str) -> Experiment:
    """Get an experiment instance by name.

    Args:
        name: Experiment name

    Returns:
        Experiment instance

    Raises:
        ValueError: If experiment not found
    """
    return Experiment.get_experiment(name)


def list_experiments() -> list[str]:
    """Get all available experiment names.

    Returns:
        List of experiment names
    """
    return Experiment.list_experiments()
