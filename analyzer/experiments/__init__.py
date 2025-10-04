"""
Experiment framework for modular PBT analysis.

Experiments are automatically registered via __init_subclass__.
Import experiment modules to register them.
"""

from .base import Experiment, ExperimentResult
from .composite import AllExperiment
from .coverage import CoverageExperiment
from .registry import get_experiment, list_experiments
from .static_analysis import StaticAnalysisExperiment

__all__ = [
    "Experiment",
    "ExperimentResult",
    "get_experiment",
    "list_experiments",
    "AllExperiment",
    "CoverageExperiment",
    "StaticAnalysisExperiment",
]
