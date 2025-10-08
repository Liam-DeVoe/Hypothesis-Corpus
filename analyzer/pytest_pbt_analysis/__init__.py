"""Pytest PBT analysis plugin for controlling test execution in experiments."""

__version__ = "0.1.0"

from .pytest_pbt_analysis import pytest_addoption, pytest_collection_modifyitems

__all__ = ["pytest_addoption", "pytest_collection_modifyitems"]
