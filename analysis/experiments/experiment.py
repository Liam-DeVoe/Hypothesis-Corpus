from abc import ABC, abstractmethod
from typing import Any


class Experiment(ABC):
    experiments: dict[str, type["Experiment"]] = {}

    # Subclasses must define this as a class attribute
    name: str

    def __init_subclass__(cls, **kwargs):
        if getattr(cls, "__abstractmethods__", None):
            return
        assert hasattr(cls, "name") and isinstance(cls.name, str)
        cls.experiments[cls.name] = cls

    @staticmethod
    @abstractmethod
    def get_schema_sql() -> str:
        """Return SQL to create database tables for this experiment."""
        pass

    @staticmethod
    @abstractmethod
    def run(node_id: str) -> dict[str, Any]:
        """Run the experiment and return data."""
        pass

    @staticmethod
    def run_repository(repo_name: str, node_ids: list[str]) -> dict[str, Any]:
        """Run repository-level analysis. Optional method that runs once per repository.

        Args:
            repo_name: Full name of the repository (e.g., 'owner/repo')
            node_ids: List of all test node IDs in this repository

        Returns:
            Dictionary of results to store, or None if not implemented
        """
        return None

    @staticmethod
    def store_repository_to_database(db: Any, repo_id: int, data: dict[str, Any]):
        """Store repository-level results to the database. Optional method.

        Args:
            db: Database instance
            repo_id: Repository ID in the database
            data: Repository-level data returned from run_repository()
        """
        pass

    @staticmethod
    @abstractmethod
    def delete_data(db: Any, repo_id: int):
        """Delete this experiment's data from the database."""
        pass

    @staticmethod
    @abstractmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        """Store experiment results to the database."""
        pass
