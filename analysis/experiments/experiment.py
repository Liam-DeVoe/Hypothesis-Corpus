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
    @abstractmethod
    def delete_data(db: Any, repo_name: str):
        """Delete this experiment's data from the database."""
        pass

    @staticmethod
    @abstractmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        """Store experiment results to the database."""
        pass
