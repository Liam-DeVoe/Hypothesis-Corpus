"""
Base experiment class and result structures.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ExperimentResult:
    """Results from running an experiment."""

    node_id: str
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "node_id": self.node_id,
            "success": self.success,
            "data": self.data,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ExperimentResult":
        """Create from dictionary."""
        return cls(
            node_id=d["node_id"],
            success=d["success"],
            data=d.get("data", {}),
            error=d.get("error"),
        )


class Experiment(ABC):
    """Base class for all experiments.

    Subclasses are automatically registered via __init_subclass__.
    Subclasses must define a class-level `name` attribute.
    """

    _registry: dict[str, type["Experiment"]] = {}

    # Subclasses must define this as a class attribute
    name: str

    def __init_subclass__(cls, **kwargs):
        """Automatically register experiment subclasses."""
        super().__init_subclass__(**kwargs)
        # Only register concrete classes (not abstract intermediates)
        if not getattr(cls, "__abstractmethods__", None):
            # Validate that name is defined
            if not hasattr(cls, "name") or not isinstance(getattr(cls, "name"), str):
                raise TypeError(
                    f"Experiment {cls.__name__} must define a 'name' class attribute"
                )
            cls._registry[cls.name] = cls

    @property
    def dependencies(self) -> list[str]:
        """List of experiment names this experiment depends on.

        Dependencies will be run first and their results made available.
        """
        return []

    def process_results(
        self,
        node_id: str,
        container_results: "ExperimentResult",
    ) -> "ExperimentResult":
        """Process results from container execution.

        The default implementation is a pass-through that returns the container
        results as-is. Override this method if you need to perform additional
        processing outside the container (e.g., AST analysis).

        Args:
            node_id: The test node ID
            container_results: ExperimentResult from container

        Returns:
            ExperimentResult with processed data
        """
        return container_results

    @abstractmethod
    def delete_data(self, db: Any, owner: str, name: str):
        """Delete this experiment's data from the database.

        Args:
            db: Database instance
            owner: Repository owner
            name: Repository name
        """
        pass

    @abstractmethod
    def store_to_database(
        self, db: Any, repo_id: int, test_id: int, result: ExperimentResult
    ):
        """Store experiment results to the database.

        Args:
            db: Database instance
            repo_id: Repository ID
            test_id: Test ID
            result: Experiment result to store
        """
        pass

    def validate_requirements(self, work_dir: Path) -> tuple[bool, str | None]:
        """Validate that requirements for this experiment are met.

        Args:
            work_dir: Working directory containing the repository

        Returns:
            (is_valid, error_message) tuple
        """
        return True, None

    @classmethod
    def get_experiment(cls, name: str) -> "Experiment":
        """Get an experiment instance by name.

        Args:
            name: Experiment name

        Returns:
            Experiment instance

        Raises:
            ValueError: If experiment not found
        """
        if name not in cls._registry:
            available = ", ".join(cls._registry.keys())
            raise ValueError(
                f"Unknown experiment '{name}'. Available experiments: {available}"
            )
        return cls._registry[name]()

    @classmethod
    def list_experiments(cls) -> list[str]:
        """Get all available experiment names.

        Returns:
            List of experiment names
        """
        return list(cls._registry.keys())
