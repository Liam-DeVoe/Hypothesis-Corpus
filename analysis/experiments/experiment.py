from abc import ABC, abstractmethod
from typing import Any


class Experiment(ABC):
    experiments: dict[str, type["Experiment"]] = {}

    # Subclasses must define these as class attributes
    name: str
    node_tables: list[str]  # tables with a node_id FK to core_node
    repo_tables: list[str] = []  # tables with a repo_id FK to core_repository

    # If True, only run on canonical parametrization nodes
    only_canonical_nodes: bool = False

    def __init_subclass__(cls, **kwargs):
        if getattr(cls, "__abstractmethods__", None):
            return
        assert hasattr(cls, "name") and isinstance(cls.name, str)
        assert hasattr(cls, "node_tables")
        cls.experiments[cls.name] = cls

    @staticmethod
    @abstractmethod
    def get_schema_sql() -> dict[str, str]:
        """Return SQL to create database tables for this experiment.

        Returns a dict mapping database names to SQL strings.
        Use "main" for the main database, or a companion db name
        (e.g. "test_cases") for companion databases.
        """

    @staticmethod
    @abstractmethod
    def run(node_id: str) -> dict[str, Any]:
        """Run the experiment and return data."""

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

    @classmethod
    def has_repository_data(cls, db: Any, repo_id: int) -> bool:
        """Check if repo-level data already exists for this experiment."""
        return any(
            db.fetchone(f"SELECT 1 FROM {table} WHERE repo_id = ?", (repo_id,))
            for table in cls.repo_tables
        )

    @classmethod
    def get_complete_repo_ids(cls, db: Any) -> set[int]:
        """Return repo IDs where all expected nodes have experiment results.

        A repo is complete if every node (filtered by only_canonical_nodes)
        exists in at least one of the experiment's node_tables.
        """
        canonical_filter = (
            "AND cn.canonical_parametrization = 1" if cls.only_canonical_nodes else ""
        )
        union = " UNION ".join(
            f"SELECT DISTINCT node_id FROM {table}" for table in cls.node_tables
        )
        rows = db.fetchall(
            f"""
            SELECT cn.repo_id
            FROM core_node cn
            LEFT JOIN ({union}) completed ON cn.id = completed.node_id
            WHERE 1=1 {canonical_filter}
            GROUP BY cn.repo_id
            HAVING COUNT(DISTINCT cn.id) = COUNT(DISTINCT completed.node_id)
            """
        )
        return {row["repo_id"] for row in rows}

    @classmethod
    def get_completed_node_db_ids(cls, db: Any, repo_id: int) -> set[int]:
        """Return set of core_node.id values that have results for this experiment.

        A node is considered completed if it exists in any of the node_tables.
        """
        completed = set()
        for table in cls.node_tables:
            rows = db.fetchall(
                f"""SELECT DISTINCT t.node_id FROM {table} t
                JOIN core_node cn ON t.node_id = cn.id
                WHERE cn.repo_id = ?""",
                (repo_id,),
            )
            completed |= {row["node_id"] for row in rows}
        return completed

    @classmethod
    def delete_data(cls, db: Any, repo_id: int):
        """Delete this experiment's data for a repository."""
        for table in cls.repo_tables:
            db.execute(f"DELETE FROM {table} WHERE repo_id = ?", (repo_id,))

        node_ids = db.fetchall("SELECT id FROM core_node WHERE repo_id = ?", (repo_id,))
        node_id_list = [row["id"] for row in node_ids]
        if node_id_list:
            placeholders = ",".join("?" * len(node_id_list))
            for table in cls.node_tables:
                db.execute(
                    f"DELETE FROM {table} WHERE node_id IN ({placeholders})",
                    node_id_list,
                )

        db.commit()

    @staticmethod
    @abstractmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        """Store experiment results to the database."""
