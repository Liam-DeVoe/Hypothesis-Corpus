import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


class Database:
    """SQLite database for storing PBT analysis results."""

    def __init__(self, db_path: str = "data/analysis.db"):
        """Initialize database connection."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_migrations()
        self._init_core_schema()
        self._init_experiment_schemas()
        self._init_task_schemas()

    def _run_migrations(self):
        pass

    def _init_core_schema(self):
        """Create core database schema shared by all experiments."""
        with self.connection() as conn:
            conn.executescript(
                """
                -- Repository information
                CREATE TABLE IF NOT EXISTS repositories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_name TEXT NOT NULL UNIQUE,
                    url TEXT NOT NULL,
                    clone_status TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                -- Node information
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_id INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    class_name TEXT,
                    node_name TEXT,
                    status TEXT,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (repo_id) REFERENCES repositories(id),
                    UNIQUE(repo_id, node_id),
                    FOREIGN KEY (repo_id) REFERENCES repositories(id)
                );

                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_nodes_repo ON nodes(repo_id);
                CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
            """
            )

    def _init_experiment_schemas(self):
        """Initialize database schemas for all registered experiments."""
        from .experiments import Experiment

        with self.connection() as conn:
            for experiment_name, experiment_class in Experiment.experiments.items():
                logger.debug(f"Initializing schema for experiment: {experiment_name}")
                schema_sql = experiment_class.get_schema_sql()
                conn.executescript(schema_sql)

    def _init_task_schemas(self):
        """Initialize database schemas for all registered tasks."""
        from .tasks import Task

        with self.connection() as conn:
            for task_name, task_class in Task.tasks.items():
                logger.debug(f"Initializing schema for task: {task_name}")
                schema_sql = task_class.get_schema_sql()
                conn.executescript(schema_sql)

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def delete_experiment_data(self, repo_name: str, tables: list[str]):
        with self.connection() as conn:
            result = conn.execute(
                "SELECT id FROM repositories WHERE repo_name = ?",
                (repo_name,),
            ).fetchone()

            if not result:
                return

            repo_id = result["id"]
            node_ids = conn.execute(
                "SELECT id FROM nodes WHERE repo_id = ?", (repo_id,)
            ).fetchall()
            node_id_list = [row["id"] for row in node_ids]

            if node_id_list:
                placeholders = ",".join("?" * len(node_id_list))
                for table in tables:
                    conn.execute(
                        f"DELETE FROM {table} WHERE node_id IN ({placeholders})",
                        node_id_list,
                    )

            conn.commit()
