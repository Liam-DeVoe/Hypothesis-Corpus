import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# Global cache for database instances (singleton per db_path)
_database_cache = {}


def get_database(db_path: str) -> "Database":
    """Get database instance from cache, creating if needed."""
    resolved_path = str(Path(db_path).resolve())
    if resolved_path not in _database_cache:
        _database_cache[resolved_path] = Database(db_path=db_path)
    return _database_cache[resolved_path]


class Database:
    """SQLite database for storing PBT analysis results.

    Maintains a single connection and provides execute methods for all database operations.
    Consumer code should use db.execute() methods rather than creating their own connections.
    """

    def __init__(self, *, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Create single persistent connection
        self._conn = sqlite3.connect(
            self.db_path, timeout=30.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row

        self._init_core_schema()
        self._init_experiment_schemas()
        self._init_task_schemas()

    def _init_core_schema(self):
        self._conn.executescript(
            """
            -- Repository information (populated by collection)
            CREATE TABLE IF NOT EXISTS core_repository (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT UNIQUE NOT NULL,
                size_bytes INTEGER NOT NULL,
                stargazers_count INTEGER NOT NULL,
                is_fork BOOLEAN NOT NULL,
                status TEXT,  -- NULL (not processed), 'valid' (installed successfully), 'invalid' (installation failed)
                status_reason TEXT,
                requirements TEXT,
                node_ids TEXT,  -- JSON list of Hypothesis test node IDs
                other_node_ids TEXT,  -- JSON list of non-Hypothesis test node IDs
                commit_hash TEXT,  -- Git commit hash at time of install_repos.py
                collection_returncode INTEGER,  -- pytest collection return code
                collection_output TEXT,  -- Container logs from pytest collection
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- MinHash data for deduplication (populated by collection)
            CREATE TABLE IF NOT EXISTS core_minhashes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER NOT NULL,
                minhash_data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (repo_id) REFERENCES core_repository(id)
            );

            -- Node information (populated by analysis)
            CREATE TABLE IF NOT EXISTS core_node (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (repo_id) REFERENCES core_repository(id),
                UNIQUE(repo_id, node_id)
            );

            -- Create indexes for better query performance
            CREATE INDEX IF NOT EXISTS idx_minhashes_repo ON core_minhashes(repo_id);
            CREATE INDEX IF NOT EXISTS idx_nodes_repo ON core_node(repo_id);
            CREATE INDEX IF NOT EXISTS idx_repository_status ON core_repository(status);
        """
        )
        self._conn.commit()

    def _init_experiment_schemas(self):
        """Initialize database schemas for all registered experiments."""
        from .experiments import Experiment

        for experiment_name, experiment_class in Experiment.experiments.items():
            logger.debug(f"Initializing schema for experiment: {experiment_name}")
            schema_sql = experiment_class.get_schema_sql()
            self._conn.executescript(schema_sql)
        self._conn.commit()

    def _init_task_schemas(self):
        """Initialize database schemas for all registered tasks."""
        from .tasks import Task

        for task_name, task_class in Task.tasks.items():
            logger.debug(f"Initializing schema for task: {task_name}")
            schema_sql = task_class.get_schema_sql()
            self._conn.executescript(schema_sql)
        self._conn.commit()

    def execute(self, query: str, parameters=None):
        if parameters is None:
            return self._conn.execute(query)
        return self._conn.execute(query, parameters)

    def executemany(self, query: str, parameters):
        return self._conn.executemany(query, parameters)

    def executescript(self, script: str):
        return self._conn.executescript(script)

    def commit(self):
        self._conn.commit()

    def fetchone(self, query: str, parameters=None):
        cursor = self.execute(query, parameters)
        return cursor.fetchone()

    def fetchall(self, query: str, parameters=None):
        cursor = self.execute(query, parameters)
        return cursor.fetchall()

    def delete_experiment_data(self, repo_name: str, tables: list[str]):
        result = self.fetchone(
            "SELECT id FROM core_repository WHERE full_name = ?",
            (repo_name,),
        )

        if not result:
            return

        repo_id = result["id"]
        node_ids = self.fetchall(
            "SELECT id FROM core_node WHERE repo_id = ?", (repo_id,)
        )
        node_id_list = [row["id"] for row in node_ids]

        if node_id_list:
            placeholders = ",".join("?" * len(node_id_list))
            for table in tables:
                self.execute(
                    f"DELETE FROM {table} WHERE node_id IN ({placeholders})",
                    node_id_list,
                )

        self.commit()
