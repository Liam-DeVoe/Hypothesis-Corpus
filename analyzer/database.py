"""
Database module for storing and retrieving analysis results.
"""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Database:
    """SQLite database for storing PBT analysis results."""

    def __init__(self, db_path: str = "data/analysis.db"):
        """Initialize database connection."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self):
        """Create database schema if it doesn't exist."""
        with self.connection() as conn:
            conn.executescript(
                """
                -- Repository information
                CREATE TABLE IF NOT EXISTS repositories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner TEXT NOT NULL,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    clone_status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(owner, name)
                );
                
                -- Test information
                CREATE TABLE IF NOT EXISTS tests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_id INTEGER NOT NULL,
                    node_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    class_name TEXT,
                    test_name TEXT,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (repo_id) REFERENCES repositories(id),
                    UNIQUE(repo_id, node_id)
                );
                
                -- Generator usage
                CREATE TABLE IF NOT EXISTS generator_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    generator_name TEXT NOT NULL,
                    count INTEGER DEFAULT 1,
                    is_composite BOOLEAN DEFAULT 0,
                    is_custom BOOLEAN DEFAULT 0,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );
                
                -- Property types
                CREATE TABLE IF NOT EXISTS property_types (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    property_type TEXT NOT NULL,
                    confidence REAL DEFAULT 1.0,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );
                
                -- Feature usage (assume, note, event, target)
                CREATE TABLE IF NOT EXISTS feature_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    feature_name TEXT NOT NULL,
                    count INTEGER DEFAULT 1,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );
                
                -- Test runner information
                CREATE TABLE IF NOT EXISTS test_runners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_id INTEGER NOT NULL,
                    runner_type TEXT NOT NULL,  -- pytest, nose, unittest, tox
                    config_file TEXT,
                    test_directory TEXT,
                    FOREIGN KEY (repo_id) REFERENCES repositories(id)
                );
                
                -- Raw test code for further analysis
                CREATE TABLE IF NOT EXISTS test_code (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    source_code TEXT NOT NULL,
                    ast_json TEXT,  -- Store AST as JSON for later analysis
                    FOREIGN KEY (test_id) REFERENCES tests(id),
                    UNIQUE(test_id)
                );
                
                -- Analysis metadata
                CREATE TABLE IF NOT EXISTS analysis_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    total_repos INTEGER DEFAULT 0,
                    successful_repos INTEGER DEFAULT 0,
                    failed_repos INTEGER DEFAULT 0,
                    configuration TEXT  -- JSON configuration used
                );
                
                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_tests_repo ON tests(repo_id);
                CREATE INDEX IF NOT EXISTS idx_generators_test ON generator_usage(test_id);
                CREATE INDEX IF NOT EXISTS idx_properties_test ON property_types(test_id);
                CREATE INDEX IF NOT EXISTS idx_features_test ON feature_usage(test_id);
                CREATE INDEX IF NOT EXISTS idx_repos_status ON repositories(clone_status);
                CREATE INDEX IF NOT EXISTS idx_tests_status ON tests(status);
            """
            )
            conn.commit()

    @contextmanager
    def connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def add_repository(self, owner: str, name: str, url: str) -> int:
        """Add a repository to the database."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO repositories (owner, name, url)
                VALUES (?, ?, ?)
                """,
                (owner, name, url),
            )
            conn.commit()

            # Get the repository ID
            result = conn.execute(
                "SELECT id FROM repositories WHERE owner = ? AND name = ?",
                (owner, name),
            ).fetchone()
            return result["id"]

    def add_test(
        self,
        repo_id: int,
        node_id: str,
        file_path: str,
        class_name: Optional[str] = None,
        test_name: Optional[str] = None,
    ) -> int:
        """Add a test to the database."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tests (repo_id, node_id, file_path, class_name, test_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                (repo_id, node_id, file_path, class_name, test_name),
            )
            conn.commit()

            # Get the test ID
            result = conn.execute(
                "SELECT id FROM tests WHERE repo_id = ? AND node_id = ?",
                (repo_id, node_id),
            ).fetchone()
            return result["id"]

    def add_generator_usage(
        self,
        test_id: int,
        generator_name: str,
        count: int = 1,
        is_composite: bool = False,
        is_custom: bool = False,
    ):
        """Record generator usage for a test."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO generator_usage (test_id, generator_name, count, is_composite, is_custom)
                VALUES (?, ?, ?, ?, ?)
                """,
                (test_id, generator_name, count, is_composite, is_custom),
            )
            conn.commit()

    def add_property_type(
        self, test_id: int, property_type: str, confidence: float = 1.0
    ):
        """Record property type for a test."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO property_types (test_id, property_type, confidence)
                VALUES (?, ?, ?)
                """,
                (test_id, property_type, confidence),
            )
            conn.commit()

    def add_feature_usage(self, test_id: int, feature_name: str, count: int = 1):
        """Record feature usage for a test."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO feature_usage (test_id, feature_name, count)
                VALUES (?, ?, ?)
                """,
                (test_id, feature_name, count),
            )
            conn.commit()

    def add_test_code(
        self, test_id: int, source_code: str, ast_json: Optional[str] = None
    ):
        """Store test source code and optional AST."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO test_code (test_id, source_code, ast_json)
                VALUES (?, ?, ?)
                """,
                (test_id, source_code, ast_json),
            )
            conn.commit()

    def update_repository_status(
        self, repo_id: int, status: str, error_message: Optional[str] = None
    ):
        """Update repository processing status."""
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE repositories
                SET clone_status = ?, error_message = ?
                WHERE id = ?
                """,
                (status, error_message, repo_id),
            )
            conn.commit()

    def update_test_status(
        self, test_id: int, status: str, error_message: Optional[str] = None
    ):
        """Update test processing status."""
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE tests
                SET status = ?, error_message = ?
                WHERE id = ?
                """,
                (status, error_message, test_id),
            )
            conn.commit()

    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get overall analysis statistics."""
        with self.connection() as conn:
            stats = {}

            # Repository stats
            repo_stats = conn.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN clone_status = 'success' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN clone_status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN clone_status = 'pending' THEN 1 ELSE 0 END) as pending
                FROM repositories
            """
            ).fetchone()
            stats["repositories"] = dict(repo_stats)

            # Test stats
            test_stats = conn.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
                FROM tests
            """
            ).fetchone()
            stats["tests"] = dict(test_stats)

            # Generator usage stats
            gen_stats = conn.execute(
                """
                SELECT generator_name, COUNT(*) as count, SUM(count) as total_uses
                FROM generator_usage
                GROUP BY generator_name
                ORDER BY total_uses DESC
                LIMIT 20
            """
            ).fetchall()
            stats["top_generators"] = [dict(row) for row in gen_stats]

            # Property type distribution
            prop_stats = conn.execute(
                """
                SELECT property_type, COUNT(*) as count
                FROM property_types
                GROUP BY property_type
                ORDER BY count DESC
            """
            ).fetchall()
            stats["property_types"] = [dict(row) for row in prop_stats]

            # Feature usage
            feature_stats = conn.execute(
                """
                SELECT feature_name, COUNT(*) as test_count, SUM(count) as total_uses
                FROM feature_usage
                GROUP BY feature_name
                ORDER BY total_uses DESC
            """
            ).fetchall()
            stats["feature_usage"] = [dict(row) for row in feature_stats]

            return stats
