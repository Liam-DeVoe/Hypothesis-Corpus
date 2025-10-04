"""
Database module for storing and retrieving analysis results.
"""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

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
                    property_text TEXT,  -- Source code of the property
                    github_permalink TEXT,  -- GitHub permalink to the property
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

                -- Coverage information for tests
                CREATE TABLE IF NOT EXISTS test_coverage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    lines_covered TEXT,  -- JSON array of line numbers
                    covered_lines INTEGER,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );

                -- Test execution results
                CREATE TABLE IF NOT EXISTS test_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    passed BOOLEAN,
                    exit_code INTEGER,
                    stdout TEXT,
                    stderr TEXT,
                    execution_time REAL,  -- seconds
                    examples_count INTEGER,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );

                -- Observability metadata
                CREATE TABLE IF NOT EXISTS observability_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    timing_data TEXT,  -- JSON timing information
                    example_data TEXT,  -- JSON examples data
                    metadata TEXT,  -- JSON additional metadata
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );

                -- Per-test-case coverage tracking for cumulative analysis
                CREATE TABLE IF NOT EXISTS test_case_coverage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_id INTEGER NOT NULL,
                    case_number INTEGER NOT NULL,  -- Order of test case execution
                    file_path TEXT NOT NULL,
                    lines_covered TEXT,  -- JSON array of line numbers for this test case
                    cumulative_lines TEXT,  -- JSON array of all unique lines seen so far
                    cumulative_count INTEGER,  -- Count of unique lines seen so far
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (test_id) REFERENCES tests(id)
                );

                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_tests_repo ON tests(repo_id);
                CREATE INDEX IF NOT EXISTS idx_generators_test ON generator_usage(test_id);
                CREATE INDEX IF NOT EXISTS idx_properties_test ON property_types(test_id);
                CREATE INDEX IF NOT EXISTS idx_features_test ON feature_usage(test_id);
                CREATE INDEX IF NOT EXISTS idx_repos_status ON repositories(clone_status);
                CREATE INDEX IF NOT EXISTS idx_tests_status ON tests(status);
                CREATE INDEX IF NOT EXISTS idx_coverage_test ON test_coverage(test_id);
                CREATE INDEX IF NOT EXISTS idx_executions_test ON test_executions(test_id);
                CREATE INDEX IF NOT EXISTS idx_observability_test ON observability_data(test_id);
                CREATE INDEX IF NOT EXISTS idx_test_case_coverage ON test_case_coverage(test_id, case_number);
                CREATE INDEX IF NOT EXISTS idx_test_case_file ON test_case_coverage(test_id, file_path);
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
        class_name: str | None = None,
        test_name: str | None = None,
        property_text: str | None = None,
        github_permalink: str | None = None,
    ) -> int:
        """Add a test to the database."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tests (repo_id, node_id, file_path, class_name, test_name, property_text, github_permalink)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    repo_id,
                    node_id,
                    file_path,
                    class_name,
                    test_name,
                    property_text,
                    github_permalink,
                ),
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
        self, test_id: int, source_code: str, ast_json: str | None = None
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
        self, repo_id: int, status: str, error_message: str | None = None
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
        self, test_id: int, status: str, error_message: str | None = None
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

    def add_test_coverage(
        self,
        test_id: int,
        file_path: str,
        lines_covered: list,
    ):
        """Store coverage information for a test."""
        import json

        covered_lines = len(lines_covered) if lines_covered else 0

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO test_coverage (test_id, file_path, lines_covered, covered_lines)
                VALUES (?, ?, ?, ?)
                """,
                (
                    test_id,
                    file_path,
                    json.dumps(lines_covered),
                    covered_lines,
                ),
            )
            conn.commit()

    def add_test_execution(
        self,
        test_id: int,
        passed: bool,
        exit_code: int,
        stdout: str = "",
        stderr: str = "",
        execution_time: float | None = None,
        examples_count: int | None = None,
    ):
        """Store test execution results."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO test_executions (test_id, passed, exit_code, stdout, stderr,
                                           execution_time, examples_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    test_id,
                    passed,
                    exit_code,
                    stdout,
                    stderr,
                    execution_time,
                    examples_count,
                ),
            )
            conn.commit()

    def add_observability_data(
        self,
        test_id: int,
        timing_data: dict | None = None,
        example_data: list | None = None,
        metadata: dict | None = None,
    ):
        """Store observability metadata."""
        import json

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO observability_data (test_id, timing_data, example_data, metadata)
                VALUES (?, ?, ?, ?)
                """,
                (
                    test_id,
                    json.dumps(timing_data) if timing_data else None,
                    json.dumps(example_data) if example_data else None,
                    json.dumps(metadata) if metadata else None,
                ),
            )
            conn.commit()

    def add_test_case_coverage(
        self,
        test_id: int,
        case_number: int,
        file_path: str,
        lines_covered: list,
        cumulative_lines: set,
    ):
        """Store per-test-case coverage with cumulative tracking."""
        import json

        cumulative_list = sorted(cumulative_lines)

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO test_case_coverage (
                    test_id, case_number, file_path, lines_covered,
                    cumulative_lines, cumulative_count
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    test_id,
                    case_number,
                    file_path,
                    json.dumps(lines_covered),
                    json.dumps(cumulative_list),
                    len(cumulative_list),
                ),
            )
            conn.commit()

    def delete_repository_data(self, owner: str, name: str):
        """Delete all data associated with a repository.

        This includes:
        - Repository record
        - All tests for this repository
        - All related data (generators, properties, features, code, coverage, executions, etc.)
        """
        with self.connection() as conn:
            # Get repository ID
            result = conn.execute(
                "SELECT id FROM repositories WHERE owner = ? AND name = ?",
                (owner, name),
            ).fetchone()

            if not result:
                logger.debug(f"No existing data found for {owner}/{name}")
                return

            repo_id = result["id"]

            # Get all test IDs for this repository
            test_ids = conn.execute(
                "SELECT id FROM tests WHERE repo_id = ?", (repo_id,)
            ).fetchall()
            test_id_list = [row["id"] for row in test_ids]

            if test_id_list:
                # Delete all related data for these tests
                # Using parameterized queries with proper placeholders
                placeholders = ",".join("?" * len(test_id_list))

                # Delete from all dependent tables
                tables_to_clean = [
                    "generator_usage",
                    "property_types",
                    "feature_usage",
                    "test_code",
                    "test_coverage",
                    "test_executions",
                    "observability_data",
                    "test_case_coverage",
                ]

                for table in tables_to_clean:
                    conn.execute(
                        f"DELETE FROM {table} WHERE test_id IN ({placeholders})",
                        test_id_list,
                    )

                # Delete tests
                conn.execute(
                    f"DELETE FROM tests WHERE id IN ({placeholders})", test_id_list
                )

            # Delete test runner info
            conn.execute("DELETE FROM test_runners WHERE repo_id = ?", (repo_id,))

            # Delete repository
            conn.execute("DELETE FROM repositories WHERE id = ?", (repo_id,))

            conn.commit()
        logger.info(f"[{owner}/{name}] deleted all data")

    def get_analysis_stats(self) -> dict[str, Any]:
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
