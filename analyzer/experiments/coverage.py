import shutil
import subprocess
from pathlib import Path
from typing import Any

try:
    from .experiment import Experiment
except ImportError:
    # When running as standalone module in container
    from experiment import Experiment


class CoverageExperiment(Experiment):
    name = "coverage"

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS node_coverage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                lines_covered TEXT,  -- JSON array of line numbers
                covered_lines INTEGER,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE TABLE IF NOT EXISTS node_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                passed BOOLEAN,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                execution_time REAL,  -- seconds
                examples_count INTEGER,
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE TABLE IF NOT EXISTS observability_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                timing_data TEXT,  -- JSON timing information
                example_data TEXT,  -- JSON examples data
                metadata TEXT,  -- JSON additional metadata
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE TABLE IF NOT EXISTS case_coverage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                case_number INTEGER NOT NULL,  -- Order of test case execution
                file_path TEXT NOT NULL,
                lines_covered TEXT,  -- JSON array of line numbers for this test case
                cumulative_lines TEXT,  -- JSON array of all unique lines seen so far
                cumulative_count INTEGER,  -- Count of unique lines seen so far
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_coverage_test ON node_coverage(node_id);
            CREATE INDEX IF NOT EXISTS idx_executions_test ON node_executions(node_id);
            CREATE INDEX IF NOT EXISTS idx_observability_test ON observability_data(node_id);
            CREATE INDEX IF NOT EXISTS idx_case_coverage ON case_coverage(node_id, case_number);
            CREATE INDEX IF NOT EXISTS idx_test_case_file ON case_coverage(node_id, file_path);
        """

    @staticmethod
    def run(file_path, node_id: str, timeout: int = 300) -> dict[str, Any]:
        from utils import parse_observability_data

        # Clear any previous observability data
        obs_dir = Path("/app/.hypothesis/observed")
        if obs_dir.exists():
            shutil.rmtree(obs_dir)

        result = subprocess.run(
            ["python", "-m", "pytest", node_id, "-xvs", "--tb=short"],
            capture_output=True,
            text=True,
            cwd="/app",
            timeout=timeout,
        )

        test_result = {
            "exit_code": result.returncode,
            "stdout": result.stdout[-5000:] if result.stdout else "",
            "stderr": result.stderr[-5000:] if result.stderr else "",
            "passed": result.returncode == 0,
        }

        # Parse observability data if it exists
        observability_data = {}
        if obs_dir.exists():
            observability_data = parse_observability_data(obs_dir)

        return {
            "test_passed": test_result.get("passed", False),
            "exit_code": test_result.get("exit_code", -1),
            "stdout": test_result.get("stdout", ""),
            "stderr": test_result.get("stderr", ""),
            "coverage": observability_data.get("coverage", {}),
            "test_cases": observability_data.get("test_cases", []),
            "timing": observability_data.get("timing", {}),
        }

    @staticmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        """Store coverage results to database."""
        import json

        with db.connection() as conn:
            # Store test execution results
            conn.execute(
                """
                INSERT INTO node_executions (node_id, passed, exit_code, stdout, stderr, execution_time, examples_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    data.get("test_passed", False),
                    data.get("exit_code", -1),
                    data.get("stdout", ""),
                    data.get("stderr", ""),
                    None,
                    None,
                ),
            )

            # Store aggregate coverage data
            coverage = data.get("coverage", {})
            if coverage:
                for file_path, lines in coverage.items():
                    lines_list = lines if isinstance(lines, list) else list(lines)
                    conn.execute(
                        """
                        INSERT INTO node_coverage (node_id, file_path, lines_covered, covered_lines)
                        VALUES (?, ?, ?, ?)
                        """,
                        (node_id, file_path, json.dumps(lines_list), len(lines_list)),
                    )

            # Store per-test-case coverage
            test_cases = data.get("test_cases", [])
            if test_cases:
                cumulative_coverage = {}

                for case_num, test_case in enumerate(test_cases):
                    if "coverage" in test_case and test_case["coverage"] is not None:
                        # Update cumulative coverage with lines from this test case
                        for file_path, lines in test_case["coverage"].items():
                            if file_path not in cumulative_coverage:
                                cumulative_coverage[file_path] = set()
                            cumulative_coverage[file_path].update(lines)

                        for file_path in cumulative_coverage:
                            cumulative_list = sorted(cumulative_coverage[file_path])
                            lines_this_case = test_case["coverage"].get(file_path, [])

                            conn.execute(
                                """
                                INSERT INTO case_coverage (
                                    node_id, case_number, file_path, lines_covered,
                                    cumulative_lines, cumulative_count
                                )
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    node_id,
                                    case_num,
                                    file_path,
                                    json.dumps(lines_this_case),
                                    json.dumps(cumulative_list),
                                    len(cumulative_list),
                                ),
                            )

            # Store observability metadata
            timing = data.get("timing", {})
            if timing:
                conn.execute(
                    """
                    INSERT INTO observability_data (node_id, timing_data, example_data, metadata)
                    VALUES (?, ?, ?, ?)
                    """,
                    (node_id, json.dumps(timing), None, None),
                )

            conn.commit()

    @staticmethod
    def delete_data(db: Any, owner: str, name: str):
        with db.connection() as conn:
            result = conn.execute(
                "SELECT id FROM repositories WHERE owner = ? AND name = ?",
                (owner, name),
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
                tables = [
                    "node_executions",
                    "node_coverage",
                    "case_coverage",
                    "observability_data",
                ]

                for table in tables:
                    conn.execute(
                        f"DELETE FROM {table} WHERE node_id IN ({placeholders})",
                        node_id_list,
                    )

            conn.commit()
