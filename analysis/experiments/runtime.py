import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from .experiment import Experiment
    from .utils import filepath_from_node
except ImportError:
    # When running as standalone module in container
    from experiment import Experiment
    from utils import filepath_from_node, subprocess_run


class RuntimeExperiment(Experiment):
    name = "runtime"
    max_examples = 500

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS runtime_coverage_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                lines_covered TEXT,  -- JSON array of line numbers
                covered_lines INTEGER,
                line_execution_counts TEXT,  -- JSON mapping: {"line_num": execution_count, ...}
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

            CREATE TABLE IF NOT EXISTS runtime_coverage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                testcase_number INTEGER NOT NULL,  -- Order of test case execution
                file_path TEXT NOT NULL,
                lines_covered TEXT,  -- JSON array of line numbers for this test case
                cumulative_count INTEGER,  -- Count of unique lines seen so far
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_coverage_test ON runtime_coverage_summary(node_id);
            CREATE INDEX IF NOT EXISTS idx_executions_test ON node_executions(node_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_coverage ON runtime_coverage(node_id, testcase_number);
            CREATE INDEX IF NOT EXISTS idx_test_case_file ON runtime_coverage(node_id, file_path);
        """

    @staticmethod
    def run(
        file_path, node_id: str, timeout: int = 300, *, debug: bool
    ) -> dict[str, Any]:
        from utils import parse_observability_data

        # Clear any previous observability data
        obs_dir = Path("/app/.hypothesis/observed")
        if obs_dir.exists():
            shutil.rmtree(obs_dir)

        pytest_args = [
            "python",
            "-m",
            "pytest",
            # optimization to only collect as much as we need to
            filepath_from_node(node_id),
            "--experiment-nodeid",
            node_id,
            "--pbt-max-examples",
            RuntimeExperiment.max_examples,
        ]

        if debug:
            pytest_args += ["-s", "-v"]

        result = subprocess_run(
            pytest_args,
            cwd="/app",
            timeout=timeout,
        )

        if debug or result.returncode != 0:
            if result.stdout:
                print("[RuntimeExperiment] Pytest stdout:", flush=True)
                print(result.stdout, flush=True)
            if result.stderr:
                print("[RuntimeExperiment] Pytest stderr:", flush=True)
                print(result.stderr, flush=True)

        assert result.returncode == 0

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

        timing_file = Path("/app/.hypothesis/execution_time.json")
        assert timing_file.exists()
        execution_time = json.loads(timing_file.read_text())["execution_time"]

        return {
            "test_passed": test_result.get("passed", False),
            "exit_code": test_result.get("exit_code", -1),
            "stdout": test_result.get("stdout", ""),
            "stderr": test_result.get("stderr", ""),
            "execution_time": execution_time,
            "coverage": observability_data.get("coverage", {}),
            "test_cases": observability_data.get("test_cases", []),
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
                    data.get("execution_time"),
                    None,
                ),
            )

            # Store aggregate coverage data
            coverage = data.get("coverage", {})
            test_cases = data.get("test_cases", [])

            if coverage:
                # Calculate line execution counts from test cases
                line_counts_per_file = defaultdict(lambda: defaultdict(int))
                for test_case in test_cases:
                    for file_path, lines in test_case["coverage"].items():
                        for line in lines:
                            line_counts_per_file[file_path][str(line)] += 1

                for file_path, lines in coverage.items():
                    lines = list(lines)
                    line_counts = dict(line_counts_per_file[file_path])

                    conn.execute(
                        """
                        INSERT INTO runtime_coverage_summary (node_id, file_path, lines_covered, covered_lines, line_execution_counts)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            node_id,
                            file_path,
                            json.dumps(lines),
                            len(lines),
                            json.dumps(line_counts),
                        ),
                    )

            # Store per-test-case coverage
            if test_cases:
                cumulative_coverage = {}

                for case_num, test_case in enumerate(test_cases):
                    # Update cumulative coverage with lines from this test case
                    for file_path, lines in test_case["coverage"].items():
                        if file_path not in cumulative_coverage:
                            cumulative_coverage[file_path] = set()
                        cumulative_coverage[file_path].update(lines)

                    for file_path, cumulative_list in cumulative_coverage.items():
                        lines_this_case = test_case["coverage"].get(file_path, [])

                        conn.execute(
                            """
                            INSERT INTO runtime_coverage (
                                node_id, testcase_number, file_path, lines_covered, cumulative_count
                            )
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                node_id,
                                case_num,
                                file_path,
                                json.dumps(lines_this_case),
                                len(cumulative_list),
                            ),
                        )

            conn.commit()

    @staticmethod
    def delete_data(db: Any, repo_name: str):
        db.delete_experiment_data(
            repo_name,
            [
                "node_executions",
                "runtime_coverage_summary",
                "runtime_coverage",
                "observability_data",
            ],
        )
