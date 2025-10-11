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

# experiment for things that have to be collected at property runtime, like timing,
# coverage, observability, etc.


class RuntimeExperiment(Experiment):
    name = "runtime"
    max_examples = 500

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS runtime_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                passed BOOLEAN,
                execution_time REAL,  -- seconds
                examples_count INTEGER,
                coverage TEXT,  -- JSON mapping: {"file_path": [line_numbers], ...}
                line_execution_counts TEXT,  -- JSON mapping: {"file_path": {"line_num": execution_count, ...}, ...}
                total_lines_covered INTEGER,  -- Sum of unique lines across all files
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE TABLE IF NOT EXISTS runtime_testcase (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                testcase_number INTEGER NOT NULL,  -- Order of test case execution
                coverage TEXT,  -- JSON mapping: {"file_path": [line_numbers], ...}
                cumulative_lines INTEGER,  -- Count of unique lines seen so far across all files
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE INDEX IF NOT EXISTS idx_runtime_summary ON runtime_summary(node_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_testcase ON runtime_testcase(node_id, testcase_number);
        """

    @staticmethod
    def run(node_id: str, timeout: int = 300, *, debug: bool) -> dict[str, Any]:
        from utils import parse_observability_data

        # Clear any previous observability data
        obs_dir = Path("/app/repo/.hypothesis/observed")
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
            cwd="/app/repo",
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

        coverage = data.get("coverage", {})
        test_cases = data.get("test_cases", [])

        # Calculate line execution counts from test cases
        line_counts_per_file = defaultdict(lambda: defaultdict(int))
        for test_case in test_cases:
            for file_path, lines in test_case["coverage"].items():
                for line in lines:
                    line_counts_per_file[file_path][str(line)] += 1

        # Convert coverage to serializable format (sets to lists)
        coverage_json = {
            file_path: list(lines) for file_path, lines in coverage.items()
        }

        # Convert line_counts_per_file to nested dict
        line_execution_counts_json = {
            file_path: dict(counts)
            for file_path, counts in line_counts_per_file.items()
        }

        # Calculate aggregate stats
        total_lines = sum(len(lines) for lines in coverage.values())

        # Store runtime summary with execution metadata and coverage
        db.execute(
            """
            INSERT INTO runtime_summary (
                node_id, passed, execution_time,
                examples_count, coverage, line_execution_counts,
                total_lines_covered
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                data.get("test_passed", False),
                data.get("execution_time"),
                None,
                json.dumps(coverage_json),
                json.dumps(line_execution_counts_json),
                total_lines,
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

                # Convert test case coverage to JSON (sets to lists)
                testcase_coverage_json = {
                    file_path: list(lines)
                    for file_path, lines in test_case["coverage"].items()
                }

                # Calculate total cumulative lines across all files
                cumulative_lines = sum(
                    len(lines) for lines in cumulative_coverage.values()
                )

                db.execute(
                    """
                    INSERT INTO runtime_testcase (
                        node_id, testcase_number, coverage, cumulative_lines
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        case_num,
                        json.dumps(testcase_coverage_json),
                        cumulative_lines,
                    ),
                )

        db.commit()

    @staticmethod
    def delete_data(db: Any, repo_name: str):
        db.delete_experiment_data(
            repo_name,
            [
                "runtime_summary",
                "runtime_testcase",
            ],
        )
