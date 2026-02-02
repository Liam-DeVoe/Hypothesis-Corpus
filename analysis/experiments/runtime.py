import json
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
                status TEXT,  -- "passed", "failed", or "skipped"
                execution_time REAL,  -- seconds
                error_message TEXT,  -- Error message if test failed
                count_test_cases INTEGER,
                coverage TEXT,  -- JSON mapping: {"file_path": [line_numbers], ...}
                line_execution_counts TEXT,  -- JSON mapping: {"file_path": {"line_num": execution_count, ...}, ...}
                total_lines_covered INTEGER,  -- Sum of unique lines across all files
                settings TEXT,  -- JSON mapping of Hypothesis settings
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE TABLE IF NOT EXISTS runtime_testcase (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                testcase_number INTEGER NOT NULL,  -- Order of test case execution
                coverage TEXT,  -- JSON mapping: {"file_path": [line_numbers], ...}
                timing TEXT,  -- JSON: observation.timing
                predicates TEXT,  -- JSON: observation.predicates
                features TEXT,  -- JSON: observation.features
                data_status INTEGER,  -- observation.data_status
                status_reason TEXT,  -- observation.status_reason
                choices_size INTEGER,  -- choices_size(observation.metadata.choice_nodes)
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE INDEX IF NOT EXISTS idx_runtime_summary ON runtime_summary(node_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_testcase ON runtime_testcase(node_id, testcase_number);
        """

    @staticmethod
    def run(node_id: str, timeout: int = 300, *, debug: bool) -> dict[str, Any]:
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
        # 0 = test passed
        # 1 = test failed
        # 2 = test execution was interrupted by the user
        # 3 = internal error happened during test execution
        # 4 = pytest command line usage error
        # 5 = no tests were collected
        if result.returncode not in {0, 1}:
            raise RuntimeError(
                f"pytest exited with code {result.returncode}. nodeid: {node_id}\n"
                f"stdout: {result.stdout}\n\n stderr: {result.stderr}"
            )

        results_file = Path("/app/test_results.json")
        assert results_file.exists()
        return json.loads(results_file.read_text())

    @staticmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        if data["status"] == "error":
            db.execute(
                """
                INSERT INTO runtime_summary (node_id, status, error_message)
                VALUES (?, ?, ?)
                """,
                (node_id, data["status"], data.get("error_message", "")),
            )
            db.commit()
            return

        observations = data["observations"]

        # Calculate aggregate coverage and line execution counts from test cases
        aggregate_coverage = {}
        line_counts_per_file = defaultdict(lambda: defaultdict(int))

        for observation in observations:
            for file_path, lines in observation["coverage"].items():
                if file_path not in aggregate_coverage:
                    aggregate_coverage[file_path] = set()
                aggregate_coverage[file_path].update(lines)

                for line in lines:
                    line_counts_per_file[file_path][str(line)] += 1

        # Convert aggregate coverage sets to lists for JSON serialization
        coverage_json = {
            file_path: sorted(lines) for file_path, lines in aggregate_coverage.items()
        }

        # Convert line_counts_per_file to nested dict
        line_execution_counts_json = {
            file_path: dict(counts)
            for file_path, counts in line_counts_per_file.items()
        }

        db.execute(
            """
            INSERT INTO runtime_summary (
                node_id, status, execution_time, error_message,
                count_test_cases, coverage, line_execution_counts,
                total_lines_covered, settings
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                data["status"],
                data["execution_time"],
                data["error_message"],
                len(observations),
                json.dumps(coverage_json),
                json.dumps(line_execution_counts_json),
                sum(len(lines) for lines in aggregate_coverage.values()),
                json.dumps(data["settings"]),
            ),
        )

        for case_num, observation in enumerate(observations):
            # Convert test case coverage to JSON (sets to lists)
            testcase_coverage_json = {
                file_path: list(lines)
                for file_path, lines in observation["coverage"].items()
            }

            # round timing values to nanosecond precision (9 decimal places)
            # to reduce db size.
            #
            # time.perf_counter only has 1ns precision on linux, so this is
            # within the order of measurement error.
            timing = {
                key: round(value, 9) for key, value in observation["timing"].items()
            }

            metadata = observation["metadata"]
            predicates = metadata["predicates"]
            data_status = int(metadata["data_status"])

            features = observation["features"]
            status_reason = observation["status_reason"]
            choices_size = observation["choices_size"]

            db.execute(
                """
                INSERT INTO runtime_testcase (
                    node_id, testcase_number, coverage, timing, predicates, features,
                    data_status, status_reason, choices_size
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    case_num,
                    json.dumps(testcase_coverage_json),
                    json.dumps(timing),
                    json.dumps(predicates),
                    json.dumps(features),
                    data_status,
                    status_reason,
                    choices_size,
                ),
            )

        db.commit()

    @staticmethod
    def delete_data(db: Any, repo_id: int):
        node_ids = db.fetchall("SELECT id FROM core_node WHERE repo_id = ?", (repo_id,))
        node_id_list = [row["id"] for row in node_ids]

        if node_id_list:
            placeholders = ",".join("?" * len(node_id_list))
            db.execute(
                f"DELETE FROM runtime_summary WHERE node_id IN ({placeholders})",
                node_id_list,
            )
            db.execute(
                f"DELETE FROM runtime_testcase WHERE node_id IN ({placeholders})",
                node_id_list,
            )

        db.commit()
