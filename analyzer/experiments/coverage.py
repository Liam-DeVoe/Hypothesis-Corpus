"""
Coverage experiment: test execution with Hypothesis observability data.

This module contains both:
1. The Experiment class (for the main analysis system)
2. Container analysis functions (run inside Docker)
"""

import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

# When run in container, base module won't exist - need standalone copy
try:
    from .base import Experiment, ExperimentResult
except ImportError:
    # Running in container - use standalone version
    from dataclasses import dataclass, field
    from typing import Any as _Any

    @dataclass
    class ExperimentResult:  # type: ignore
        """Results from running an experiment."""

        node_id: str
        success: bool
        data: dict[str, _Any] = field(default_factory=dict)
        error: str | None = None

        def to_dict(self) -> dict[str, _Any]:
            return {
                "node_id": self.node_id,
                "success": self.success,
                "data": self.data,
                "error": self.error,
            }

    Experiment = object  # type: ignore


class CoverageExperiment(Experiment):
    """Executes tests and collects coverage via Hypothesis observability."""

    name = "coverage"

    def store_to_database(
        self, db: Any, repo_id: int, test_id: int, result: ExperimentResult
    ):
        """Store coverage results to database."""
        data = result.data

        # Store test execution results
        db.add_test_execution(
            test_id,
            passed=data.get("test_passed", False),
            exit_code=data.get("exit_code", -1),
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
        )

        # Store aggregate coverage data
        coverage = data.get("coverage", {})
        if coverage:
            for file_path, lines in coverage.items():
                db.add_test_coverage(
                    test_id,
                    file_path,
                    lines if isinstance(lines, list) else list(lines),
                )

        # Store per-test-case coverage
        test_cases = data.get("test_cases", [])
        if test_cases:
            cumulative_coverage = {}

            for case_num, test_case in enumerate(test_cases):
                if "coverage" in test_case and test_case["coverage"] is not None:
                    for file_path, lines in test_case["coverage"].items():
                        if file_path not in cumulative_coverage:
                            cumulative_coverage[file_path] = set()

                        cumulative_coverage[file_path].update(lines)

                        db.add_test_case_coverage(
                            test_id,
                            case_num,
                            file_path,
                            lines,
                            cumulative_coverage[file_path],
                        )

        # Store observability metadata
        timing = data.get("timing", {})
        if timing:
            db.add_observability_data(test_id, timing_data=timing)

    def delete_data(self, db: Any, owner: str, name: str):
        """Delete coverage data."""
        tables = [
            "test_executions",
            "test_coverage",
            "test_case_coverage",
            "observability_data",
        ]
        db.delete_experiment_data(owner, name, tables)


# =============================================================================
# Container Analysis Functions (run inside Docker)
# =============================================================================


def run_analysis(file_path, node_id: str) -> ExperimentResult:
    """Run a single test and collect coverage information.

    This function runs inside the Docker container.

    Args:
        node_id: Test node ID
        timeout: Timeout in seconds

    Returns:
        ExperimentResult with test results and coverage data
    """
    # Import helpers (will be copied to container)
    try:
        from container_helpers import parse_observability_data
    except ImportError:
        # Fallback for testing
        from .container_helpers import parse_observability_data

    try:
        # Clear any previous observability data
        obs_dir = Path("/app/.hypothesis/observed")
        if obs_dir.exists():
            shutil.rmtree(obs_dir)

        # Run the test with pytest
        cmd = ["python", "-m", "pytest", node_id, "-xvs", "--tb=short"]

        print(f"Starting pytest subprocess for {node_id}", flush=True)
        start_pytest = time.time()
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd="/app", timeout=timeout
        )
        pytest_time = time.time() - start_pytest
        print(f"[TIMING] Pytest execution: {pytest_time:.3f}s", flush=True)

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

        return ExperimentResult(
            node_id=node_id,
            success=True,
            data={
                "test_passed": test_result.get("passed", False),
                "exit_code": test_result.get("exit_code", -1),
                "stdout": test_result.get("stdout", ""),
                "stderr": test_result.get("stderr", ""),
                "coverage": observability_data.get("coverage", {}),
                "test_cases": observability_data.get("test_cases", []),
                "timing": observability_data.get("timing", {}),
            },
        )

    except subprocess.TimeoutExpired:
        return ExperimentResult(
            node_id=node_id,
            success=False,
            error=f"Test timed out after {timeout} seconds",
        )
    except Exception as e:
        return ExperimentResult(node_id=node_id, success=False, error=str(e))
