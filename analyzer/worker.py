"""
Worker module for parallel processing of repositories.
"""

import json
import logging
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Process, Queue
from typing import Dict, List, Optional

from .analysis import PropertyAnalyzer
from .database import Database
from .test_runner import TestRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkItem:
    """Represents a repository to process."""

    repo_name: str
    node_ids: List[str]
    requirements: str
    repo_id: Optional[int] = None


class Worker(Process):
    """Worker process for analyzing repositories."""

    def __init__(
        self,
        worker_id: int,
        task_queue: Queue,
        result_queue: Queue,
        db_path: str,
        docker_image: str = "pbt-analyzer:latest",
    ):
        """Initialize worker process."""
        super().__init__()
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.db_path = db_path
        self.docker_image = docker_image
        self.daemon = True

    def run(self):
        """Main worker loop."""
        logger.info(f"[w{self.worker_id}] Worker started")

        # Initialize components in the worker process
        db = Database(self.db_path)
        test_runner = TestRunner(self.docker_image, worker_id=self.worker_id)
        analyzer = PropertyAnalyzer()

        while True:
            try:
                # Get next work item (timeout after 5 seconds)
                work_item = self.task_queue.get(timeout=5)

                if work_item is None:  # Poison pill to stop worker
                    logger.info(f"[w{self.worker_id}] Worker stopping")
                    break

                logger.info(f"[w{self.worker_id}][{work_item.repo_name}] Processing")

                # Process the repository
                result = self._process_repository(work_item, db, test_runner, analyzer)

                # Send result back
                self.result_queue.put(
                    {
                        "worker_id": self.worker_id,
                        "repo_name": work_item.repo_name,
                        "success": result.get("success", False),
                        "data": result,
                    }
                )

            except mp.queues.Empty:
                continue  # No work available, keep waiting
            except Exception as e:
                repo_name = (
                    work_item.repo_name if "work_item" in locals() else "unknown"
                )
                logger.error(f"[w{self.worker_id}][{repo_name}] Error: {e}")
                logger.error(traceback.format_exc())

                # Send error result
                if "work_item" in locals():
                    self.result_queue.put(
                        {
                            "worker_id": self.worker_id,
                            "repo_name": work_item.repo_name,
                            "success": False,
                            "error": str(e),
                        }
                    )

    def _process_repository(
        self,
        work_item: WorkItem,
        db: Database,
        test_runner: TestRunner,
        analyzer: PropertyAnalyzer,
    ) -> Dict:
        """Process a single repository."""
        try:
            # Delete any existing data for this repository before processing
            owner, name = work_item.repo_name.split("/")
            db.delete_repository_data(owner, name)

            # Add repository to database
            work_item.repo_id = db.add_repository(
                owner, name, f"https://github.com/{work_item.repo_name}"
            )

            # Run tests in container and collect results
            results = test_runner.process_repository(
                work_item.repo_name, work_item.node_ids, work_item.requirements
            )

            if results is None:
                error_msg = "No results returned from test runner"
                db.update_repository_status(work_item.repo_id, "failed", error_msg)
                return {"success": False, "error": error_msg}

            if "error" in results:
                db.update_repository_status(
                    work_item.repo_id, "failed", results["error"]
                )
                return {"success": False, "error": results["error"]}

            # Process each test result
            tests_processed = 0
            tests_failed = 0

            for node_id, test_results in results.items():
                if node_id == "error":
                    continue

                # Parse node_id
                parts = node_id.split("::")
                file_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else None
                test_name = parts[2] if len(parts) > 2 else parts[-1]

                # Extract property text and GitHub permalink from results
                property_text = test_results.get("property_text")
                github_permalink = test_results.get("github_permalink")

                # Add test to database with property text and permalink
                test_id = db.add_test(
                    work_item.repo_id,
                    node_id,
                    file_path,
                    class_name,
                    test_name,
                    property_text=property_text,
                    github_permalink=github_permalink,
                )

                if "error" in test_results:
                    db.update_test_status(test_id, "failed", test_results["error"])
                    tests_failed += 1
                    continue

                # Extract analysis and coverage data from new structure
                analysis_data = test_results.get("analysis", {})
                coverage_data = test_results.get("coverage", {})

                # Store test code if available (now in analysis data)
                if "source_code" in test_results:
                    source_code = test_results["source_code"]
                elif analysis_data:
                    # Try to get source from file path
                    source_code = None
                else:
                    source_code = None

                if source_code:
                    # Perform additional analysis on source code
                    enhanced_results = analyzer.analyze_source(source_code)
                    analysis_data.update(enhanced_results)

                    # Store in database
                    db.add_test_code(
                        test_id,
                        source_code,
                        json.dumps(analysis_data.get("ast", {})),
                    )

                # Store test execution results if available
                if coverage_data and "test_result" in coverage_data:
                    test_result = coverage_data["test_result"]
                    if test_result:
                        db.add_test_execution(
                            test_id,
                            passed=test_result.get("passed", False),
                            exit_code=test_result.get("exit_code", -1),
                            stdout=test_result.get("stdout", ""),
                            stderr=test_result.get("stderr", ""),
                        )

                # Store coverage information if available
                if coverage_data and "observability_data" in coverage_data:
                    obs_data = coverage_data["observability_data"]

                    # Store aggregate coverage data for each file
                    if "coverage" in obs_data and obs_data["coverage"] is not None:
                        for cov_file_path, lines in obs_data["coverage"].items():
                            db.add_test_coverage(
                                test_id,
                                cov_file_path,
                                lines if isinstance(lines, list) else list(lines),
                            )

                    # Store per-test-case coverage for cumulative tracking
                    if "test_cases" in obs_data:
                        # Track cumulative coverage per file
                        cumulative_coverage = {}

                        for case_num, test_case in enumerate(obs_data["test_cases"]):
                            if (
                                "coverage" in test_case
                                and test_case["coverage"] is not None
                            ):
                                for file_path, lines in test_case["coverage"].items():
                                    # Initialize cumulative set for this file if needed
                                    if file_path not in cumulative_coverage:
                                        cumulative_coverage[file_path] = set()

                                    # Add this test case's lines to cumulative
                                    cumulative_coverage[file_path].update(lines)

                                    # Store this test case's coverage with cumulative total
                                    db.add_test_case_coverage(
                                        test_id,
                                        case_num,
                                        file_path,
                                        lines,
                                        cumulative_coverage[file_path],
                                    )

                    # Store observability metadata
                    if any(k in obs_data for k in ["timing", "examples", "metadata"]):
                        db.add_observability_data(
                            test_id,
                            timing_data=obs_data.get("timing"),
                            example_data=obs_data.get("examples"),
                            metadata=obs_data.get("metadata"),
                        )

                # Store generator usage (from analysis)
                generators = analysis_data.get("generators", {}) or {}
                for gen_name, count in generators.items():
                    if gen_name in ["composite", "custom_strategies"]:
                        db.add_generator_usage(
                            test_id,
                            gen_name,
                            1,
                            is_composite=(gen_name == "composite"),
                            is_custom=(gen_name == "custom_strategies"),
                        )
                    else:
                        db.add_generator_usage(test_id, gen_name, count)

                # Store property types (from analysis)
                for prop_type in analysis_data.get("property_types", ["general"]):
                    db.add_property_type(test_id, prop_type)

                # Store feature usage (from analysis)
                features = analysis_data.get("features", {}) or {}
                for feature, count in features.items():
                    db.add_feature_usage(test_id, feature, count)

                db.update_test_status(test_id, "success")
                tests_processed += 1

            # Update repository status
            db.update_repository_status(work_item.repo_id, "success")

            return {
                "success": True,
                "tests_processed": tests_processed,
                "tests_failed": tests_failed,
                "results": results,
            }

        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{work_item.repo_name}] Error processing: {e}"
            )
            if work_item.repo_id:
                db.update_repository_status(work_item.repo_id, "failed", str(e))
            return {"success": False, "error": str(e)}


class WorkerPool:
    """Manages a pool of worker processes."""

    def __init__(
        self,
        num_workers: int = 4,
        db_path: str = "data/analysis.db",
        docker_image: str = "pbt-analyzer:latest",
    ):
        """Initialize worker pool."""
        self.num_workers = num_workers
        self.db_path = db_path
        self.docker_image = docker_image
        self.task_queue = mp.Queue(maxsize=100)
        self.result_queue = mp.Queue()
        self.workers = []
        self.results = []

    def start(self):
        """Start all worker processes."""
        logger.info(f"Starting worker pool with {self.num_workers} workers")

        for i in range(self.num_workers):
            worker = Worker(
                i, self.task_queue, self.result_queue, self.db_path, self.docker_image
            )
            worker.start()
            self.workers.append(worker)

    def submit(self, work_item: WorkItem):
        """Submit a work item to the pool."""
        self.task_queue.put(work_item)

    def submit_batch(self, work_items: List[WorkItem]):
        """Submit multiple work items."""
        for item in work_items:
            self.submit(item)

    def get_result(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """Get a result from the result queue."""
        try:
            result = self.result_queue.get(timeout=timeout)
            self.results.append(result)
            return result
        except mp.queues.Empty:
            return None

    def wait_for_completion(
        self, expected_count: int, timeout: int = 3600
    ) -> List[Dict]:
        """Wait for all tasks to complete."""
        logger.info(f"Waiting for {expected_count} tasks to complete")

        start_time = time.time()
        completed = 0

        while completed < expected_count:
            if time.time() - start_time > timeout:
                logger.error(
                    f"Timeout waiting for tasks (completed {completed}/{expected_count})"
                )
                break

            result = self.get_result(timeout=1)
            if result:
                completed += 1
                logger.info(
                    f"Completed {completed}/{expected_count}: {result['repo_name']} "
                    f"(success: {result['success']})"
                )

        return self.results

    def shutdown(self):
        """Shutdown all workers."""
        logger.info("Shutting down worker pool")

        # Send poison pills to all workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)

        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=10)
            if worker.is_alive():
                logger.warning(f"[w{worker.worker_id}] Did not stop gracefully")
                worker.terminate()

        logger.info("Worker pool shutdown complete")

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()
