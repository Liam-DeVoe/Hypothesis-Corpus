"""
Worker module for parallel processing of repositories.
"""

import logging
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Process, Queue

from .analysis import PropertyAnalyzer
from .database import Database
from .test_runner import TestRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkItem:
    """Represents a repository to process."""

    repo_name: str
    node_ids: list[str]
    requirements: str
    repo_id: int | None = None


class Worker(Process):
    """Worker process for analyzing repositories."""

    def __init__(
        self,
        worker_id: int,
        task_queue: Queue,
        result_queue: Queue,
        db_path: str,
        docker_image: str = "pbt-analyzer:latest",
        experiment_name: str = "all",
    ):
        """Initialize worker process."""
        super().__init__()
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.db_path = db_path
        self.docker_image = docker_image
        self.experiment_name = experiment_name
        self.daemon = True

    def run(self):
        """Main worker loop."""
        logger.info(f"[w{self.worker_id}] Worker started")

        # Initialize components in the worker process
        db = Database(self.db_path)
        test_runner = TestRunner(self.docker_image, worker_id=self.worker_id)
        analyzer = PropertyAnalyzer()

        # Load experiment
        from .experiments import get_experiment

        try:
            experiment = get_experiment(self.experiment_name)
            logger.info(f"[w{self.worker_id}] Loaded experiment: {experiment.name}")
        except ValueError as e:
            logger.error(f"[w{self.worker_id}] {e}")
            return

        while True:
            try:
                # Get next work item (timeout after 5 seconds)
                work_item = self.task_queue.get(timeout=5)

                if work_item is None:  # Poison pill to stop worker
                    logger.info(f"[w{self.worker_id}] Worker stopping")
                    break

                logger.info(f"[w{self.worker_id}][{work_item.repo_name}] Processing")

                # Process the repository
                result = self._process_repository(
                    work_item, db, test_runner, analyzer, experiment
                )

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
        experiment,
    ) -> dict:
        """Process a single repository."""
        try:
            # Delete any existing data for this repository before processing
            owner, name = work_item.repo_name.split("/")
            experiment.delete_data(db, owner, name)

            # Add repository to database
            work_item.repo_id = db.add_repository(
                owner, name, f"https://github.com/{work_item.repo_name}"
            )

            # Run tests in container with experiment name
            results = test_runner.process_repository(
                work_item.repo_name,
                work_item.node_ids,
                work_item.requirements,
                experiment_name=experiment.name,
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

            # Process each test result using the experiment
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

                # Use experiment to process and store results
                try:
                    # Import ExperimentResult for deserialization
                    from .experiments.base import ExperimentResult

                    # For "all" experiment, pass the whole dict (contains all sub-experiments)
                    # For individual experiments, extract their specific result
                    if experiment.name == "all":
                        # Process results through composite experiment
                        experiment_result = experiment.process_results(
                            node_id, test_results
                        )
                    else:
                        # Get the result dict from the appropriate key
                        result_key_map = {
                            "static": "analysis",
                            "coverage": "coverage",
                            "ast": "ast_data",
                        }
                        result_key = result_key_map.get(experiment.name)

                        if not result_key or result_key not in test_results:
                            db.update_test_status(
                                test_id,
                                "failed",
                                f"Missing result key '{result_key}' in test results",
                            )
                            tests_failed += 1
                            continue

                        # Deserialize the container result
                        container_result = ExperimentResult.from_dict(
                            test_results[result_key]
                        )

                        # Process results through experiment (pass-through for most, enhanced for AST)
                        experiment_result = experiment.process_results(
                            node_id, container_result
                        )

                    if not experiment_result.success:
                        db.update_test_status(
                            test_id, "failed", experiment_result.error
                        )
                        tests_failed += 1
                        continue

                    # Store results using experiment
                    experiment.store_to_database(
                        db, work_item.repo_id, test_id, experiment_result
                    )

                    db.update_test_status(test_id, "success")
                    tests_processed += 1

                except Exception as e:
                    logger.error(
                        f"[w{self.worker_id}][{work_item.repo_name}] Error "
                        f"processing {node_id}: {traceback.format_exception(e)}"
                    )
                    db.update_test_status(test_id, "failed", str(e))
                    tests_failed += 1

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
        experiment_name: str = "all",
    ):
        """Initialize worker pool."""
        self.num_workers = num_workers
        self.db_path = db_path
        self.docker_image = docker_image
        self.experiment_name = experiment_name
        self.task_queue = mp.Queue(maxsize=100)
        self.result_queue = mp.Queue()
        self.workers = []
        self.results = []

    def start(self):
        """Start all worker processes."""
        logger.info(f"Starting worker pool with {self.num_workers} workers")

        for i in range(self.num_workers):
            worker = Worker(
                i,
                self.task_queue,
                self.result_queue,
                self.db_path,
                self.docker_image,
                self.experiment_name,
            )
            worker.start()
            self.workers.append(worker)

    def submit(self, work_item: WorkItem):
        """Submit a work item to the pool."""
        self.task_queue.put(work_item)

    def submit_batch(self, work_items: list[WorkItem]):
        """Submit multiple work items."""
        for item in work_items:
            self.submit(item)

    def get_result(self, timeout: float | None = None) -> dict | None:
        """Get a result from the result queue."""
        try:
            result = self.result_queue.get(timeout=timeout)
            self.results.append(result)
            return result
        except mp.queues.Empty:
            return None

    def wait_for_completion(
        self, expected_count: int, timeout: int = 3600
    ) -> list[dict]:
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
