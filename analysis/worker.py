import logging
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass
from multiprocessing import Process, Queue

import requests

from .database import Database
from .test_runner import TestRunner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class WorkItem:
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
        docker_image: str,
        experiments: list[str],
        debug: bool,
    ):
        """Initialize worker process."""
        super().__init__()
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.db_path = db_path
        self.docker_image = docker_image
        self.experiments = experiments
        self.debug = debug
        self.daemon = True

    def run(self):
        """Main worker loop."""
        logger.info(f"[w{self.worker_id}] Worker started")

        # Initialize components in the worker process
        db = Database(db_path=self.db_path)
        test_runner = TestRunner(self.docker_image, worker_id=self.worker_id)

        # Load experiments
        from .experiments import Experiment

        experiments = [Experiment.experiments[name] for name in self.experiments]
        logger.info(
            f"[w{self.worker_id}] Loaded experiments: {[e.name for e in experiments]}"
        )

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
                    work_item, db, test_runner, experiments
                )

                # Send result back
                result_to_send = {
                    "worker_id": self.worker_id,
                    "repo_name": work_item.repo_name,
                    "success": result.get("success", False),
                    "data": result,
                }
                # Flatten error to top level if present
                if "error" in result:
                    result_to_send["error"] = result["error"]

                self.result_queue.put(result_to_send)

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
        experiments: list,
    ) -> dict:
        """Process a single repository."""
        try:
            try:
                response = requests.get(
                    f"https://api.github.com/repos/{work_item.repo_name}",
                    timeout=10,
                )
                if response.status_code != 200:
                    error_msg = f"Repository not found or not accessible (status {response.status_code})"
                    logger.warning(
                        f"[w{self.worker_id}][{work_item.repo_name}] {error_msg}"
                    )
                    return {"success": False, "error": error_msg}
            except requests.RequestException as e:
                error_msg = f"Failed to check repository: {str(e)}"
                logger.warning(
                    f"[w{self.worker_id}][{work_item.repo_name}] {error_msg}"
                )
                return {"success": False, "error": error_msg}

            # Delete any existing data for this repository before processing
            for experiment in experiments:
                experiment.delete_data(db, work_item.repo_name)

            result = db.fetchone(
                "SELECT id FROM core_repository WHERE full_name = ?",
                (work_item.repo_name,),
            )
            assert result
            work_item.repo_id = result["id"]

            # Run all experiments for this repository
            all_nodes_processed = 0
            all_nodes_failed = 0

            for experiment in experiments:
                logger.info(
                    f"[w{self.worker_id}][{work_item.repo_name}] Running experiment: {experiment.name}"
                )

                # Run tests in container with experiment name
                results = test_runner.process_repository(
                    work_item.repo_name,
                    work_item.node_ids,
                    work_item.requirements,
                    experiment_name=experiment.name,
                    debug=self.debug,
                )

                if results is None:
                    error_msg = f"No results returned from test runner for experiment {experiment.name}"
                    logger.error(
                        f"[w{self.worker_id}][{work_item.repo_name}] {error_msg}"
                    )
                    continue

                if "error" in results:
                    error_msg = (
                        f"Experiment {experiment.name} failed: {results['error']}"
                    )
                    logger.error(
                        f"[w{self.worker_id}][{work_item.repo_name}] {error_msg}"
                    )
                    continue

                # Process each node result using the experiment
                nodes_processed = 0
                nodes_failed = 0

                for node_id, test_results in results.items():
                    if node_id == "error":
                        continue

                    # Add node to database
                    db.execute(
                        """
                        INSERT OR IGNORE INTO core_node (repo_id, node_id)
                        VALUES (?, ?)
                        """,
                        (
                            work_item.repo_id,
                            node_id,
                        ),
                    )
                    db.commit()
                    result = db.fetchone(
                        "SELECT id FROM core_node WHERE repo_id = ? AND node_id = ?",
                        (work_item.repo_id, node_id),
                    )
                    node_db_id = result["id"]

                    if "error" in test_results:
                        error_msg = test_results["error"]
                        if "traceback" in test_results:
                            error_msg = f"{error_msg}\n\nTraceback:\n{test_results['traceback']}"

                        logger.error(
                            f"[w{self.worker_id}][{work_item.repo_name}] Node {node_id} failed: {error_msg}"
                        )

                        nodes_failed += 1
                        continue

                    try:
                        # Extract experiment data using the experiment name as key
                        experiment_data = test_results.get(experiment.name, {})

                        if not experiment_data:
                            logger.warning(
                                f"[w{self.worker_id}][{work_item.repo_name}] "
                                f"No data for {node_id} in experiment {experiment.name}"
                            )
                            nodes_failed += 1
                            continue

                        # Store results using experiment
                        experiment.store_to_database(
                            db, work_item.repo_id, node_db_id, experiment_data
                        )

                        nodes_processed += 1

                    except Exception as e:
                        logger.error(
                            f"[w{self.worker_id}][{work_item.repo_name}] Error "
                            f"processing {node_id} for {experiment.name}: {traceback.format_exception(e)}"
                        )
                        nodes_failed += 1

                # Track totals across experiments
                all_nodes_processed += nodes_processed
                all_nodes_failed += nodes_failed
                logger.info(
                    f"[w{self.worker_id}][{work_item.repo_name}] Experiment {experiment.name}: "
                    f"{nodes_processed} processed, {nodes_failed} failed"
                )

            return {
                "success": True,
                "nodes_processed": all_nodes_processed,
                "nodes_failed": all_nodes_failed,
            }

        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{work_item.repo_name}] Error processing: "
                f"{traceback.format_exception(e)}"
            )
            return {"success": False, "error": str(e)}


class WorkerPool:
    """Manages a pool of worker processes."""

    def __init__(
        self,
        *,
        num_workers: int,
        db_path: str,
        docker_image: str,
        experiments: list[str],
        debug: bool,
    ):
        self.num_workers = num_workers
        self.db_path = db_path
        self.docker_image = docker_image
        self.experiments = experiments
        self.debug = debug
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
                self.experiments,
                self.debug,
            )
            worker.start()
            self.workers.append(worker)

    def submit(self, work_item: WorkItem):
        """Submit a work item to the pool."""
        self.task_queue.put(work_item)

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
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
