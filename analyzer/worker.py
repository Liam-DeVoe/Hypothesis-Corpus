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
        docker_image: str = "pbt-analyzer:latest",
        experiment_name: str = "coverage",
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

        # Load experiment
        from .experiments import Experiment

        experiment = Experiment.experiments[self.experiment_name]
        logger.info(f"[w{self.worker_id}] Loaded experiment: {experiment.name}")

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
                    work_item, db, test_runner, experiment
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
        experiment,
    ) -> dict:
        """Process a single repository."""
        owner, name = work_item.repo_name.split("/")
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
            experiment.delete_data(db, owner, name)

            # Add repository to database
            with db.connection() as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO repositories (owner, name, url) VALUES (?, ?, ?)",
                    (owner, name, f"https://github.com/{work_item.repo_name}"),
                )
                conn.commit()
                result = conn.execute(
                    "SELECT id FROM repositories WHERE owner = ? AND name = ?",
                    (owner, name),
                ).fetchone()
                work_item.repo_id = result["id"]

            # Run tests in container with experiment name
            results = test_runner.process_repository(
                work_item.repo_name,
                work_item.node_ids,
                work_item.requirements,
                experiment_name=experiment.name,
            )

            if results is None:
                error_msg = "No results returned from test runner"
                with db.connection() as conn:
                    conn.execute(
                        "UPDATE repositories SET clone_status = ?, error_message = ? WHERE id = ?",
                        ("failed", error_msg, work_item.repo_id),
                    )
                    conn.commit()
                return {"success": False, "error": error_msg}

            if "error" in results:
                with db.connection() as conn:
                    conn.execute(
                        "UPDATE repositories SET clone_status = ?, error_message = ? WHERE id = ?",
                        ("failed", results["error"], work_item.repo_id),
                    )
                    conn.commit()
                return {"success": False, "error": results["error"]}

            # Process each node result using the experiment
            nodes_processed = 0
            nodes_failed = 0

            for node_id, test_results in results.items():
                if node_id == "error":
                    continue

                # Parse node_id
                parts = node_id.split("::")
                file_path = parts[0]
                class_name = parts[1] if len(parts) > 1 else None
                node_name = parts[2] if len(parts) > 2 else parts[-1]

                # Add node to database
                with db.connection() as conn:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO nodes (repo_id, node_id, file_path, class_name, node_name)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            work_item.repo_id,
                            node_id,
                            file_path,
                            class_name,
                            node_name,
                        ),
                    )
                    conn.commit()
                    result = conn.execute(
                        "SELECT id FROM nodes WHERE repo_id = ? AND node_id = ?",
                        (work_item.repo_id, node_id),
                    ).fetchone()
                    node_db_id = result["id"]

                if "error" in test_results:
                    with db.connection() as conn:
                        conn.execute(
                            "UPDATE nodes SET status = ?, error_message = ? WHERE id = ?",
                            ("failed", test_results["error"], node_db_id),
                        )
                        conn.commit()
                    nodes_failed += 1
                    continue

                try:
                    # Extract experiment data (now just a dict)
                    # For coverage experiment, the data is under the "coverage" key
                    experiment_data = test_results.get("coverage", {})

                    if not experiment_data:
                        with db.connection() as conn:
                            conn.execute(
                                "UPDATE nodes SET status = ?, error_message = ? WHERE id = ?",
                                ("failed", "No experiment data returned", node_db_id),
                            )
                            conn.commit()
                        nodes_failed += 1
                        continue

                    # Store results using experiment
                    experiment.store_to_database(
                        db, work_item.repo_id, node_db_id, experiment_data
                    )

                    with db.connection() as conn:
                        conn.execute(
                            "UPDATE nodes SET status = ?, error_message = ? WHERE id = ?",
                            ("success", None, node_db_id),
                        )
                        conn.commit()
                    nodes_processed += 1

                except Exception as e:
                    logger.error(
                        f"[w{self.worker_id}][{work_item.repo_name}] Error "
                        f"processing {node_id}: {traceback.format_exception(e)}"
                    )
                    with db.connection() as conn:
                        conn.execute(
                            "UPDATE nodes SET status = ?, error_message = ? WHERE id = ?",
                            ("failed", str(e), node_db_id),
                        )
                        conn.commit()
                    nodes_failed += 1

            # Update repository status
            with db.connection() as conn:
                conn.execute(
                    "UPDATE repositories SET clone_status = ?, error_message = ? WHERE id = ?",
                    ("success", None, work_item.repo_id),
                )
                conn.commit()

            return {
                "success": True,
                "nodes_processed": nodes_processed,
                "nodes_failed": nodes_failed,
                "results": results,
            }

        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{work_item.repo_name}] Error processing: {e}"
            )
            if work_item.repo_id:
                with db.connection() as conn:
                    conn.execute(
                        "UPDATE repositories SET clone_status = ?, error_message = ? WHERE id = ?",
                        ("failed", str(e), work_item.repo_id),
                    )
                    conn.commit()
            return {"success": False, "error": str(e)}


class WorkerPool:
    """Manages a pool of worker processes."""

    def __init__(
        self,
        num_workers: int = 4,
        db_path: str = "data/analysis.db",
        docker_image: str = "pbt-analyzer:latest",
        experiment_name: str = "coverage",
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
