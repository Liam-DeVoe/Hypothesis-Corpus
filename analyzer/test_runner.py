"""
Test runner module for executing tests in isolated Docker containers.
"""

import json
import logging
import shutil
import subprocess
import tarfile
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional

import docker

logger = logging.getLogger(__name__)


class TestRunner:
    """Test runner for analyzing repositories in Docker containers."""

    RUNNER_TIMEOUT = 60 * 60  # 1 hour timeout

    def __init__(
        self, docker_image: str = "pbt-analyzer:latest", worker_id: Optional[int] = None
    ):
        """Initialize the test runner."""
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.worker_id = worker_id

    def clone_repository(self, repo_url: str, target_dir: Path) -> bool:
        """Clone a repository to the target directory."""
        try:
            # Construct GitHub URL if just owner/repo is provided
            if not repo_url.startswith(("http://", "https://", "git@")):
                repo_url = f"https://github.com/{repo_url}.git"

            logger.info(f"[w{self.worker_id}][{repo_url}] Cloning repository")
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(target_dir)],
                capture_output=True,
                text=True,
                check=True,
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(
                f"[w{self.worker_id}][{repo_url}] Failed to clone repository: {e.stderr}"
            )
            return False
        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{repo_url}] Failed to clone repository: {e}"
            )
            return False

    def setup_environment(
        self, work_dir: Path, requirements: str, node_ids: List[str]
    ) -> bool:
        """Set up environment by copying analysis module and config."""
        try:
            # Write requirements to file if provided
            if requirements:
                req_file = work_dir / "requirements.txt"
                req_file.write_text(requirements)

            # Copy the container analysis module
            import analyzer.container_analysis as container_analysis

            analysis_module_path = Path(container_analysis.__file__)
            shutil.copy(analysis_module_path, work_dir / "container_analysis.py")

            # Write configuration for the container
            config = {"node_ids": node_ids, "repo_dir": "/app"}
            config_file = work_dir / "config.json"
            config_file.write_text(json.dumps(config, indent=2))

            return True
        except Exception as e:
            logger.error(f"[w{self.worker_id}][unknown] Failed to setup environment: {e}")
            return False

    def extract_test_code(self, repo_dir: Path, node_id: str) -> Optional[str]:
        """Extract the source code of a specific test."""
        try:
            # Parse node_id (format: path/to/test.py::TestClass::test_method)
            parts = node_id.split("::")
            file_path = repo_dir / parts[0]

            if not file_path.exists():
                logger.error(f"[w{self.worker_id}][unknown] Test file not found: {file_path}")
                return None

            # Read the entire file content
            # In a production implementation, you could extract just the specific test
            return file_path.read_text()

        except Exception as e:
            logger.error(f"[w{self.worker_id}][unknown] Failed to extract test code: {e}")
            return None

    def run_in_container(
        self, repo_name: str, work_dir: Path, node_ids: List[str]
    ) -> Dict[str, any]:
        """Run tests in container by copying files instead of mounting (avoids Mac penalty)."""
        try:
            logger.info(
                f"[w{self.worker_id}][{repo_name}] Running container analysis"
            )

            # Create tar archive of the work directory
            start_tar = time.time()
            tar_stream = BytesIO()
            with tarfile.open(fileobj=tar_stream, mode="w") as tar:
                # Add all files from work_dir as /app in the container
                for item in work_dir.iterdir():
                    tar.add(item, arcname=f"/app/{item.name}")
            tar_stream.seek(0)
            tar_time = time.time() - start_tar
            logger.info(f"[w{self.worker_id}][{repo_name}] Created tar archive in {tar_time:.3f}s")

            # Create container WITHOUT volumes (no Mac penalty!)
            container = self.docker_client.containers.create(
                self.docker_image,
                command=["python", "/app/container_analysis.py"],
                environment={
                    "HYPOTHESIS_EXPERIMENTAL_OBSERVABILITY": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                mem_limit="2g",
                security_opt=["no-new-privileges"],
            )

            try:
                # Copy files into container
                start_copy = time.time()
                container.put_archive("/", tar_stream.read())
                copy_time = time.time() - start_copy
                logger.info(
                    f"[w{self.worker_id}][{repo_name}] Copied files into container in {copy_time:.3f}s"
                )

                # Start container
                container.start()

                # Wait for completion
                result = container.wait(timeout=self.RUNNER_TIMEOUT)

                # Get logs
                logs = container.logs(stdout=True, stderr=True).decode("utf-8")

                # Log the container output for debugging
                logger.debug(
                    f"[w{self.worker_id}][{repo_name}] Container logs:\n{logs}"
                )
                logger.debug(
                    f"[w{self.worker_id}][{repo_name}] Container exit code: {result.get('StatusCode', 'unknown')}"
                )

                # Extract and log timing information
                for line in logs.split("\n"):
                    if "[TIMING]" in line:
                        logger.info(f"[w{self.worker_id}][{repo_name}] {line.strip()}")

                # Extract results from container
                start_extract = time.time()
                try:
                    # Get results.json from container
                    bits, _ = container.get_archive("/app/results.json")
                    tar_stream = BytesIO()
                    for chunk in bits:
                        tar_stream.write(chunk)
                    tar_stream.seek(0)

                    # Extract from tar
                    with tarfile.open(fileobj=tar_stream) as tar:
                        results_file = tar.extractfile("results.json")
                        if results_file:
                            results = json.loads(results_file.read().decode("utf-8"))
                            extract_time = time.time() - start_extract
                            logger.info(
                                f"[w{self.worker_id}][{repo_name}] Extracted results in {extract_time:.3f}s"
                            )
                            return results
                        else:
                            logger.error(
                                f"[w{self.worker_id}][{repo_name}] Could not extract results.json from tar"
                            )
                            return {"error": "Could not extract results", "logs": logs}

                except docker.errors.NotFound as e:
                    logger.error(
                        f"[w{self.worker_id}][{repo_name}] No results file generated: {e}"
                    )
                    logger.debug(f"[w{self.worker_id}][{repo_name}] Container logs:\n{logs}")
                    return {"error": "No results generated", "logs": logs}
                except Exception as e:
                    logger.error(
                        f"[w{self.worker_id}][{repo_name}] Error extracting results: {e}"
                    )
                    logger.debug(
                        f"[w{self.worker_id}][{repo_name}] Error type: {type(e).__name__}, message: {str(e)}"
                    )
                    return {"error": f"Failed to extract results: {str(e)}"}

            finally:
                # Clean up container
                container.remove(force=True)

            # If we get here without returning, something went wrong
            return {"error": "Unknown error during container execution"}

        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{repo_name}] Container execution failed: {e}"
            )
            return {"error": str(e)}

    def process_repository(
        self, repo_name: str, node_ids: List[str], requirements: str
    ) -> Dict[str, any]:
        """Process a complete repository."""
        work_dir = None
        try:
            # Create temporary working directory
            work_dir = Path(
                tempfile.mkdtemp(prefix=f"pbt_analysis_{repo_name.replace('/', '_')}_")
            )

            # Clone repository
            if not self.clone_repository(repo_name, work_dir / "repo"):
                return {"error": "Failed to clone repository"}

            # Setup environment
            repo_dir = work_dir / "repo"
            if not self.setup_environment(repo_dir, requirements, node_ids):
                return {"error": "Failed to setup environment"}

            # Run analysis in container
            results = self.run_in_container(repo_name, repo_dir, node_ids)
            # Check if results is valid
            if results is None:
                return {"error": "No results returned from container"}

            if "error" in results:
                return results  # Return error as-is

            # Extract test code for each node_id
            for node_id in node_ids:
                code = self.extract_test_code(repo_dir, node_id)
                if code and node_id in results:
                    results[node_id]["source_code"] = code

            return results

        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{repo_name}] Failed to process repository: {e}"
            )
            return {"error": str(e)}
        finally:
            # Clean up temporary directory
            if work_dir and work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)
