import json
import logging
import shutil
import subprocess
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path

import docker

logger = logging.getLogger(__name__)


class TestRunner:
    """Test runner for analyzing repositories in Docker containers."""

    RUNNER_TIMEOUT = 60 * 60  # 1 hour timeout

    def __init__(
        self, docker_image: str = "pbt-analyzer:latest", worker_id: int | None = None
    ):
        """Initialize the test runner."""
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.worker_id = worker_id

    def get_git_commit_hash(self, repo_dir: Path) -> str | None:
        """Get the current git commit hash of a repository."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except Exception as e:
            logger.error(f"[w{self.worker_id}] Failed to get git hash: {e}")
            return None

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
        self,
        work_dir: Path,
        requirements: str,
        node_ids: list[str],
        experiment_name: str = "coverage",
        *,
        debug: bool,
    ) -> bool:
        """Set up environment by copying experiment modules and config."""
        try:
            # Write requirements to file if provided
            if requirements:
                req_file = work_dir / "requirements.txt"
                req_file.write_text(requirements)

            # Copy experiment modules that will run in the container
            import analyzer.experiments as experiments_package

            experiments_dir = Path(experiments_package.__file__).parent

            # Always copy shared helpers and runner
            shutil.copy(
                experiments_dir / "utils.py",
                work_dir / "utils.py",
            )
            shutil.copy(
                experiments_dir / "runner.py",
                work_dir / "runner.py",
            )

            # Always copy base experiment module
            shutil.copy(
                experiments_dir / "experiment.py",
                work_dir / "experiment.py",
            )

            # Copy the specific experiment module
            experiment_file = experiments_dir / f"{experiment_name}.py"
            assert experiment_file.exists()
            shutil.copy(experiment_file, work_dir / f"{experiment_name}.py")

            import analyzer.pytest_pbt_analysis as pbt_package

            pbt_source_dir = Path(pbt_package.__file__).parent
            pbt_dest_dir = work_dir / "pytest_pbt_analysis"
            shutil.copytree(pbt_source_dir, pbt_dest_dir)

            # Write configuration for the container
            config = {
                "node_ids": node_ids,
                "repo_dir": "/app",
                "experiment_name": experiment_name,
                "debug": debug,
            }
            config_file = work_dir / "config.json"
            config_file.write_text(json.dumps(config, indent=2))

            return True
        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][unknown] Failed to setup environment: {e}"
            )
            return False

    def run_in_container(
        self, repo_name: str, work_dir: Path, node_ids: list[str], debug: bool
    ) -> dict[str, any]:
        """Run tests in container by copying files instead of mounting (avoids Mac penalty)."""
        logger.info(f"[w{self.worker_id}][{repo_name}] Running container analysis")

        tar_stream = BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            # Add all files from work_dir as /app in the container
            for item in work_dir.iterdir():
                tar.add(item, arcname=f"/app/{item.name}")
        tar_stream.seek(0)

        secrets_path = Path(__file__).parent.parent / "secrets.json"
        with open(secrets_path) as f:
            secrets = json.load(f)

        environment = {
            "HYPOTHESIS_EXPERIMENTAL_OBSERVABILITY": "1",
            "CLAUDE_CODE_OAUTH_TOKEN": secrets["claude_code_oauth_token"],
        }

        container = self.docker_client.containers.create(
            self.docker_image,
            command=["python", "/app/runner.py"],
            environment=environment,
            mem_limit="2g",
            security_opt=["no-new-privileges"],
        )
        container.put_archive("/", tar_stream.read())

        container.start()
        result = container.wait(timeout=self.RUNNER_TIMEOUT)
        logs = container.logs(stdout=True, stderr=True).decode("utf-8")
        if debug:
            logger.info(f"[w{self.worker_id}][{repo_name}] Container logs:\n{logs}")
            logger.info(
                f"[w{self.worker_id}][{repo_name}] Container exit code: {result.get('StatusCode', 'unknown')}"
            )

        bits, _ = container.get_archive("/app/results.json")
        tar_stream = BytesIO()
        for chunk in bits:
            tar_stream.write(chunk)
        tar_stream.seek(0)

        try:
            with tarfile.open(fileobj=tar_stream) as tar:
                results_file = tar.extractfile("results.json")
                if results_file:
                    results = json.loads(results_file.read().decode("utf-8"))
                    return results
                else:
                    logger.error(
                        f"[w{self.worker_id}][{repo_name}] Could not extract results.json from tar"
                    )
                    return {"error": "Could not extract results", "logs": logs}
        finally:
            container.remove(force=True)

    def process_repository(
        self,
        repo_name: str,
        node_ids: list[str],
        requirements: str,
        experiment_name: str = "coverage",
        *,
        debug: bool,
    ) -> dict[str, any]:
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
            if not self.setup_environment(
                repo_dir, requirements, node_ids, experiment_name, debug=debug
            ):
                return {"error": "Failed to setup environment"}

            results = self.run_in_container(repo_name, repo_dir, node_ids, debug)
            if results is None:
                return {"error": "No results returned from container"}

            if "error" in results:
                return results

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
