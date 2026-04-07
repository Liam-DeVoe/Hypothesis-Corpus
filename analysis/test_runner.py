import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import traceback
from io import BytesIO
from pathlib import Path

import docker

from analysis.collect.utils import CACHE_VOLUME_NAME

logger = logging.getLogger(__name__)


class TestRunner:
    """Test runner for analyzing repositories in Docker containers."""

    RUNNER_TIMEOUT = 60 * 60  # 1 hour timeout

    def __init__(
        self,
        docker_image: str = "hypothesis-corpus:latest",
        worker_id: int | None = None,
        container_id_queue=None,
    ):
        """Initialize the test runner."""
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.worker_id = worker_id
        self.container_id_queue = container_id_queue

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

    def clone_repository(
        self, repo_url: str, target_dir: Path, commit_hash: str
    ) -> bool:
        try:
            # Construct GitHub URL if just owner/repo is provided
            if not repo_url.startswith(("http://", "https://", "git@")):
                repo_url = f"https://github.com/{repo_url}.git"

            logger.info(f"[w{self.worker_id}][{repo_url}] Cloning repository")

            logger.info(
                f"[w{self.worker_id}][{repo_url}] Fetching commit {commit_hash[:7]} with depth 1"
            )
            # we want to pull down a specific commit from the remote. We'll
            # initialize an empty repo, then fetch that specific commit with --depth 1,
            # then reify the files by unpacking (checking out) the current git state.
            #
            # see https://stackoverflow.com/questions/31278902/how-to-
            # shallow-clone-a-specific-commit-with-depth-1.
            target_dir.mkdir(parents=True, exist_ok=True)

            subprocess.run(
                ["git", "init"],
                cwd=target_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            subprocess.run(
                ["git", "remote", "add", "origin", repo_url],
                cwd=target_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            subprocess.run(
                ["git", "fetch", "--depth", "1", "origin", commit_hash],
                cwd=target_dir,
                capture_output=True,
                text=True,
                check=True,
            )
            subprocess.run(
                ["git", "checkout", "FETCH_HEAD"],
                cwd=target_dir,
                capture_output=True,
                text=True,
                check=True,
            )

            # sanity check
            assert self.get_git_commit_hash(target_dir) == commit_hash, (
                self.get_git_commit_hash(target_dir),
                commit_hash,
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
        app_dir: Path,
        requirements: str,
        node_ids: list[str],
        experiment_name: str,
        *,
        debug: bool,
        repo_name: str,
        skip_run_repo: bool = False,
    ) -> bool:
        """Set up environment by copying experiment modules and config to app_dir."""
        # Write requirements to file if provided
        if requirements:
            req_file = app_dir / "requirements.txt"
            req_file.write_text(requirements)

        # Copy experiment modules that will run in the container
        import analysis.experiments as experiments_package

        experiments_dir = Path(experiments_package.__file__).parent

        # Always copy shared helpers and runner
        shutil.copy(
            experiments_dir / "utils.py",
            app_dir / "utils.py",
        )
        shutil.copy(
            experiments_dir / "runner.py",
            app_dir / "runner.py",
        )

        # Always copy base experiment module
        shutil.copy(
            experiments_dir / "experiment.py",
            app_dir / "experiment.py",
        )

        # Copy the specific experiment module
        experiment_file = experiments_dir / f"{experiment_name}.py"
        assert experiment_file.exists()
        shutil.copy(experiment_file, app_dir / f"{experiment_name}.py")

        import analysis.pytest_hypothesis_corpus as pbt_package

        pbt_source_dir = Path(pbt_package.__file__).parent
        pbt_dest_dir = app_dir / "pytest_hypothesis_corpus"
        shutil.copytree(pbt_source_dir, pbt_dest_dir)

        # Write configuration for the container
        config = {
            "node_ids": node_ids,
            "repo_dir": "/app/repo",
            "experiment_name": experiment_name,
            "debug": debug,
            "repo_name": repo_name,
            "skip_run_repo": skip_run_repo,
        }
        config_file = app_dir / "config.json"
        config_file.write_text(json.dumps(config, indent=2))

    def _extract_file_from_container(self, container, path: str) -> str | None:
        """Extract a file from the container. Returns content string or None."""
        try:
            bits, _ = container.get_archive(path)
            tar_stream = BytesIO()
            for chunk in bits:
                tar_stream.write(chunk)
            tar_stream.seek(0)
            with tarfile.open(fileobj=tar_stream) as tar:
                filename = os.path.basename(path)
                f = tar.extractfile(filename)
                if f:
                    return f.read().decode("utf-8")
        except Exception:
            return None
        return None

    def run_in_container(
        self, repo_name: str, work_dir: Path, node_ids: list[str], debug: bool
    ) -> dict[str, any]:
        """Run tests in container by copying files instead of mounting (avoids Mac penalty)."""
        logger.info(f"[w{self.worker_id}][{repo_name}] Running container analysis")

        tar_stream = BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            tar.add(work_dir / "app", arcname="/app")
            tar.add(work_dir / "repo", arcname="/app/repo")
        tar_stream.seek(0)

        secrets_path = Path(__file__).parent / "secrets.json"
        with open(secrets_path) as f:
            secrets = json.load(f)

        environment = {
            "CLAUDE_CODE_OAUTH_TOKEN": secrets["claude_code_oauth_token"],
            "UV_LINK_MODE": "copy",
        }

        container = self.docker_client.containers.create(
            self.docker_image,
            command=["python", "/app/runner.py"],
            environment=environment,
            mem_limit="2g",
            security_opt=["no-new-privileges"],
            volumes={CACHE_VOLUME_NAME: {"bind": "/root/.cache/uv", "mode": "rw"}},
        )
        container.put_archive("/", tar_stream.read())

        # Publish container ID so it can be stopped during shutdown
        if self.container_id_queue is not None:
            self.container_id_queue.put(
                {"container_id": container.id, "worker_id": self.worker_id}
            )

        container.start()

        if debug:
            logger.info(
                f"[w{self.worker_id}][{repo_name}] Container started, streaming logs..."
            )
            for line in container.logs(stream=True, stdout=True, stderr=True):
                logger.info(
                    f"[w{self.worker_id}][{repo_name}] {line.decode('utf-8').rstrip()}"
                )
            result = container.wait(timeout=self.RUNNER_TIMEOUT)
            logger.info(
                f"[w{self.worker_id}][{repo_name}] Container exit code: {result.get('StatusCode', 'unknown')}"
            )
        else:
            result = container.wait(timeout=self.RUNNER_TIMEOUT)

        exit_code = result.get("StatusCode", -1)
        logger.info(
            f"[w{self.worker_id}][{repo_name}] Container exit code: {exit_code}"
        )

        # Extract result files from container (may exist even on non-zero exit)
        try:
            repo_content = self._extract_file_from_container(
                container, "/app/repository_results.json"
            )
            nodes_content = self._extract_file_from_container(
                container, "/app/node_results.jsonl"
            )

            if repo_content is None and nodes_content is None:
                raise RuntimeError(
                    f"Container exited with code {exit_code}, no results recovered"
                )

            if exit_code != 0 and nodes_content is not None:
                logger.warning(
                    f"[w{self.worker_id}][{repo_name}] Container exited with "
                    f"code {exit_code}, recovered partial results"
                )

            # Parse repository results
            repository_results = json.loads(repo_content) if repo_content else {}

            # Parse node results from JSONL
            nodes = {}
            if nodes_content:
                for line in nodes_content.strip().split("\n"):
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        node_id = entry.pop("node_id")
                        nodes[node_id] = entry
                    except json.JSONDecodeError:
                        # Truncated line from OOM mid-write
                        logger.warning(
                            f"[w{self.worker_id}][{repo_name}] "
                            "Skipping truncated JSONL line"
                        )

            return {
                "repository": repository_results,
                "nodes": nodes,
                "exit_code": exit_code,
            }
        finally:
            container.remove(force=True)

    def process_repository(
        self,
        repo_name: str,
        node_ids: list[str],
        requirements: str,
        commit_hash: str,
        experiment_name: str,
        *,
        debug: bool,
        skip_run_repo: bool = False,
    ) -> dict[str, any]:
        """Process a complete repository."""
        work_dir = None
        try:
            work_dir = Path(
                tempfile.mkdtemp(prefix=f"hypothesis_corpus_{repo_name.replace('/', '_')}_")
            )
            repo_dir = work_dir / "repo"
            app_dir = work_dir / "app"
            app_dir.mkdir(exist_ok=True)

            self.clone_repository(repo_name, repo_dir, commit_hash)
            self.setup_environment(
                app_dir,
                requirements,
                node_ids,
                experiment_name,
                debug=debug,
                repo_name=repo_name,
                skip_run_repo=skip_run_repo,
            )
            results = self.run_in_container(repo_name, work_dir, node_ids, debug)
            assert results is not None
            return results
        except Exception as e:
            logger.error(
                f"[w{self.worker_id}][{repo_name}] Failed to process repository: {e}"
            )
            return {
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
        finally:
            if work_dir and work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)
