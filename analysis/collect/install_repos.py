import json
import shutil
import subprocess
import tarfile
import tempfile
from io import BytesIO
from pathlib import Path

import docker

from .utils import (
    CACHE_SIZE_LIMIT,
    CACHE_VOLUME_NAME,
    clean_uv_cache,
    get_cache_volume_size,
)

PRE_INSTALL = [
    "setuptools==78.1.0",
]

POST_INSTALL = [
    "pytest==8.4.2",
    "hypothesis==6.140.3",
]

# seconds to allow `pytest --collect-only` to run for during installation before
# timing out
PYTEST_COLLECTION_TIMEOUT = 5 * 60  # 5 minutes


def install_repository(
    repo_name: str, docker_image: str = "pbt-analysis:latest", debug: bool = False
) -> dict:
    """Process a single repository: install and collect tests using Docker."""
    container = None
    work_dir = Path(tempfile.mkdtemp(prefix=f"install_{repo_name.replace('/', '_')}_"))
    repo_dir = work_dir / "repo"
    app_dir = work_dir / "app"
    docker_client = docker.from_env()

    try:
        # Check cache size and clean if necessary
        size_bytes = get_cache_volume_size(docker_client)
        if size_bytes > CACHE_SIZE_LIMIT:
            size_gb = size_bytes / (1024**3)
            print(f"uv cache exceeded limit ({size_gb:.2f}GB), cleaning cache")
            clean_uv_cache(docker_client)

        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                f"https://github.com/{repo_name}.git",
                str(repo_dir),
            ],
            capture_output=True,
            text=True,
            timeout=300,
            check=True,
        )

        # Create app directory and copy installation infrastructure
        app_dir.mkdir(exist_ok=True)
        install_script_source = Path(__file__).parent / "_install.py"
        shutil.copy(install_script_source, app_dir / "_install.py")

        # Create config file for the installation script
        config = {
            "pre_install": PRE_INSTALL,
            "post_install": POST_INSTALL,
            "pytest_collection_timeout": PYTEST_COLLECTION_TIMEOUT,
        }
        config_path = app_dir / "_install_config.json"
        config_path.write_text(json.dumps(config))

        # Create tar archive from structured directories
        tar_stream = BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            tar.add(app_dir, arcname="/app")
            tar.add(repo_dir, arcname="/app/repo")
        tar_stream.seek(0)

        # Create and run container
        container = docker_client.containers.create(
            docker_image,
            command=["python", "/app/_install.py"],
            mem_limit="2g",
            security_opt=["no-new-privileges"],
            volumes={CACHE_VOLUME_NAME: {"bind": "/root/.cache/uv", "mode": "rw"}},
            environment={"UV_LINK_MODE": "copy"},
        )
        container.put_archive("/", tar_stream.read())
        container.start()

        if debug:
            # Stream logs in real-time
            print("Container logs (streaming):")
            for log_line in container.logs(stream=True, follow=True):
                print(log_line.decode("utf-8", errors="replace"), end="", flush=True)

        # Wait for completion (30 minute timeout)
        result = container.wait(timeout=30 * 60)
        logs = container.logs(stdout=True, stderr=True).decode("utf-8")

        if debug:
            print(f"Container logs:\n{logs}")

        if result.get("StatusCode") != 0:
            raise RuntimeError(
                f"container exit code {result.get("StatusCode")}. Container logs:: {logs}"
            )

        # Extract results
        bits, _ = container.get_archive("/app/_install_results.json")
        tar_stream = BytesIO()
        for chunk in bits:
            tar_stream.write(chunk)
        tar_stream.seek(0)

        with tarfile.open(fileobj=tar_stream) as tar:
            results_file = tar.extractfile("_install_results.json")
            results = json.loads(results_file.read().decode("utf-8"))

        results["collection_output"] = logs
        return results
    finally:
        # Clean up
        if work_dir and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        if container:
            container.remove(force=True)
