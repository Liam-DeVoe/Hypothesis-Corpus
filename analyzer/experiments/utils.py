import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def setup_dependencies(requirements_file: Path | None = None) -> bool:
    """Install Python dependencies in the container."""

    def run_pip_install(args: list[str], description: str) -> bool:
        """Run pip install with timing."""
        print(f"Installing {description}...", flush=True)
        start_time = time.time()

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--quiet",
                    "--disable-pip-version-check",
                ]
                + args,
                capture_output=True,
                text=True,
            )
            elapsed = time.time() - start_time
            print(f"[TIMING] Install {description}: {elapsed:.3f}s", flush=True)

            if result.returncode != 0:
                print(
                    f"Warning: {description} install had issues: {result.stderr}",
                    flush=True,
                )
            return result.returncode == 0
        except Exception as e:
            elapsed = time.time() - start_time
            print(
                f"[TIMING] Install {description} failed after {elapsed:.3f}s: {e}",
                flush=True,
            )
            return False

    print("Starting dependency installation...", flush=True)

    # Install project requirements if they exist
    if requirements_file and requirements_file.exists():
        run_pip_install(
            ["--no-dependencies", "-r", str(requirements_file)], "project requirements"
        )

    # Try to install the repository itself as a library
    run_pip_install(["--no-dependencies", "-e", "/app"], "repository package")

    if not run_pip_install(["pytest"], "pytest"):
        print("Failed to install pytest", flush=True)
        return False

    if not run_pip_install(["-U", "hypothesis"], "hypothesis"):
        print("Failed to install hypothesis", flush=True)
        return False

    print("Setup complete!", flush=True)
    return True


def parse_observability_data(obs_dir: Path) -> dict[str, Any]:
    """Parse Hypothesis observability JSONL files."""
    data = {"coverage": {}, "timing": {}, "examples": [], "test_cases": []}

    try:
        jsonl_files = list(obs_dir.glob("**/*.jsonl"))

        for jsonl_file in jsonl_files:
            try:
                with open(jsonl_file) as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)

                            # Process different types of observability data
                            if "type" in entry:
                                if entry["type"] == "test_case":
                                    data["test_cases"].append(entry)
                                elif entry["type"] == "timing":
                                    data["timing"][entry.get("phase", "unknown")] = (
                                        entry.get("duration", 0)
                                    )

                            # Extract coverage information
                            if "coverage" in entry:
                                for file_path, lines in entry["coverage"].items():
                                    if file_path not in data["coverage"]:
                                        data["coverage"][file_path] = set()
                                    data["coverage"][file_path].update(lines)
            except Exception:
                # Skip any problematic JSONL files
                pass

        # Convert sets to lists for JSON serialization
        for file_path in data["coverage"]:
            data["coverage"][file_path] = sorted(data["coverage"][file_path])

    except Exception as e:
        data["error"] = f"Failed to parse observability data: {e}"

    return data
