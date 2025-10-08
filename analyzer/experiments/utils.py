import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def setup_dependencies(requirements_file: Path | None = None) -> bool:
    """Install Python dependencies in the container."""

    def run_pip_install(args: list[str], description: str) -> bool:
        """Run pip install with timing."""
        print(f"Installing {description}...", flush=True)

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
            if result.returncode != 0:
                print(
                    f"Warning: {description} install had issues: {result.stderr}",
                    flush=True,
                )
            return result.returncode == 0
        except Exception:
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
    data = {"coverage": {}, "test_cases": []}

    for jsonl_file in list(obs_dir.glob("**/*.jsonl")):
        with open(jsonl_file) as f:
            for line in f:
                if not line.strip():
                    continue

                entry = json.loads(line)
                if entry["type"] != "test_case":
                    continue

                data["test_cases"].append(entry)

                # Aggregate coverage across all test cases
                coverage = entry["coverage"]
                if coverage:
                    for file_path, lines in coverage.items():
                        if file_path not in data["coverage"]:
                            data["coverage"][file_path] = set()
                        data["coverage"][file_path].update(lines)

    # Convert sets to lists for JSON serialization
    for file_path in data["coverage"]:
        data["coverage"][file_path] = sorted(data["coverage"][file_path])

    return data
