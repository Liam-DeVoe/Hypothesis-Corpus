"""
Shared helper functions for container analysis.

These functions are used by multiple experiments and run inside the Docker container.
"""

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


def extract_property_source(
    file_path: Path, node_id: str
) -> tuple[str | None, int | None]:
    """Extract the source code of a specific property test and its line number."""
    try:
        import ast

        content = file_path.read_text()
        tree = ast.parse(content)

        # Parse node_id to find the test function
        parts = node_id.split("::")
        test_name = parts[-1] if parts else None
        class_name = parts[-2] if len(parts) > 2 else None

        # Special case for stateful tests (e.g., TestUnionFind::runTest)
        if class_name and test_name == "runTest":
            # Look for TestClass = SomethingRules.TestCase pattern
            for line_no, line in enumerate(content.splitlines(), 1):
                if f"{class_name} = " in line and "TestCase" in line:
                    # Find the RuleBasedStateMachine class
                    rules_class = line.split("=")[1].split(".")[0].strip()

                    # Find the RuleBasedStateMachine class in AST
                    for node in ast.walk(tree):
                        if isinstance(node, ast.ClassDef) and node.name == rules_class:
                            lines = content.splitlines()
                            start_line = node.lineno - 1
                            end_line = node.end_lineno
                            property_source = "\n".join(lines[start_line:end_line])
                            return property_source, node.lineno

        # Find the test function in the AST
        for node in ast.walk(tree):
            # If test is in a class
            if (
                class_name
                and isinstance(node, ast.ClassDef)
                and node.name == class_name
            ):
                # If we can't find the specific method, return the whole class
                lines = content.splitlines()
                start_line = node.lineno - 1
                end_line = node.end_lineno
                default_class_source = "\n".join(lines[start_line:end_line])
                default_line = node.lineno

                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == test_name:
                        # Get the source lines for this function
                        lines = content.splitlines()
                        start_line = item.lineno - 1
                        end_line = item.end_lineno
                        property_source = "\n".join(lines[start_line:end_line])
                        return property_source, item.lineno

                # Return whole class if method not found
                return default_class_source, default_line

            # If test is a top-level function
            elif (
                not class_name
                and isinstance(node, ast.FunctionDef)
                and node.name == test_name
            ):
                lines = content.splitlines()
                start_line = node.lineno - 1
                end_line = node.end_lineno
                property_source = "\n".join(lines[start_line:end_line])
                # Also return the line number for permalink construction
                return property_source, node.lineno

        # Fallback to returning a reasonable chunk around the test
        if test_name:
            for i, line in enumerate(content.splitlines()):
                if f"def {test_name}" in line:
                    # Return up to 50 lines from the function definition
                    lines = content.splitlines()
                    start = i
                    end = min(i + 50, len(lines))
                    return "\n".join(lines[start:end]), i + 1

    except Exception as e:
        print(f"Failed to extract property source: {e}", flush=True)

    return None, None


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
