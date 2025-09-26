"""
Analysis functions that run inside the Docker container.
This module gets copied into the container and executed there.
"""

import json
import re
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional


def setup_dependencies(requirements_file: Optional[Path] = None) -> bool:
    """Install Python dependencies in the container."""

    def run_pip_install(args: List[str], description: str) -> bool:
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


def extract_property_source(file_path: Path, node_id: str) -> tuple[Optional[str], Optional[int]]:
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


def analyze_test_file(file_path: Path, node_id: str) -> Dict[str, Any]:
    """Analyze a test file for property-based testing patterns."""

    HYPOTHESIS_STRATEGIES = [
        "binary",
        "booleans",
        "builds",
        "characters",
        "complex_numbers",
        "composite",
        "data",
        "dates",
        "datetimes",
        "decimals",
        "deferred",
        "dictionaries",
        "emails",
        "fixed_dictionaries",
        "floats",
        "fractions",
        "from_regex",
        "from_type",
        "frozensets",
        "functions",
        "integers",
        "ip_addresses",
        "iterables",
        "just",
        "lists",
        "none",
        "nothing",
        "one_of",
        "permutations",
        "randoms",
        "recursive",
        "register_type_strategy",
        "runner",
        "sampled_from",
        "sets",
        "shared",
        "slices",
        "text",
        "timedeltas",
        "times",
        "timezone_keys",
        "timezones",
        "tuples",
        "uuids",
    ]

    results = {
        "node_id": node_id,
        "strategies_used": [],
        "property_types": [],
        "features": {
            "uses_assume": False,
            "uses_note": False,
            "uses_event": False,
            "uses_target": False,
            "uses_settings": False,
            "uses_seed": False,
            "uses_database": False,
            "uses_stateful": False,
            "uses_composite": False,
            "max_examples": None,
        },
        "property_source": None,
        "property_line_number": None,
    }

    try:
        content = file_path.read_text()

        # Extract the property source code
        property_text, line_number = extract_property_source(file_path, node_id)
        if property_text:
            results["property_source"] = property_text
            results["property_line_number"] = line_number

        # Find strategies using regex
        for strategy in HYPOTHESIS_STRATEGIES:
            pattern = rf"\b{strategy}\s*\("
            if re.search(pattern, content):
                results["strategies_used"].append(strategy)

        # Detect features
        results["features"]["uses_assume"] = "assume(" in content
        results["features"]["uses_note"] = "note(" in content
        results["features"]["uses_event"] = "event(" in content
        results["features"]["uses_target"] = "target(" in content
        results["features"]["uses_settings"] = (
            "@settings" in content or "settings(" in content
        )
        results["features"]["uses_seed"] = "@seed" in content
        results["features"]["uses_database"] = (
            "database=" in content or "ExampleDatabase" in content
        )
        results["features"]["uses_stateful"] = "RuleBasedStateMachine" in content
        results["features"]["uses_composite"] = "@composite" in content

        # Extract max_examples if present
        max_examples_match = re.search(r"max_examples\s*=\s*(\d+)", content)
        if max_examples_match:
            results["features"]["max_examples"] = int(max_examples_match.group(1))

        # Classify property types
        if "math" in content.lower() or "arithmetic" in content.lower():
            results["property_types"].append("mathematical")
        if any(
            x in content
            for x in [
                "encode",
                "decode",
                "serialize",
                "deserialize",
                "json.dumps",
                "json.loads",
            ]
        ):
            results["property_types"].append("round_trip")
        if "RuleBasedStateMachine" in content:
            results["property_types"].append("model_based")
        if any(x in content for x in ["oracle", "reference"]):
            results["property_types"].append("oracle")
        if any(x in content for x in ["metamorphic", "transformation"]):
            results["property_types"].append("metamorphic")

    except Exception as e:
        results["error"] = f"Failed to analyze: {e}"

    return results


def run_test_with_coverage(node_id: str, timeout: int = 300) -> Dict[str, Any]:
    """Run a single test and collect coverage information."""
    results = {
        "node_id": node_id,
        "test_result": None,
        "observability_data": {},
        "error": None,
    }

    try:
        # Clear any previous observability data
        obs_dir = Path("/app/.hypothesis/observed")
        if obs_dir.exists():
            shutil.rmtree(obs_dir)

        # Run the test with pytest
        cmd = ["python", "-m", "pytest", node_id, "-xvs", "--tb=short"]

        print(f"Starting pytest subprocess for {node_id}", flush=True)
        start_pytest = time.time()
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd="/app", timeout=timeout
        )
        pytest_time = time.time() - start_pytest
        print(f"[TIMING] Pytest execution: {pytest_time:.3f}s", flush=True)

        results["test_result"] = {
            "exit_code": result.returncode,
            "stdout": result.stdout[-5000:] if result.stdout else "",
            "stderr": result.stderr[-5000:] if result.stderr else "",
            "passed": result.returncode == 0,
        }

        # Parse observability data if it exists
        if obs_dir.exists():
            results["observability_data"] = parse_observability_data(obs_dir)

    except subprocess.TimeoutExpired:
        results["error"] = f"Test timed out after {timeout} seconds"
    except Exception as e:
        results["error"] = str(e)

    return results


def parse_observability_data(obs_dir: Path) -> Dict[str, Any]:
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


def analyze_repository(node_ids: List[str]) -> Dict[str, Any]:
    """Main analysis function that processes all tests in a repository."""
    import time

    print("Starting analysis...", flush=True)
    print(f"Python version: {sys.version}", flush=True)
    print(f"Current directory: {Path.cwd()}", flush=True)
    print(f"Node IDs to process: {node_ids}", flush=True)

    results = {}
    total_tests = len(node_ids)

    for i, node_id in enumerate(node_ids, 1):
        parts = node_id.split("::")
        file_path = Path(parts[0])

        print(f"\nProcessing test {i}/{total_tests}: {node_id}", flush=True)
        print(f"Looking for file: {file_path}", flush=True)

        if file_path.exists():
            print(f"Found file: {file_path}", flush=True)

            # First analyze the test file for patterns
            start_analysis = time.time()
            analysis_results = analyze_test_file(file_path, node_id)
            analysis_time = time.time() - start_analysis
            print(f"[TIMING] Static analysis: {analysis_time:.3f}s", flush=True)

            # Then run the test to collect coverage
            print("Running test with coverage...", flush=True)
            coverage_results = run_test_with_coverage(node_id)

            # Combine results
            results[node_id] = {
                "analysis": analysis_results,
                "coverage": coverage_results,
                "file_path": str(file_path),
            }

            print(
                f"Test result: {coverage_results.get('test_result', {}).get('passed', False)}"
            )
            if coverage_results.get("observability_data", {}).get("coverage"):
                coverage_files = len(coverage_results["observability_data"]["coverage"])
                print(f"Coverage data collected for {coverage_files} files")

        else:
            print(f"File not found: {file_path}")
            results[node_id] = {"error": f"File not found: {file_path}"}

    return results


def main():
    """Entry point for container execution."""
    import json
    import sys
    from pathlib import Path

    try:
        # Read configuration
        config_file = Path("/app/config.json")
        if not config_file.exists():
            print("ERROR: No config.json found", flush=True)
            sys.exit(1)

        config = json.loads(config_file.read_text())
        node_ids = config.get("node_ids", [])
        requirements_file = (
            Path("/app/requirements.txt")
            if Path("/app/requirements.txt").exists()
            else None
        )

        # Setup dependencies
        if not setup_dependencies(requirements_file):
            print("ERROR: Failed to setup dependencies", flush=True)
            sys.exit(1)

        # Run analysis
        results = analyze_repository(node_ids)

        # Write results
        print("\nWriting results to results.json", flush=True)
        with open("/app/results.json", "w") as f:
            json.dump(results, f, indent=2, default=str)

        print("Analysis complete", flush=True)

    except Exception as e:
        print(f"ERROR in main: {e}", flush=True)
        print(f"Traceback: {traceback.format_exc()}", flush=True)

        # Write error to results
        with open("/app/results.json", "w") as f:
            json.dump(
                {"error": str(e), "traceback": traceback.format_exc()}, f, indent=2
            )

        sys.exit(1)


if __name__ == "__main__":
    main()
