#!/usr/bin/env python
"""
Container runner: entry point for experiment execution inside Docker.

This script is copied into the container and coordinates running the
appropriate experiment analysis functions.
"""

import json
import sys
import time
import traceback
from pathlib import Path


def main():
    """Run the configured experiment inside the container."""
    try:
        # Read configuration
        config_file = Path("/app/config.json")
        if not config_file.exists():
            print("ERROR: No config.json found", flush=True)
            sys.exit(1)

        config = json.loads(config_file.read_text())
        node_ids = config.get("node_ids", [])
        experiment_name = config.get("experiment_name", "all")
        requirements_file = (
            Path("/app/requirements.txt")
            if Path("/app/requirements.txt").exists()
            else None
        )

        print(f"Starting analysis with experiment: {experiment_name}", flush=True)
        print(f"Python version: {sys.version}", flush=True)
        print(f"Current directory: {Path.cwd()}", flush=True)
        print(f"Node IDs to process: {node_ids}", flush=True)

        # Setup dependencies
        from container_helpers import setup_dependencies

        if not setup_dependencies(requirements_file):
            print("ERROR: Failed to setup dependencies", flush=True)
            sys.exit(1)

        # Import the appropriate experiment module with standardized interface
        if experiment_name == "static":
            from static_analysis import run_analysis

            result_key = "analysis"
        elif experiment_name == "coverage":
            from coverage import run_analysis

            result_key = "coverage"
        elif experiment_name == "ast":
            from ast_analysis import run_analysis

            result_key = "ast_data"
        elif experiment_name == "all":
            # Import all experiments (each with run_analysis function)
            import ast_analysis
            import coverage
            import static_analysis

            run_analysis = None  # Will handle separately
            result_key = None
        else:
            print(f"ERROR: Unknown experiment: {experiment_name}", flush=True)
            sys.exit(1)

        # Process all test nodes
        results = {}
        total_tests = len(node_ids)

        for i, node_id in enumerate(node_ids, 1):
            parts = node_id.split("::")
            file_path = Path(parts[0])

            print(f"\nProcessing test {i}/{total_tests}: {node_id}", flush=True)
            print(f"Looking for file: {file_path}", flush=True)

            if not file_path.exists():
                print(f"File not found: {file_path}", flush=True)
                results[node_id] = {"error": f"File not found: {file_path}"}
                continue

            print(f"Found file: {file_path}", flush=True)

            node_results = {"file_path": str(file_path)}

            # Run the appropriate analysis
            if experiment_name == "all":
                # Run all experiments using their standardized interfaces
                print("Running static analysis...", flush=True)
                start = time.time()
                static_results = static_analysis.run_analysis(file_path, node_id)
                print(
                    f"[TIMING] Static analysis: {time.time() - start:.3f}s", flush=True
                )
                node_results["analysis"] = static_results.to_dict()

                print("Running test with coverage...", flush=True)
                coverage_results = coverage.run_analysis(file_path, node_id)
                node_results["coverage"] = coverage_results.to_dict()

                print("Collecting source code...", flush=True)
                ast_results = ast_analysis.run_analysis(file_path, node_id)
                node_results["ast_data"] = ast_results.to_dict()
            else:
                # Run single experiment using standardized interface
                print(f"Running {experiment_name} experiment...", flush=True)
                start = time.time()

                # All experiments now return ExperimentResult
                exp_result = run_analysis(file_path, node_id)

                print(
                    f"[TIMING] {experiment_name}: {time.time() - start:.3f}s",
                    flush=True,
                )
                node_results[result_key] = exp_result.to_dict()

            results[node_id] = node_results

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
