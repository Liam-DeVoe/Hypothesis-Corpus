import importlib
import json
import sys
import traceback
from pathlib import Path

try:
    from .utils import filepath_from_node
except ImportError:
    from utils import filepath_from_node


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
        experiment_name = config.get("experiment_name", "coverage")
        debug = config.get("debug", False)
        requirements_file = Path("/app/requirements.txt")
        assert requirements_file.exists()

        print(f"Starting analysis with experiment: {experiment_name}", flush=True)
        print(f"Python version: {sys.version}", flush=True)
        print(f"Current directory: {Path.cwd()}", flush=True)
        print(f"Node IDs to process: {node_ids}", flush=True)

        # Setup dependencies
        from utils import setup_dependencies

        if not setup_dependencies(requirements_file):
            print("ERROR: Failed to setup dependencies", flush=True)
            sys.exit(1)

        import experiment

        sys.modules["experiment"] = experiment
        Experiment = experiment.Experiment
        importlib.import_module(experiment_name)

        experiment_class = Experiment.experiments.get(experiment_name)
        if not experiment_class:
            print(f"ERROR: Unknown experiment: {experiment_name}", flush=True)
            print(
                f"Available experiments: {list(Experiment.experiments.keys())}",
                flush=True,
            )
            sys.exit(1)

        # Process all test nodes
        results = {}
        total_tests = len(node_ids)

        for i, node_id in enumerate(node_ids, 1):
            file_path = filepath_from_node(node_id)

            print(f"\nProcessing test {i}/{total_tests}: {node_id}", flush=True)
            assert file_path.exists(), f"file not found: {file_path}"
            node_results = {"file_path": str(file_path)}

            print(f"Running {experiment_name} experiment...", flush=True)

            try:
                exp_data = experiment_class.run(file_path, node_id, debug=debug)
                node_results[experiment_name] = exp_data
            except Exception as e:
                print(f"ERROR: Experiment failed: {e}", flush=True)
                print(f"Traceback: {traceback.format_exc()}", flush=True)
                node_results["error"] = str(e)
                node_results["traceback"] = traceback.format_exc()

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
