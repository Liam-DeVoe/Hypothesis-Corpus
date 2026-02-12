import importlib
import json
import os
import sys
import traceback
from pathlib import Path

try:
    from .utils import pip_install
except ImportError:
    from utils import pip_install


def main():
    """Run the configured experiment inside the container."""
    try:
        # Read configuration
        config_file = Path("/app/config.json")
        if not config_file.exists():
            print("ERROR: No config.json found", flush=True)
            sys.exit(1)

        config = json.loads(config_file.read_text())
        node_ids = config["node_ids"]
        experiment_name = config["experiment_name"]
        debug = config["debug"]
        requirements_file = Path("/app/requirements.txt")
        assert requirements_file.exists()

        print(f"Starting analysis with experiment: {experiment_name}", flush=True)
        print(f"Python version: {sys.version}", flush=True)
        print(f"Current directory: {Path.cwd()}", flush=True)
        print(f"Node IDs to process: {node_ids}", flush=True)

        # install library and dependencies
        print("installing dependencies...", flush=True)

        pbt_analysis_dir = Path("/app/pytest_pbt_analysis")
        assert pbt_analysis_dir.exists()

        pip_install(["--no-dependencies", "-r", str(requirements_file)])
        pip_install(["--no-dependencies", "/app/repo"])
        pip_install([str(pbt_analysis_dir)])

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

        # Run repository-level analysis
        repo_name = config["repo_name"]
        skip_run_repo = config["skip_run_repo"]
        repository_results = {}

        if skip_run_repo:
            print("Skipping repository-level analysis (already completed)", flush=True)
        else:
            print(
                f"Running repository-level analysis for {repo_name}...",
                flush=True,
            )
            try:
                repository_results["data"] = experiment_class.run_repository(
                    repo_name, node_ids
                )
            except Exception as e:
                print(f"ERROR: Repository-level analysis failed: {e}", flush=True)
                print(f"Traceback: {traceback.format_exc()}", flush=True)
                repository_results["error"] = str(e)
                repository_results["traceback"] = traceback.format_exc()

        # Write repository results immediately
        with open("/app/repository_results.json", "w") as f:
            json.dump(repository_results, f, indent=2, default=str)

        # Process all test nodes, writing results incrementally
        with open("/app/node_results.jsonl", "w") as f:
            for i, node_id in enumerate(node_ids, 1):
                print(
                    f"Processing test {i}/{len(node_ids)}: {node_id}",
                    flush=True,
                )
                print(f"Running {experiment_name} experiment...", flush=True)
                node_data = {"node_id": node_id}

                try:
                    node_data[experiment_name] = experiment_class.run(
                        node_id, debug=debug
                    )
                except Exception as e:
                    print(f"ERROR: Experiment failed: {e}", flush=True)
                    print(f"Traceback: {traceback.format_exc()}", flush=True)
                    node_data["error"] = str(e)
                    node_data["traceback"] = traceback.format_exc()

                # Write as JSONL line and flush to survive OOM
                f.write(json.dumps(node_data) + "\n")
                f.flush()
                os.fsync(f.fileno())

        print("Analysis complete", flush=True)

    except Exception as e:
        print(f"ERROR in main: {e}", flush=True)
        print(f"Traceback: {traceback.format_exc()}", flush=True)

        # Write error to repository results so host can see it
        with open("/app/repository_results.json", "w") as f:
            json.dump(
                {"error": str(e), "traceback": traceback.format_exc()},
                f,
                indent=2,
            )

        sys.exit(1)


if __name__ == "__main__":
    main()
