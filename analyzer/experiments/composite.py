"""
Composite experiment: runs all analyses (default behavior).
"""

from typing import Any

from .base import Experiment, ExperimentResult


class AllExperiment(Experiment):
    """Runs all available experiments (static analysis, coverage, AST analysis)."""

    name = "all"

    def __init__(self):
        # Load all other experiments (exclude self)
        self.experiments = [
            Experiment.get_experiment(name)
            for name in Experiment.list_experiments()
            if name != "all"
        ]

    def process_results(
        self,
        node_id: str,
        container_results: dict[str, Any],
    ) -> ExperimentResult:
        """Process results by running all experiment processors.

        Container results contain serialized ExperimentResult dicts from each
        experiment under their respective keys (analysis, coverage, ast_data).
        """
        if "error" in container_results:
            return ExperimentResult(
                node_id=node_id, success=False, error=container_results["error"]
            )

        # Map experiment names to their result keys in container_results
        result_key_map = {
            "static": "analysis",
            "coverage": "coverage",
            "ast": "ast_data",
        }

        # Combine data from all experiments
        combined_data = {}

        for exp in self.experiments:
            result_key = result_key_map.get(exp.name)
            if not result_key or result_key not in container_results:
                return ExperimentResult(
                    node_id=node_id,
                    success=False,
                    error=f"Missing result key '{result_key}' for experiment '{exp.name}'",
                )

            # Deserialize the container result
            exp_container_result = ExperimentResult.from_dict(
                container_results[result_key]
            )

            # Process through experiment (pass-through for most, enhanced for AST)
            result = exp.process_results(node_id, exp_container_result)

            if not result.success:
                # If any experiment fails, return its error
                return result

            # Combine data from each experiment
            combined_data[exp.name] = result.data

        return ExperimentResult(node_id=node_id, success=True, data=combined_data)

    def store_to_database(
        self, db: Any, repo_id: int, test_id: int, result: ExperimentResult
    ):
        """Store data by calling all experiment store methods.

        The result contains data organized by experiment name, so we need to
        extract each experiment's data and create individual ExperimentResults.
        """
        for exp in self.experiments:
            # Extract data for this specific experiment
            exp_data = result.data.get(exp.name, {})

            # Create an ExperimentResult for this experiment
            exp_result = ExperimentResult(
                node_id=result.node_id, success=True, data=exp_data
            )

            # Store using the experiment's store method
            exp.store_to_database(db, repo_id, test_id, exp_result)

    def delete_data(self, db: Any, owner: str, name: str):
        """Delete data by calling all experiment delete methods."""
        for exp in self.experiments:
            exp.delete_data(db, owner, name)
