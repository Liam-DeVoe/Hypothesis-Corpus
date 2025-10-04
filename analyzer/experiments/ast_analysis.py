"""
AST analysis experiment: enhanced source code analysis.

This module contains both:
1. The Experiment class (for the main analysis system)
2. Container analysis functions (run inside Docker)
"""

import json
from pathlib import Path
from typing import Any

# When run in container, analysis module won't exist - handle gracefully
try:
    from ..analysis import PropertyAnalyzer
    from .base import Experiment, ExperimentResult
except ImportError:
    # Running in container - use standalone version
    from dataclasses import dataclass, field
    from typing import Any as _Any

    PropertyAnalyzer = None  # type: ignore

    @dataclass
    class ExperimentResult:  # type: ignore
        """Results from running an experiment."""

        node_id: str
        success: bool
        data: dict[str, _Any] = field(default_factory=dict)
        error: str | None = None

        def to_dict(self) -> dict[str, _Any]:
            return {
                "node_id": self.node_id,
                "success": self.success,
                "data": self.data,
                "error": self.error,
            }

    Experiment = object  # type: ignore


class ASTAnalysisExperiment(Experiment):
    """Performs enhanced AST-based source code analysis."""

    name = "ast"

    def __init__(self):
        self.analyzer = PropertyAnalyzer()

    def process_results(
        self,
        node_id: str,
        container_results: ExperimentResult,
    ) -> ExperimentResult:
        """Process AST analysis results.

        This performs the actual AST analysis on the source code collected
        from the container.
        """
        if not container_results.success:
            return ExperimentResult(
                node_id=node_id, success=False, error=container_results.error
            )

        # Get source code from container results
        source_code = container_results.data.get("source_code")
        if not source_code:
            return ExperimentResult(
                node_id=node_id,
                success=False,
                error="No source code available for AST analysis",
            )

        # Perform AST analysis
        try:
            enhanced_results = self.analyzer.analyze_source(source_code)

            return ExperimentResult(
                node_id=node_id,
                success=True,
                data={
                    "source_code": source_code,  # Include source code for storage
                    "generators": enhanced_results.get("generators", {}),
                    "features": enhanced_results.get("features", {}),
                    "property_types": enhanced_results.get("property_types", []),
                    "complexity_metrics": enhanced_results.get(
                        "complexity_metrics", {}
                    ),
                    "patterns": enhanced_results.get("patterns", {}),
                    "ast": enhanced_results.get("ast", {}),
                },
            )
        except Exception as e:
            return ExperimentResult(
                node_id=node_id, success=False, error=f"AST analysis failed: {e}"
            )

    def store_to_database(
        self, db: Any, repo_id: int, test_id: int, result: ExperimentResult
    ):
        """Store AST analysis results to database."""
        data = result.data
        source_code = data.get("source_code", "")

        # Store source code with AST
        if source_code:
            db.add_test_code(test_id, source_code, json.dumps(data.get("ast", {})))

        # Store generators from enhanced analysis
        generators = data.get("generators", {})
        for gen_name, count in generators.items():
            if gen_name in ["composite", "custom_strategies"]:
                db.add_generator_usage(
                    test_id,
                    gen_name,
                    count,
                    is_composite=(gen_name == "composite"),
                    is_custom=(gen_name == "custom_strategies"),
                )
            else:
                db.add_generator_usage(test_id, gen_name, count)

        # Store property types
        property_types = data.get("property_types", [])
        for prop_type in property_types:
            db.add_property_type(test_id, prop_type)

        # Store features
        features = data.get("features", {})
        for feature_name, count in features.items():
            if isinstance(count, (int, bool)):
                feature_count = count if isinstance(count, int) else (1 if count else 0)
                if feature_count > 0:
                    db.add_feature_usage(test_id, feature_name, feature_count)

    def delete_data(self, db: Any, owner: str, name: str):
        """Delete AST analysis data."""
        tables = [
            "test_code",
            "generator_usage",
            "property_types",
            "feature_usage",
        ]
        db.delete_experiment_data(owner, name, tables)


# =============================================================================
# Container Analysis Functions (run inside Docker)
# =============================================================================


def run_analysis(file_path: Path, node_id: str) -> ExperimentResult:
    """Collect source code for AST analysis.

    This function runs inside the Docker container and returns the source code
    in the result data. The actual AST analysis happens outside the container
    in the ASTAnalysisExperiment.process_results() method.

    Args:
        file_path: Path to the test file
        node_id: Test node ID

    Returns:
        ExperimentResult with source code in data
    """
    try:
        source_code = file_path.read_text()
        return ExperimentResult(
            node_id=node_id, success=True, data={"source_code": source_code}
        )
    except Exception as e:
        return ExperimentResult(
            node_id=node_id, success=False, error=f"Failed to read source code: {e}"
        )
