"""
Static analysis experiment: pattern detection without test execution.

This module contains both:
1. The Experiment class (for the main analysis system)
2. Container analysis functions (run inside Docker)
"""

import re
from pathlib import Path
from typing import Any

# When run in container, base module won't exist - need standalone copy
try:
    from .base import Experiment, ExperimentResult
except ImportError:
    # Running in container - use standalone version
    from dataclasses import dataclass, field
    from typing import Any as _Any

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


class StaticAnalysisExperiment(Experiment):
    """Analyzes test files for Hypothesis patterns without executing tests."""

    name = "static"

    def store_to_database(
        self, db: Any, repo_id: int, test_id: int, result: ExperimentResult
    ):
        """Store static analysis results to database."""
        data = result.data

        # Store generators/strategies
        strategies = data.get("strategies_used", [])
        for strategy in strategies:
            db.add_generator_usage(test_id, strategy, count=1)

        # Store property types
        property_types = data.get("property_types", [])
        for prop_type in property_types:
            db.add_property_type(test_id, prop_type)

        # Store features
        features = data.get("features", {})
        for feature_name, feature_value in features.items():
            if isinstance(feature_value, bool) and feature_value:
                db.add_feature_usage(test_id, feature_name, count=1)
            elif isinstance(feature_value, int) and feature_value > 0:
                db.add_feature_usage(test_id, feature_name, count=feature_value)

    def delete_data(self, db: Any, owner: str, name: str):
        """Delete static analysis data."""
        tables = ["generator_usage", "property_types", "feature_usage"]
        db.delete_experiment_data(owner, name, tables)


# =============================================================================
# Container Analysis Functions (run inside Docker)
# =============================================================================


def run_analysis(file_path: Path, node_id: str) -> ExperimentResult:
    """Analyze a test file for property-based testing patterns.

    This function runs inside the Docker container.

    Args:
        file_path: Path to the test file
        node_id: Test node ID

    Returns:
        ExperimentResult with static analysis results
    """
    # Import helpers (will be copied to container)
    try:
        from container_helpers import extract_property_source
    except ImportError:
        # Fallback for testing
        from .container_helpers import extract_property_source

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

    data = {
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
            data["property_source"] = property_text
            data["property_line_number"] = line_number

        # Find strategies using regex
        for strategy in HYPOTHESIS_STRATEGIES:
            pattern = rf"\b{strategy}\s*\("
            if re.search(pattern, content):
                data["strategies_used"].append(strategy)

        # Detect features
        data["features"]["uses_assume"] = "assume(" in content
        data["features"]["uses_note"] = "note(" in content
        data["features"]["uses_event"] = "event(" in content
        data["features"]["uses_target"] = "target(" in content
        data["features"]["uses_settings"] = (
            "@settings" in content or "settings(" in content
        )
        data["features"]["uses_seed"] = "@seed" in content
        data["features"]["uses_database"] = (
            "database=" in content or "ExampleDatabase" in content
        )
        data["features"]["uses_stateful"] = "RuleBasedStateMachine" in content
        data["features"]["uses_composite"] = "@composite" in content

        # Extract max_examples if present
        max_examples_match = re.search(r"max_examples\s*=\s*(\d+)", content)
        if max_examples_match:
            data["features"]["max_examples"] = int(max_examples_match.group(1))

        # Classify property types
        if "math" in content.lower() or "arithmetic" in content.lower():
            data["property_types"].append("mathematical")
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
            data["property_types"].append("round_trip")
        if "RuleBasedStateMachine" in content:
            data["property_types"].append("model_based")
        if any(x in content for x in ["oracle", "reference"]):
            data["property_types"].append("oracle")
        if any(x in content for x in ["metamorphic", "transformation"]):
            data["property_types"].append("metamorphic")

        return ExperimentResult(node_id=node_id, success=True, data=data)

    except Exception as e:
        return ExperimentResult(
            node_id=node_id, success=False, error=f"Failed to analyze: {e}"
        )
