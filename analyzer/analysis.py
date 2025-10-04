"""
Analysis module for extracting patterns from property-based tests.
"""

import ast
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class PropertyAnalyzer:
    """Analyzer for extracting patterns from property-based test code."""

    def __init__(self):
        """Initialize the analyzer with pattern definitions."""
        self.strategy_patterns = self._build_strategy_patterns()
        self.property_type_patterns = self._build_property_patterns()

    def _build_strategy_patterns(self) -> dict[str, re.Pattern]:
        """Build regex patterns for detecting Hypothesis strategies."""
        strategies = [
            "integers",
            "floats",
            "text",
            "binary",
            "booleans",
            "lists",
            "dictionaries",
            "tuples",
            "sets",
            "frozensets",
            "one_of",
            "none",
            "just",
            "sampled_from",
            "permutations",
            "datetimes",
            "dates",
            "times",
            "timedeltas",
            "uuids",
            "emails",
            "urls",
            "ip_addresses",
            "complex_numbers",
            "fractions",
            "decimals",
            "characters",
            "from_regex",
            "from_type",
            "deferred",
            "data",
            "builds",
            "fixed_dictionaries",
            "recursive",
            "composite",
            "shared",
            "runner",
        ]

        patterns = {}
        for strategy in strategies:
            # Match both st.strategy and strategies.strategy
            patterns[strategy] = re.compile(
                rf"\b(?:st|strategies)\.{strategy}\s*\(", re.IGNORECASE
            )

        return patterns

    def _build_property_patterns(self) -> dict[str, list[re.Pattern]]:
        """Build patterns for classifying property types."""
        return {
            "mathematical": [
                re.compile(
                    r"\b(?:commutative|associative|distributive|identity)\b", re.I
                ),
                re.compile(r"\b(?:inverse|idempotent|transitive|symmetric)\b", re.I),
                re.compile(r"\b(?:monotonic|homomorphism|isomorphism)\b", re.I),
            ],
            "roundtrip": [
                re.compile(r"encode.*decode|decode.*encode", re.I),
                re.compile(r"serialize.*deserialize|deserialize.*serialize", re.I),
                re.compile(r"dump.*load|load.*dump", re.I),
                re.compile(r"parse.*format|format.*parse", re.I),
                re.compile(r"to_.*from_|from_.*to_", re.I),
            ],
            "model_based": [
                re.compile(r"RuleBasedStateMachine"),
                re.compile(r"@rule\s*\("),
                re.compile(r"@invariant\s*\("),
                re.compile(r"@initialize\s*\("),
                re.compile(r"Bundle\s*\("),
            ],
            "oracle": [
                re.compile(
                    r"reference.*implementation|implementation.*reference", re.I
                ),
                re.compile(r"expected.*actual|actual.*expected", re.I),
                re.compile(r"golden.*master", re.I),
            ],
            "metamorphic": [
                re.compile(r"metamorphic", re.I),
                re.compile(r"transform.*preserve", re.I),
                re.compile(r"equivalent.*transformation", re.I),
            ],
            "differential": [
                re.compile(r"differential.*testing", re.I),
                re.compile(r"compare.*implementations", re.I),
                re.compile(r"version.*compatibility", re.I),
            ],
        }

    def analyze_source(self, source_code: str) -> dict[str, Any]:
        """Perform comprehensive analysis on source code."""
        results = {
            "generators": {},
            "features": {},
            "property_types": [],
            "complexity_metrics": {},
            "patterns": {},
        }

        try:
            # Analyze generators
            results["generators"] = self.extract_generators(source_code)

            # Analyze features
            results["features"] = self.extract_features(source_code)

            # Classify property types
            results["property_types"] = self.classify_property_types(source_code)

            # Extract AST-based metrics
            tree = ast.parse(source_code)
            results["ast"] = self.analyze_ast(tree)

            # Analyze complexity
            results["complexity_metrics"] = self.calculate_complexity(tree, source_code)

            # Detect patterns
            results["patterns"] = self.detect_patterns(source_code)

        except SyntaxError as e:
            logger.error(f"Syntax error in source code: {e}")
            results["parse_error"] = str(e)
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            results["analysis_error"] = str(e)

        return results

    def extract_generators(self, source: str) -> dict[str, int]:
        """Extract Hypothesis strategy usage from source code."""
        generators = {}

        # Count strategy usage
        for name, pattern in self.strategy_patterns.items():
            matches = pattern.findall(source)
            if matches:
                generators[f"st.{name}"] = len(matches)

        # Check for composite strategies
        if "@composite" in source or "@st.composite" in source:
            generators["composite"] = source.count("@composite") + source.count(
                "@st.composite"
            )

        # Check for custom strategies
        custom_pattern = re.compile(
            r"def\s+\w+\s*\([^)]*\)\s*->\s*(?:st\.)?SearchStrategy"
        )
        if custom_pattern.search(source):
            generators["custom_strategies"] = len(custom_pattern.findall(source))

        # Check for strategy composition
        composition_patterns = {
            "maps": r"\.map\s*\(",
            "filters": r"\.filter\s*\(",
            "flatmaps": r"\.flatmap\s*\(",
        }

        for name, pattern in composition_patterns.items():
            matches = re.findall(pattern, source)
            if matches:
                generators[f"composition_{name}"] = len(matches)

        return generators

    def extract_features(self, source: str) -> dict[str, int]:
        """Extract Hypothesis feature usage."""
        features = {}

        feature_patterns = {
            "assume": r"\bassume\s*\(",
            "note": r"\bnote\s*\(",
            "event": r"\bevent\s*\(",
            "target": r"\btarget\s*\(",
            "example": r"@example\s*\(",
            "given": r"@given\s*\(",
            "settings": r"@settings\s*\(",
            "reproduce_failure": r"@reproduce_failure\s*\(",
            "seed": r"@seed\s*\(",
            "deadline": r"deadline\s*=",
            "max_examples": r"max_examples\s*=",
            "stateful_step_count": r"stateful_step_count\s*=",
        }

        for feature, pattern in feature_patterns.items():
            matches = re.findall(pattern, source)
            if matches:
                features[feature] = len(matches)

        # Check for hypothesis profiles
        if "settings.register_profile" in source or "register_profile" in source:
            features["profiles"] = True

        # Check for health checks
        if "suppress_health_check" in source or "HealthCheck" in source:
            features["health_checks"] = True

        return features

    def classify_property_types(self, source: str) -> list[str]:
        """Classify the types of properties being tested."""
        detected_types = []

        for prop_type, patterns in self.property_type_patterns.items():
            for pattern in patterns:
                if pattern.search(source):
                    if prop_type not in detected_types:
                        detected_types.append(prop_type)
                    break

        # If no specific type detected, classify as general
        if not detected_types:
            detected_types.append("general")

        return detected_types

    def analyze_ast(self, tree: ast.AST) -> dict[str, Any]:
        """Analyze AST for structural information."""
        ast_info = {"classes": [], "functions": [], "decorators": [], "imports": []}

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                ast_info["classes"].append(
                    {
                        "name": node.name,
                        "bases": [self._get_name(base) for base in node.bases],
                        "decorators": [self._get_name(d) for d in node.decorator_list],
                    }
                )
            elif isinstance(node, ast.FunctionDef):
                ast_info["functions"].append(
                    {
                        "name": node.name,
                        "decorators": [self._get_name(d) for d in node.decorator_list],
                        "args_count": len(node.args.args),
                    }
                )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    ast_info["imports"].append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    ast_info["imports"].append(node.module)

        return ast_info

    def _get_name(self, node: ast.AST) -> str:
        """Extract name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Call):
            return self._get_name(node.func)
        else:
            return str(type(node).__name__)

    def calculate_complexity(self, tree: ast.AST, source: str) -> dict[str, Any]:
        """Calculate various complexity metrics."""
        metrics = {
            "lines_of_code": len(source.splitlines()),
            "num_classes": sum(
                1 for _ in ast.walk(tree) if isinstance(_, ast.ClassDef)
            ),
            "num_functions": sum(
                1 for _ in ast.walk(tree) if isinstance(_, ast.FunctionDef)
            ),
            "num_assertions": source.count("assert "),
            "cyclomatic_complexity": self._calculate_cyclomatic_complexity(tree),
        }

        # Calculate test-specific metrics
        metrics["num_test_methods"] = sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test")
        )

        # Count @given decorators (property tests)
        metrics["num_property_tests"] = sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            for dec in node.decorator_list
            if self._get_name(dec).endswith("given")
        )

        return metrics

    def _calculate_cyclomatic_complexity(self, tree: ast.AST) -> int:
        """Calculate McCabe cyclomatic complexity."""
        complexity = 1  # Base complexity

        for node in ast.walk(tree):
            if isinstance(node, (ast.If, ast.While, ast.For, ast.ExceptHandler)):
                complexity += 1
            elif isinstance(node, ast.BoolOp):
                complexity += len(node.values) - 1

        return complexity

    def detect_patterns(self, source: str) -> dict[str, bool]:
        """Detect specific testing patterns."""
        patterns = {}

        # Check for parametrized testing
        patterns["uses_parametrize"] = "@pytest.mark.parametrize" in source

        # Check for fixtures
        patterns["uses_fixtures"] = (
            "@pytest.fixture" in source or "def fixture" in source
        )

        # Check for setup/teardown
        patterns["has_setup"] = "def setup" in source or "def setUp" in source
        patterns["has_teardown"] = "def teardown" in source or "def tearDown" in source

        # Check for mocking
        patterns["uses_mocking"] = "mock" in source.lower() or "patch" in source

        # Check for async tests
        patterns["has_async_tests"] = "async def test" in source

        # Check for database/IO operations
        patterns["uses_database"] = any(
            db in source.lower() for db in ["database", "db", "sql", "orm"]
        )
        patterns["uses_files"] = any(
            op in source for op in ["open(", "read(", "write("]
        )
        patterns["uses_network"] = any(
            net in source.lower() for net in ["http", "request", "socket", "url"]
        )

        return patterns
