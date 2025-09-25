"""
Test script to run the sample analysis without Docker.
This is a simplified version for testing the prototype workflow.
"""

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from analyzer.analysis import PropertyAnalyzer
from analyzer.database import Database


def clone_repository(repo_name: str, target_dir: Path) -> bool:
    """Clone repository for testing."""
    try:
        repo_url = f"https://github.com/{repo_name}.git"
        print(f"Cloning {repo_url}...")
        # Use subprocess instead of GitPython to avoid dependency issues
        result = subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(target_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        else:
            print(f"Git clone error: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error cloning: {e}")
        return False


def analyze_test_file(file_path: Path) -> dict:
    """Analyze a test file for PBT patterns."""
    results = {}

    try:
        source = file_path.read_text()

        # Find generators
        generators = {}
        strategies = ["integers", "floats", "text", "lists", "dictionaries"]
        for strategy in strategies:
            pattern = rf"\bst\.{strategy}\b"
            matches = re.findall(pattern, source)
            if matches:
                generators[f"st.{strategy}"] = len(matches)

        results["generators"] = generators

        # Find features
        features = {}
        feature_patterns = {
            "assume": r"\bassume\s*\(",
            "given": r"@given\s*\(",
        }
        for feature, pattern in feature_patterns.items():
            matches = re.findall(pattern, source)
            if matches:
                features[feature] = len(matches)

        results["features"] = features

        # Classify property type
        if "serialize" in source.lower() or "encode" in source.lower():
            results["property_types"] = ["roundtrip"]
        else:
            results["property_types"] = ["general"]

        results["source_code"] = source[:500]  # Store first 500 chars

    except Exception as e:
        results["error"] = str(e)

    return results


def main():
    """Run sample test."""
    print("🔬 Testing PBT Analysis Prototype with MarkCBell/bigger")
    print("-" * 50)

    # Sample data
    sample_data = {
        "MarkCBell/bigger": {
            "node_ids": ["tests/structures.py::TestUnionFind::runTest"],
            "requirements.txt": "hypothesis==6.112.5\npytest==8.2.2",
        }
    }

    # Initialize database
    db = Database("data/analysis.db")
    print("✅ Database initialized")

    # Initialize analyzer
    analyzer = PropertyAnalyzer()

    # Process the sample repository
    repo_name = "MarkCBell/bigger"
    repo_data = sample_data[repo_name]

    # Add to database
    owner, name = repo_name.split("/")
    repo_id = db.add_repository(owner, name, f"https://github.com/{repo_name}")
    print(f"✅ Repository added to database (ID: {repo_id})")

    # Create temp directory for cloning
    temp_dir = Path(tempfile.mkdtemp(prefix="pbt_test_"))

    try:
        # Clone repository
        if clone_repository(repo_name, temp_dir):
            print("✅ Repository cloned successfully")

            # Analyze each test
            for node_id in repo_data["node_ids"]:
                parts = node_id.split("::")
                file_path = temp_dir / parts[0]

                if file_path.exists():
                    print(f"📝 Analyzing {node_id}...")

                    # Add test to database
                    test_id = db.add_test(repo_id, node_id, parts[0])

                    # Analyze the test file
                    results = analyze_test_file(file_path)

                    # Enhanced analysis with PropertyAnalyzer
                    if "error" not in results:
                        source = file_path.read_text()
                        enhanced = analyzer.analyze_source(source)
                        results.update(enhanced)

                    # Store results in database
                    if "generators" in results:
                        for gen, count in results["generators"].items():
                            db.add_generator_usage(test_id, gen, count)
                        print(f"  - Found {len(results['generators'])} generator types")

                    if "property_types" in results:
                        for prop_type in results["property_types"]:
                            db.add_property_type(test_id, prop_type)
                        print(
                            f"  - Property type: {', '.join(results['property_types'])}"
                        )

                    if "features" in results:
                        for feature, count in results["features"].items():
                            db.add_feature_usage(test_id, feature, count)
                        print(
                            f"  - Features used: {', '.join(results['features'].keys())}"
                        )

                    # Store source code
                    if "source_code" in results:
                        db.add_test_code(test_id, results["source_code"])

                    db.update_test_status(test_id, "success")
                    print("  ✅ Test analysis complete")
                else:
                    print(f"  ❌ Test file not found: {file_path}")
                    db.update_test_status(test_id, "failed", "File not found")

            db.update_repository_status(repo_id, "success")
        else:
            db.update_repository_status(repo_id, "failed", "Clone failed")
            print("❌ Failed to clone repository")

    finally:
        # Cleanup
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

    # Display results
    print("\n" + "=" * 50)
    print("📊 Analysis Results")
    print("=" * 50)

    stats = db.get_analysis_stats()

    print(
        f"\nRepositories: {stats['repositories']['total']} total, "
        f"{stats['repositories']['successful']} successful"
    )
    print(
        f"Tests: {stats['tests']['total']} total, "
        f"{stats['tests']['successful']} successful"
    )

    if stats["top_generators"]:
        print("\nTop Generators:")
        for gen in stats["top_generators"][:5]:
            print(f"  • {gen['generator_name']}: {gen['total_uses']} uses")

    if stats["property_types"]:
        print("\nProperty Types:")
        for prop in stats["property_types"]:
            print(f"  • {prop['property_type']}: {prop['count']} tests")

    if stats["feature_usage"]:
        print("\nFeature Usage:")
        for feature in stats["feature_usage"]:
            print(f"  • {feature['feature_name']}: {feature['total_uses']} uses")

    print("\n" + "=" * 50)
    print("✨ Test complete! View dashboard with: streamlit run dashboard.py")
    print("=" * 50)


if __name__ == "__main__":
    main()
