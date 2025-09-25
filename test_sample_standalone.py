#!/usr/bin/env python3
"""
Standalone test script to demonstrate the PBT analysis workflow.
This version has minimal dependencies and can run without Docker.
"""

import re
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path


def setup_database(db_path: str):
    """Create a simplified database schema."""
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS repositories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            node_id TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            FOREIGN KEY (repo_id) REFERENCES repositories(id)
        );
        
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_id INTEGER NOT NULL,
            generator_count INTEGER DEFAULT 0,
            feature_count INTEGER DEFAULT 0,
            property_type TEXT,
            source_preview TEXT,
            FOREIGN KEY (test_id) REFERENCES tests(id)
        );
    """
    )
    conn.commit()
    conn.close()
    return db_path


def clone_repository(repo_name: str, target_dir: Path) -> bool:
    """Clone repository using git."""
    try:
        repo_url = f"https://github.com/{repo_name}.git"
        print(f"📦 Cloning {repo_url}...")
        result = subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(target_dir)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error cloning: {e}")
        return False


def analyze_test_file(file_path: Path) -> dict:
    """Analyze a test file for PBT patterns."""
    results = {
        "generators": {},
        "features": {},
        "property_type": "general",
        "source_preview": "",
    }

    try:
        source = file_path.read_text()
        results["source_preview"] = (
            source[:200] + "..." if len(source) > 200 else source
        )

        # Find Hypothesis strategies
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
            "one_of",
            "just",
            "sampled_from",
        ]

        for strategy in strategies:
            pattern = rf"\bst\.{strategy}\s*\("
            matches = re.findall(pattern, source)
            if matches:
                results["generators"][f"st.{strategy}"] = len(matches)

        # Find Hypothesis features
        feature_patterns = {
            "given": r"@given\s*\(",
            "assume": r"\bassume\s*\(",
            "note": r"\bnote\s*\(",
            "event": r"\bevent\s*\(",
            "example": r"@example\s*\(",
        }

        for feature, pattern in feature_patterns.items():
            matches = re.findall(pattern, source)
            if matches:
                results["features"][feature] = len(matches)

        # Classify property type
        if re.search(r"encode.*decode|serialize.*deserialize", source, re.I):
            results["property_type"] = "roundtrip"
        elif re.search(r"commutative|associative|distributive", source, re.I):
            results["property_type"] = "mathematical"
        elif "RuleBasedStateMachine" in source:
            results["property_type"] = "model_based"

    except Exception as e:
        results["error"] = str(e)

    return results


def main():
    """Run the sample test."""
    print("=" * 60)
    print("🔬 PBT Corpus Analysis - Prototype Test")
    print("=" * 60)
    print()

    # Sample data
    sample = {
        "repo": "MarkCBell/bigger",
        "node_ids": ["tests/structures.py::TestUnionFind::runTest"],
        "requirements": "hypothesis==6.112.5\npytest==8.2.2",
    }

    # Setup database
    db_path = "data/test_analysis.db"
    Path("data").mkdir(exist_ok=True)
    setup_database(db_path)
    conn = sqlite3.connect(db_path)

    print(f"📊 Testing with repository: {sample['repo']}")
    print(f"📝 Test nodes: {', '.join(sample['node_ids'])}")
    print()

    # Add repository to database
    cursor = conn.execute(
        "INSERT INTO repositories (name) VALUES (?)", (sample["repo"],)
    )
    repo_id = cursor.lastrowid
    conn.commit()

    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp(prefix="pbt_test_"))

    try:
        # Clone repository
        if clone_repository(sample["repo"], temp_dir):
            print("✅ Repository cloned successfully")
            print()

            # Analyze each test
            for node_id in sample["node_ids"]:
                print(f"🔍 Analyzing: {node_id}")

                # Add test to database
                cursor = conn.execute(
                    "INSERT INTO tests (repo_id, node_id) VALUES (?, ?)",
                    (repo_id, node_id),
                )
                test_id = cursor.lastrowid
                conn.commit()

                # Parse node_id to get file path
                parts = node_id.split("::")
                file_path = temp_dir / parts[0]

                if file_path.exists():
                    # Analyze the test file
                    results = analyze_test_file(file_path)

                    # Store results
                    conn.execute(
                        """
                        INSERT INTO analysis_results 
                        (test_id, generator_count, feature_count, property_type, source_preview)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            test_id,
                            len(results["generators"]),
                            len(results["features"]),
                            results["property_type"],
                            results["source_preview"],
                        ),
                    )

                    # Update test status
                    conn.execute(
                        "UPDATE tests SET status = 'success' WHERE id = ?", (test_id,)
                    )
                    conn.commit()

                    # Display results
                    print(f"  ✓ Generators found: {len(results['generators'])}")
                    if results["generators"]:
                        for gen, count in list(results["generators"].items())[:3]:
                            print(f"    - {gen}: {count} uses")

                    print(f"  ✓ Features used: {len(results['features'])}")
                    if results["features"]:
                        for feat, count in results["features"].items():
                            print(f"    - {feat}: {count} uses")

                    print(f"  ✓ Property type: {results['property_type']}")
                    print()
                else:
                    print(f"  ❌ Test file not found: {file_path}")
                    conn.execute(
                        "UPDATE tests SET status = 'failed' WHERE id = ?", (test_id,)
                    )
                    conn.commit()

            # Update repository status
            conn.execute(
                "UPDATE repositories SET status = 'success' WHERE id = ?", (repo_id,)
            )
            conn.commit()

        else:
            print("❌ Failed to clone repository")
            conn.execute(
                "UPDATE repositories SET status = 'failed' WHERE id = ?", (repo_id,)
            )
            conn.commit()

    finally:
        # Cleanup
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

        # Display summary
        print("=" * 60)
        print("📊 Analysis Summary")
        print("=" * 60)

        # Get statistics from database
        stats = conn.execute(
            """
            SELECT 
                COUNT(DISTINCT r.id) as repos,
                COUNT(DISTINCT t.id) as tests,
                COUNT(DISTINCT CASE WHEN t.status = 'success' THEN t.id END) as successful_tests,
                AVG(ar.generator_count) as avg_generators,
                AVG(ar.feature_count) as avg_features
            FROM repositories r
            LEFT JOIN tests t ON r.id = t.repo_id
            LEFT JOIN analysis_results ar ON t.id = ar.test_id
        """
        ).fetchone()

        print(f"Repositories analyzed: {stats[0]}")
        print(f"Tests analyzed: {stats[1]}")
        print(f"Successful tests: {stats[2]}")
        if stats[3]:
            print(f"Average generators per test: {stats[3]:.1f}")
        if stats[4]:
            print(f"Average features per test: {stats[4]:.1f}")

        print()
        print("✨ Test complete!")
        print("📈 To view the full dashboard, run: streamlit run dashboard.py")
        print("   (Note: The dashboard requires additional dependencies)")

        conn.close()


if __name__ == "__main__":
    main()
