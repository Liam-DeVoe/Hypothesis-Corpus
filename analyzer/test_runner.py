"""
Test runner module for executing tests in isolated Docker containers.
"""

import json
import logging
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import docker

logger = logging.getLogger(__name__)


class TestRunner:
    """Run tests in isolated Docker containers."""

    # 1 hour
    RUNNER_TIMEOUT = 60 * 60

    def __init__(self, docker_image: str = "pbt-analyzer:latest", worker_id: Optional[int] = None):
        """Initialize test runner with Docker client."""
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.worker_id = worker_id

    def clone_repository(self, repo_url: str, target_dir: Path) -> bool:
        """Clone a repository to the target directory."""
        try:
            # Construct GitHub URL if just owner/repo is provided
            if not repo_url.startswith(("http://", "https://", "git@")):
                repo_url = f"https://github.com/{repo_url}.git"

            logger.info(f"[w{self.worker_id}] Cloning repository: {repo_url}")
            # Use subprocess to run git clone command
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(target_dir)],
                capture_output=True,
                text=True,
                check=True,
            )
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"[w{self.worker_id}] Failed to clone repository {repo_url}: {e.stderr}")
            return False
        except Exception as e:
            logger.error(f"[w{self.worker_id}] Failed to clone repository {repo_url}: {e}")
            return False

    def setup_environment(self, work_dir: Path, requirements: str) -> bool:
        """Set up Python environment with requirements."""
        try:
            # Write requirements to file
            req_file = work_dir / "requirements.txt"
            req_file.write_text(requirements)

            # Create setup script following the template to handle dependency conflicts
            setup_script = work_dir / "setup.sh"
            setup_script.write_text(
                """#!/bin/bash
set -e

# Write progress updates as simple strings
echo 'Installing pytest and hypothesis...' > /app/progress.json

# Install core testing requirements
pip install --quiet --disable-pip-version-check pytest hypothesis

echo 'Installing project requirements...' > /app/progress.json

# Install requirements with --no-dependencies to avoid version conflicts
# This is safe because we're reproducing a frozen environment where all deps
# were already resolved during pytest collection
pip install --quiet --disable-pip-version-check --no-dependencies -r requirements.txt

echo 'Installing repository package...' > /app/progress.json

# Always try to install the repository itself as a library
# This ensures imports like 'from mylibrary import func_under_test' work
# Use --no-dependencies to avoid conflicts and ignore failures for non-libraries
pip install --quiet --disable-pip-version-check --no-dependencies -e . 2>/dev/null || true

echo 'Setup complete, starting analysis...' > /app/progress.json
"""
            )
            setup_script.chmod(0o755)

            return True
        except Exception as e:
            logger.error(f"[w{self.worker_id}] Failed to setup environment: {e}")
            return False

    def _monitor_container_progress(self, container, work_dir: Path, repo_name: str, worker_id: Optional[int] = None):
        """Monitor container progress by reading progress.json file."""
        progress_file = work_dir / "progress.json"
        last_message_seen = None

        # Format worker prefix
        worker_prefix = f"[w{worker_id}]" if worker_id is not None else f"[{repo_name}]"

        while True:
            try:
                # Check container status
                container.reload()
                if container.status not in ["running", "created"]:
                    break

                # Read progress file if it exists
                if progress_file.exists():
                    with open(progress_file) as f:
                        content = f.read().strip()

                    # If the message changed, log it
                    if content != last_message_seen:
                        logger.info(f"{worker_prefix} {repo_name}: {content}")
                        last_message_seen = content

                time.sleep(5)
            except Exception as e:
                logger.debug(f"[w{self.worker_id}] Error monitoring progress for {repo_name}: {e}")
                break

    def extract_test_code(self, repo_dir: Path, node_id: str) -> Optional[str]:
        """Extract the source code of a specific test."""
        try:
            # Parse node_id (format: path/to/test.py::TestClass::test_method)
            parts = node_id.split("::")
            file_path = repo_dir / parts[0]

            if not file_path.exists():
                logger.error(f"[w{self.worker_id}] Test file not found: {file_path}")
                return None

            # Read the entire file for now
            # In a more sophisticated version, we'd parse and extract just the test
            return file_path.read_text()
        except Exception as e:
            logger.error(f"[w{self.worker_id}] Failed to extract test code: {e}")
            return None

    def run_in_container(
        self, repo_name: str, work_dir: Path, node_ids: List[str]
    ) -> Dict[str, any]:
        """Run tests in a Docker container and collect results."""
        try:
            # Create analysis script
            analysis_script = work_dir / "analyze.py"
            analysis_script.write_text(self._create_analysis_script(node_ids))

            # Run container with observability enabled
            container = self.docker_client.containers.run(
                self.docker_image,
                command=[
                    "bash",
                    "-xc",
                    "cd /app && echo 'Starting setup...' && ./setup.sh && echo 'Running analysis...' && python analyze.py 2>&1",
                ],
                volumes={str(work_dir): {"bind": "/app", "mode": "rw"}},
                environment={
                    "HYPOTHESIS_EXPERIMENTAL_OBSERVABILITY": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                remove=False,
                detach=True,
                mem_limit="2g",
                security_opt=["no-new-privileges"],
            )

            # Start progress monitoring in a separate thread
            monitor_thread = threading.Thread(
                target=self._monitor_container_progress,
                args=(container, work_dir, repo_name, self.worker_id),
                daemon=True,
            )
            monitor_thread.start()

            # Wait for completion
            result = container.wait(timeout=self.RUNNER_TIMEOUT)
            logs = container.logs(stdout=True, stderr=True).decode("utf-8")

            # Log the container output for debugging
            logger.debug(f"[w{self.worker_id}] Container logs for {repo_name}:\n{logs}")
            logger.debug(f"[w{self.worker_id}] Container exit code: {result.get('StatusCode', 'unknown')}")

            # Clean up container
            container.remove()

            # Parse results
            results_file = work_dir / "results.json"
            if results_file.exists():
                return json.loads(results_file.read_text())
            else:
                logger.error(f"[w{self.worker_id}] No results file generated for {repo_name}")
                logger.error(f"[w{self.worker_id}] Container logs:\n{logs}")
                return {"error": "No results generated", "logs": logs}

        except Exception as e:
            logger.error(f"[w{self.worker_id}] Container execution failed for {repo_name}: {e}")
            return {"error": str(e)}

    def _create_analysis_script(self, node_ids: List[str]) -> str:
        """Create Python script to run inside container for analysis."""
        return f'''#!/usr/bin/env python
"""
Analysis script to run inside Docker container.
"""

import sys
import json
import ast
import re
import subprocess
import shutil
import traceback
from pathlib import Path
from typing import Dict, Any, List

print("Script starting...", flush=True)
print(f"sys.executable: {{sys.executable}}", flush=True)

def run_test_with_coverage(node_id: str) -> Dict[str, Any]:
    """Run a single test and collect coverage information."""
    results = {{
        'node_id': node_id,
        'coverage': {{}},
        'observability_data': {{}},
        'test_result': None,
        'error': None
    }}
    # 5 minute timeout per test
    test_timeout = 60 * 5

    try:
        # Clear any previous observability data
        obs_dir = Path('/app/.hypothesis/observed')
        if obs_dir.exists():
            shutil.rmtree(obs_dir)

        # Run the specific test with pytest
        # Need to be in the repo directory for pytest to find the tests
        cmd = [
            'python', '-m', 'pytest',
            node_id,
            '-xvs',  # Stop on first failure, verbose, no capture
            '--tb=short'  # Short traceback format
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd='/app',  # Run from the app directory where the repo is mounted
            timeout=test_timeout
        )

        results['test_result'] = {{
            'exit_code': result.returncode,
            'stdout': result.stdout[-5000:] if result.stdout else '',  # Last 5000 chars
            'stderr': result.stderr[-5000:] if result.stderr else '',
            'passed': result.returncode == 0
        }}

        # Parse observability data if it exists
        if obs_dir.exists():
            results['observability_data'] = parse_observability_data(obs_dir)

    except subprocess.TimeoutExpired:
        results['error'] = f'Test timed out after {{test_timeout}} seconds'
    except Exception as e:
        results['error'] = str(e)

    return results

def parse_observability_data(obs_dir: Path) -> Dict[str, Any]:
    """Parse Hypothesis observability JSONL files."""
    data = {{
        'coverage': {{}},  # Aggregate coverage across all test cases
        'timing': {{}},
        'examples': [],
        'metadata': {{}},
        'test_cases': []  # Individual test case data with coverage
    }}

    try:
        # Hypothesis writes to .jsonl files in jsonlines format
        jsonl_files = list(obs_dir.glob('**/*.jsonl'))

        for jsonl_file in jsonl_files:
            try:
                with open(jsonl_file, 'r') as f:
                    for line_num, line in enumerate(f):
                        line = line.strip()
                        if line:
                            entry = json.loads(line)
                            # Store each test case with its order
                            entry['case_number'] = line_num
                            process_observability_entry(entry, data)
            except (json.JSONDecodeError, IOError) as e:
                data['parse_error'] = f"Error reading {{jsonl_file}}: {{e}}"

    except Exception as e:
        data['parse_error'] = str(e)

    return data

def process_observability_entry(entry: Dict, data: Dict) -> None:
    """Process a single observability entry."""
    # Store individual test case with its coverage
    if 'type' in entry and entry['type'] == 'test_case':
        test_case = {{
            'case_number': entry.get('case_number', 0),
            'args': entry.get('arguments', {{}}),
            'result': entry.get('status'),
            'timing': entry.get('timing'),
            'coverage': {{}}
        }}

        # Extract coverage for this test case
        if 'coverage' in entry:
            coverage = entry['coverage']
            if isinstance(coverage, dict):
                for file_path, lines in coverage.items():
                    if isinstance(lines, list):
                        test_case['coverage'][file_path] = lines

                        # Also update aggregate coverage
                        if file_path not in data['coverage']:
                            data['coverage'][file_path] = set()
                        data['coverage'][file_path].update(lines)

        data['test_cases'].append(test_case)

        # Legacy: store in examples too for compatibility
        data['examples'].append({{
            'args': test_case['args'],
            'result': test_case['result'],
            'timing': test_case['timing']
        }})

    # Extract coverage information if not a test case entry
    elif 'coverage' in entry:
        coverage = entry['coverage']
        if isinstance(coverage, dict):
            for file_path, lines in coverage.items():
                if file_path not in data['coverage']:
                    data['coverage'][file_path] = set()
                if isinstance(lines, list):
                    data['coverage'][file_path].update(lines)

    # Extract timing information
    if 'timing' in entry:
        data['timing'].update(entry['timing'])

    # Store metadata
    if 'metadata' in entry:
        data['metadata'].update(entry['metadata'])

def analyze_test_file(file_path, node_ids):
    """Analyze a test file for PBT patterns."""
    results = {{}}

    try:
        with open(file_path, 'r') as f:
            source = f.read()

        # Parse AST
        tree = ast.parse(source)

        # Analyze for patterns
        results['generators'] = find_generators(source)
        results['features'] = find_features(source)
        results['property_types'] = classify_property(source)
        results['test_runner'] = detect_test_runner(file_path)

    except Exception as e:
        results['error'] = str(e)

    return results

def find_generators(source):
    """Find Hypothesis strategy usage."""
    generators = {{}}

    # Common Hypothesis strategies
    strategies = [
        'integers', 'floats', 'text', 'binary', 'booleans',
        'lists', 'dictionaries', 'tuples', 'sets', 'frozensets',
        'one_of', 'none', 'just', 'sampled_from', 'permutations',
        'datetimes', 'dates', 'times', 'timedeltas', 'uuids',
        'emails', 'urls', 'ip_addresses'
    ]

    for strategy in strategies:
        pattern = r'\\bst\\.' + strategy + r'\\b|\\bstrategies\\.' + strategy + r'\\b'
        matches = re.findall(pattern, source)
        if matches:
            generators['st.' + strategy] = len(matches)

    # Check for composite strategies
    if '@composite' in source or '@st.composite' in source:
        generators['composite'] = True

    # Check for custom strategies (basic heuristic)
    if re.search(r'def\\s+\\w+\\s*\\([^)]*\\)\\s*->.*Strategy', source):
        generators['custom_strategies'] = True

    # Check for data() strategy
    if 'data()' in source or '.data()' in source:
        generators['st.data'] = source.count('data()')

    return generators

def find_features(source):
    """Find usage of Hypothesis features."""
    features = {{}}

    feature_patterns = {{
        'assume': r'\\bassume\\s*\\(',
        'note': r'\\bnote\\s*\\(',
        'event': r'\\bevent\\s*\\(',
        'target': r'\\btarget\\s*\\(',
        'example': r'@example\\s*\\(',
        'given': r'@given\\s*\\(',
        'settings': r'@settings\\s*\\(',
        'max_examples': r'max_examples\\s*=',
    }}

    for feature, pattern in feature_patterns.items():
        matches = re.findall(pattern, source)
        if matches:
            features[feature] = len(matches)

    return features

def classify_property(source):
    """Classify the type of property being tested."""
    types = []

    # Mathematical properties
    math_patterns = [
        (r'commutative|associative|distributive|identity', 'mathematical'),
        (r'inverse|idempotent|transitive', 'mathematical'),
    ]

    # Round-trip properties
    roundtrip_patterns = [
        (r'encode.*decode|decode.*encode', 'roundtrip'),
        (r'serialize.*deserialize|deserialize.*serialize', 'roundtrip'),
        (r'dump.*load|load.*dump', 'roundtrip'),
        (r'parse.*format|format.*parse', 'roundtrip'),
    ]

    # Model-based testing
    model_patterns = [
        (r'RuleBasedStateMachine', 'model_based'),
        (r'@rule\\s*\\(', 'model_based'),
        (r'@invariant\\s*\\(', 'model_based'),
    ]

    all_patterns = math_patterns + roundtrip_patterns + model_patterns

    for pattern, prop_type in all_patterns:
        if re.search(pattern, source, re.IGNORECASE):
            if prop_type not in types:
                types.append(prop_type)

    # If no specific type detected, mark as general
    if not types:
        types.append('general')

    return types

def detect_test_runner(file_path):
    """Detect which test runner is being used."""
    runner_info = {{}}

    # Check for pytest
    if 'pytest' in str(file_path) or file_path.name.startswith('test_'):
        runner_info['framework'] = 'pytest'

    # Check for test directory structure
    parts = file_path.parts
    if 'tests' in parts:
        runner_info['test_dir'] = 'tests'
    elif 'test' in parts:
        runner_info['test_dir'] = 'test'

    return runner_info

# Main execution
def main():
    import traceback
    try:
        print("Starting analysis...")
        print(f"Python version: {{sys.version}}")
        print(f"Current directory: {{Path.cwd()}}")
        node_ids = {json.dumps(node_ids)}
        print(f"Node IDs to process: {{node_ids}}")
        results = {{}}

        total_tests = len(node_ids)
        for i, node_id in enumerate(node_ids, 1):
            parts = node_id.split('::')
            file_path = Path(parts[0])

            # Write progress file for external monitoring
            progress_file = Path('/app/progress.json')
            test_name = parts[-1] if len(parts) > 1 else parts[0]
            with open(progress_file, 'w') as f:
                f.write(f'Test {{i}}/{{total_tests}}: {{test_name}}')

            print(f"\\nProcessing test {{i}}/{{total_tests}}: {{node_id}}")
            print(f"Looking for file: {{file_path}}")

            if file_path.exists():
                print(f"Found file: {{file_path}}")

                # First analyze the test file for patterns
                analysis_results = analyze_test_file(file_path, node_id)

                # Then run the test to collect coverage
                print(f"Running test with coverage...")
                coverage_results = run_test_with_coverage(node_id)

                # Combine results
                results[node_id] = {{
                    'analysis': analysis_results,
                    'coverage': coverage_results,
                    'file_path': str(file_path)
                }}

                print(f"Test result: {{coverage_results.get('test_result', {{}}).get('passed', False)}}")
                if coverage_results.get('observability_data', {{}}).get('coverage'):
                    coverage_files = len(coverage_results['observability_data']['coverage'])
                    print(f"Coverage data collected for {{coverage_files}} files")
            else:
                print(f"File not found: {{file_path}}")
                results[node_id] = {{'error': f'File not found: {{file_path}}'}}

        # Convert sets to lists for JSON serialization
        for node_id in results:
            if 'coverage' in results[node_id]:
                cov_data = results[node_id]['coverage']
                if 'observability_data' in cov_data and 'coverage' in cov_data['observability_data']:
                    for file_path in cov_data['observability_data']['coverage']:
                        if isinstance(cov_data['observability_data']['coverage'][file_path], set):
                            cov_data['observability_data']['coverage'][file_path] = list(
                                cov_data['observability_data']['coverage'][file_path]
                            )

        # Write results
        print(f"\\nWriting results to results.json")
        with open('/app/progress.json', 'w') as f:
            f.write('Writing results...')
        with open('results.json', 'w') as f:
            json.dump(results, f, indent=2)
        with open('/app/progress.json', 'w') as f:
            f.write('Analysis complete')
        print("Analysis complete")
    except Exception as e:
        print(f"ERROR in main: {{e}}")
        print(f"Traceback: {{traceback.format_exc()}}")
        # Still try to write partial results
        with open('results.json', 'w') as f:
            json.dump({{"error": str(e), "traceback": traceback.format_exc()}}, f, indent=2)

if __name__ == '__main__':
    main()
'''

    def process_repository(
        self, repo_name: str, node_ids: List[str], requirements: str
    ) -> Dict[str, any]:
        """Process a complete repository."""
        work_dir = None
        try:
            # Create temporary working directory
            work_dir = Path(
                tempfile.mkdtemp(prefix=f"pbt_analysis_{repo_name.replace('/', '_')}_")
            )

            # Clone repository
            if not self.clone_repository(repo_name, work_dir / "repo"):
                return {"error": "Failed to clone repository"}

            # Setup environment
            repo_dir = work_dir / "repo"
            if not self.setup_environment(repo_dir, requirements):
                return {"error": "Failed to setup environment"}

            # Run analysis in container
            results = self.run_in_container(repo_name, repo_dir, node_ids)

            # Extract test code for each node_id
            for node_id in node_ids:
                code = self.extract_test_code(repo_dir, node_id)
                if code and node_id in results:
                    results[node_id]["source_code"] = code

            return results

        except Exception as e:
            logger.error(f"[w{self.worker_id}] Failed to process repository {repo_name}: {e}")
            return {"error": str(e)}
        finally:
            # Clean up temporary directory
            if work_dir and work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)
