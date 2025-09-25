"""
Test runner module for executing tests in isolated Docker containers.
"""

import os
import json
import tempfile
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import docker
from git import Repo

logger = logging.getLogger(__name__)


class TestRunner:
    """Run tests in isolated Docker containers."""
    
    def __init__(self, docker_image: str = "pbt-analyzer:latest", timeout: int = 300):
        """Initialize test runner with Docker client."""
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.timeout = timeout
    
    def clone_repository(self, repo_url: str, target_dir: Path) -> bool:
        """Clone a repository to the target directory."""
        try:
            # Construct GitHub URL if just owner/repo is provided
            if not repo_url.startswith(('http://', 'https://', 'git@')):
                repo_url = f"https://github.com/{repo_url}.git"
            
            logger.info(f"Cloning repository: {repo_url}")
            Repo.clone_from(repo_url, target_dir, depth=1)
            return True
        except Exception as e:
            logger.error(f"Failed to clone repository {repo_url}: {e}")
            return False
    
    def setup_environment(self, work_dir: Path, requirements: str) -> bool:
        """Set up Python environment with requirements."""
        try:
            # Write requirements to file
            req_file = work_dir / "requirements.txt"
            req_file.write_text(requirements)
            
            # Create setup script
            setup_script = work_dir / "setup.sh"
            setup_script.write_text("""#!/bin/bash
set -e
python -m venv /tmp/venv
source /tmp/venv/bin/activate
pip install --quiet --disable-pip-version-check pytest hypothesis
pip install --quiet --disable-pip-version-check -r requirements.txt
""")
            setup_script.chmod(0o755)
            
            return True
        except Exception as e:
            logger.error(f"Failed to setup environment: {e}")
            return False
    
    def extract_test_code(self, repo_dir: Path, node_id: str) -> Optional[str]:
        """Extract the source code of a specific test."""
        try:
            # Parse node_id (format: path/to/test.py::TestClass::test_method)
            parts = node_id.split("::")
            file_path = repo_dir / parts[0]
            
            if not file_path.exists():
                logger.error(f"Test file not found: {file_path}")
                return None
            
            # Read the entire file for now
            # In a more sophisticated version, we'd parse and extract just the test
            return file_path.read_text()
        except Exception as e:
            logger.error(f"Failed to extract test code: {e}")
            return None
    
    def run_in_container(self, repo_name: str, work_dir: Path, node_ids: List[str]) -> Dict[str, any]:
        """Run tests in a Docker container and collect results."""
        try:
            # Create analysis script
            analysis_script = work_dir / "analyze.py"
            analysis_script.write_text(self._create_analysis_script(node_ids))
            
            # Run container
            container = self.docker_client.containers.run(
                self.docker_image,
                command=[
                    "bash", "-c",
                    "cd /app && ./setup.sh && python analyze.py"
                ],
                volumes={
                    str(work_dir): {'bind': '/app', 'mode': 'rw'}
                },
                remove=False,
                detach=True,
                mem_limit='2g',
                network_mode='none',  # No network access for security
                security_opt=['no-new-privileges'],
            )
            
            # Wait for completion
            result = container.wait(timeout=self.timeout)
            logs = container.logs(stdout=True, stderr=True).decode('utf-8')
            
            # Clean up container
            container.remove()
            
            # Parse results
            results_file = work_dir / "results.json"
            if results_file.exists():
                return json.loads(results_file.read_text())
            else:
                logger.error(f"No results file generated for {repo_name}")
                return {"error": "No results generated", "logs": logs}
            
        except Exception as e:
            logger.error(f"Container execution failed for {repo_name}: {e}")
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
from pathlib import Path

# Activate virtual environment
sys.path.insert(0, '/tmp/venv/lib/python3.11/site-packages')

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
        pattern = rf'\\bst\\.{{strategy}}\\b|\\bstrategies\\.{{strategy}}\\b'
        matches = re.findall(pattern, source)
        if matches:
            generators[f'st.{{strategy}}'] = len(matches)
    
    # Check for composite strategies
    if '@composite' in source or '@st.composite' in source:
        generators['composite'] = True
    
    # Check for custom strategies (basic heuristic)
    if re.search(r'def\\s+\\w+\\s*\\([^)]*\\)\\s*->.*Strategy', source):
        generators['custom_strategies'] = True
    
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
    node_ids = {json.dumps(node_ids)}
    results = {{}}
    
    for node_id in node_ids:
        parts = node_id.split('::')
        file_path = Path(parts[0])
        
        if file_path.exists():
            results[node_id] = analyze_test_file(file_path, node_id)
        else:
            results[node_id] = {{'error': f'File not found: {{file_path}}'}}
    
    # Write results
    with open('results.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == '__main__':
    main()
'''
    
    def process_repository(self, repo_name: str, node_ids: List[str], 
                          requirements: str) -> Dict[str, any]:
        """Process a complete repository."""
        work_dir = None
        try:
            # Create temporary working directory
            work_dir = Path(tempfile.mkdtemp(prefix=f"pbt_analysis_{repo_name.replace('/', '_')}_"))
            
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
                    results[node_id]['source_code'] = code
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to process repository {repo_name}: {e}")
            return {"error": str(e)}
        finally:
            # Clean up temporary directory
            if work_dir and work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)