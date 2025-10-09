"""
Script that runs inside Docker container to install repository and collect pytest nodes.

This script is copied into the repository directory and executed in the Docker container.
It receives configuration through a config.json file that should be present in /app/.
"""

import json
import subprocess
import sys
from pathlib import Path

# Load configuration
with open("/app/_install_config.json") as f:
    config = json.load(f)

PRE_INSTALL = config["pre_install"]
POST_INSTALL = config["post_install"]


def pip_install(args):
    """Run pip install."""
    cmd = [sys.executable, "-m", "pip", "install", "--quiet"] + args
    subprocess.run(cmd, cwd="/app", capture_output=True)


def try_install_repo():
    """Try various installation methods."""
    source_root = Path("/app")

    # Try installing via -e
    pip_install(["-e", str(source_root)])

    # Try common extras
    for extra in ["dev", "test", "tests"]:
        pip_install(["-e", f"{source_root}[{extra}]"])

    # Try requirements files
    for p in list(source_root.glob("*.txt")) + list(source_root.glob("*/*.txt")):
        if p.parent.name.startswith("."):
            continue
        pip_install(["-r", str(p)])


# Install pre-install packages
for package in PRE_INSTALL:
    pip_install([package])

# Try to install repository
try_install_repo()

# Install post-install packages
for package in POST_INSTALL:
    pip_install([package])

# Get installed packages
result = subprocess.run(
    [sys.executable, "-m", "pip", "freeze"],
    capture_output=True,
    text=True,
)
packages = [
    line
    for line in result.stdout.strip().split("\n")
    if not (line.startswith("-e") or " @ file:" in line)
]

# Collect pytest nodes
result = subprocess.run(
    [sys.executable, "-m", "pytest", "--collect-only", "-q", "/app"],
    capture_output=True,
    text=True,
    cwd="/app",
)

node_ids = []
for line in result.stdout.splitlines():
    line = line.strip()
    if "::" in line and not line.startswith("<"):
        node_ids.append(line)

# Write results
output = {
    "requirements": "\n".join(packages),
    "node_ids": node_ids,
    "collection_returncode": result.returncode,
}

with open("/app/_install_results.json", "w") as f:
    json.dump(output, f)
