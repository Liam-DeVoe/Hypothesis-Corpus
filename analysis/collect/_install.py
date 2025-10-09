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

    # first, try installing through the happy path.
    pip_install(["-e", str(source_root)])

    for extra in ["dev", "test", "tests"]:
        pip_install(["-e", f"{source_root}[{extra}]"])

    # regardless of whether this succeeded or not, try installing all requirements
    # we can find, up to one level deep. We need to be able to find eg:
    # * requirements.txt
    # * requirements_test.txt
    # * dependencies/test.txt
    #
    # TODO try parsing as a requirements file first and don't install if not valid syntax
    for p in list(source_root.glob("*.txt")) + list(source_root.glob("*/*.txt")):
        # installing as editable results in some egg dirs being created which
        # may contain text files which are interpreted as "valid" requirements files.
        # avoid these.
        #
        #  if p.parent.name.endswith((".eggs", ".egg-info"))
        #
        # as an additional safety, let's ignore any dot dirs. I think you're
        # kind of insane if you use dot dirs to store requirements files,
        # though I may quickly regret these words.
        if p.parent.name.startswith("."):
            continue
        pip_install(["-r", str(p)])


for package in PRE_INSTALL:
    pip_install([package])

try_install_repo()

for package in POST_INSTALL:
    pip_install([package])

result = subprocess.run(
    [sys.executable, "-m", "pip", "freeze"],
    capture_output=True,
    text=True,
)

# remove the top level package, which was installed as editable from a path.
# We should only be triggering the -e check here, but I've left @ file: just
# in case.
packages = [
    line
    for line in result.stdout.strip().split("\n")
    if not (line.startswith("-e") or " @ file:" in line)
]

# we've done our best to install the package and its dependencies. now
# try collecting tests with pytest
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

node_ids.append(result.stdout)
# Write results
output = {
    "requirements": "\n".join(packages),
    "node_ids": node_ids,
    "collection_returncode": result.returncode,
}

with open("/app/_install_results.json", "w") as f:
    json.dump(output, f)
