"""
Script that runs inside Docker container to install repository and collect pytest nodes.

This script is copied into the repository directory and executed in the Docker container.
It receives configuration through a config.json file that should be present in /app/.
"""

import json
import os
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
    subprocess.run(cmd, cwd="/app/repo", capture_output=True)


def try_install_repo():
    """Try various installation methods."""
    source_root = Path("/app/repo")

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

# Configure git to trust /app/repo directory (fixes dubious ownership error)
subprocess.run(
    ["git", "config", "--global", "--add", "safe.directory", "/app/repo"],
    capture_output=True,
)

commit_hash = subprocess.run(
    ["git", "rev-parse", "HEAD"],
    capture_output=True,
    text=True,
    cwd="/app/repo",
)
assert commit_hash.returncode == 0
commit_hash = commit_hash.stdout.strip()

# we've done our best to install the package and its dependencies. now
# try collecting tests with pytest

# this deferred import is important: we haven't installed pytest or hypothesis until
# this point, in POST_INSTALL
import pytest
from hypothesis import is_hypothesis_test


class CollectionPlugin:
    def __init__(self):
        self.nodeids = []
        self.other_nodeids = []

    def pytest_collection_finish(self, session):
        items = []
        other_items = []
        for item in session.items:
            # item is likely some custom pytest Item, not a Function item. skip.
            # happens for:
            # * https://github.com/DKISTDC/dkist
            #   * uses https://github.com/asdf-format/asdf (AsdfSchemaItem)
            # * https://github.com/avengerpenguin/kropotkin
            #   * uses https://github.com/realpython/pytest-mypy (MypyItem)
            # * https://github.com/stephen-bunn/groveco_challenge
            #   * uses https://github.com/tholo/pytest-flake8 (Flake8Item)
            if not hasattr(item, "obj"):
                continue

            if is_hypothesis_test(item.obj):
                items.append(item)
            else:
                other_items.append(item)
        self.nodeids = [item.nodeid for item in items]
        self.other_nodeids = [item.nodeid for item in other_items]


plugin = CollectionPlugin()
os.chdir("/app/repo")
collection_returncode = pytest.main(["--collect-only"], plugins=[plugin])
output = {
    "requirements": "\n".join(packages),
    "node_ids": plugin.nodeids,
    "other_node_ids": plugin.other_nodeids,
    "commit_hash": commit_hash,
    "collection_returncode": collection_returncode,
}

with open("/app/_install_results.json", "w") as f:
    json.dump(output, f)
