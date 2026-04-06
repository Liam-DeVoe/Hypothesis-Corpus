"""
Script that runs inside Docker container to install repository and collect pytest nodes.

This script is copied into the repository directory and executed in the Docker container.
It receives configuration through a config.json file that should be present in /app/.
"""

import inspect
import json
import os
import signal
import subprocess
from pathlib import Path

# Load configuration
with open("/app/_install_config.json") as f:
    config = json.load(f)

PRE_INSTALL = config["pre_install"]
POST_INSTALL = config["post_install"]
PYTEST_COLLECTION_TIMEOUT = config["pytest_collection_timeout"]
FROZEN_REINSTALL_REQUIREMENTS = config[
    "frozen_reinstall_requirements"
]  # stored frozen requirements, or None


def pip_install(args):
    cmd = ["uv", "pip", "install", "--system", "--quiet"] + args
    command_str = " ".join(cmd)
    print(f"running: {command_str}", flush=True)

    r = subprocess.run(cmd, cwd="/app/repo", capture_output=True, text=True)

    result = "success" if r.returncode == 0 else "failure"
    print(f"(returncode {r.returncode}) {command_str} result: {result}", flush=True)
    if r.returncode != 0:
        print(f"{command_str} stderr: {r.stderr}", flush=True)
        print(f"{command_str} stdout: {r.stdout}", flush=True)


def try_install_repo():
    """Try various installation methods."""
    source_root = Path("/app/repo")

    # first, try installing through the happy path.
    # Use non-editable install to avoid pytest plugin entry point issues
    pip_install([str(source_root)])

    for extra in ["dev", "test", "tests"]:
        pip_install([f"{source_root}[{extra}]"])

    # regardless of whether this succeeded or not, try installing all requirements
    # we can find, up to one level deep. We need to be able to find eg:
    # * requirements.txt
    # * requirements_test.txt
    # * dependencies/test.txt
    #
    # TODO try parsing as a requirements file first and don't install if not valid syntax
    for p in list(source_root.glob("*.txt")) + list(source_root.glob("*/*.txt")):
        # installing the package results in some egg/dist dirs being created which
        # may contain text files which are interpreted as "valid" requirements files.
        # avoid these.
        #
        #  if p.parent.name.endswith((".eggs", ".egg-info"))
        #
        # as an additional safety, let's ignore any dot dirs. I think you're
        # kind of insane if you use dot dirs to store requirements files,
        # though I may quickly regret these words.
        if (
            p.parent.name.startswith(".")
            or p.parent.name.endswith(".egg-info")
            or p.name.startswith(".")
        ):
            print(f"rejecting possible requirements file {p}", flush=True)
            continue

        if not p.is_file():
            print(
                f"rejecting possible requirements file {p} because it's not a file",
                flush=True,
            )
            continue

        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        lines = [line for line in lines if not line.strip().startswith("#")]
        if len(lines) > 1000:
            print(
                f"rejecting possible requirements file {p} with {len(lines)} lines",
                flush=True,
            )
            # requirement files with more than 1k lines of non-comments are more
            # likely to be wordlists or etc.
            continue
        pip_install(["-r", str(p)])


for package in PRE_INSTALL:
    pip_install([package])

# support reinstalling a repository with a frozen set of requirements, instead of
# re-discovering requirements, which might result in different resolution when rerun than
# earlier.
if FROZEN_REINSTALL_REQUIREMENTS is not None:
    req_path = Path("/app/requirements.txt")
    req_path.write_text(FROZEN_REINSTALL_REQUIREMENTS)
    pip_install(["--no-deps", "-r", str(req_path)])
    pip_install(["--no-deps", str(Path("/app/repo"))])
    for package in POST_INSTALL:
        pip_install([package])
    packages = FROZEN_REINSTALL_REQUIREMENTS.strip().split("\n")
else:
    try_install_repo()

    for package in POST_INSTALL:
        pip_install([package])

    result = subprocess.run(
        ["uv", "pip", "freeze"],
        capture_output=True,
        text=True,
    )

    # remove the top level package, which was installed from a local path.
    # We should only be triggering the @ file: check here, but I've left the -e
    # check just in case.
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

# this deferred import is important: we haven't installed pytest until
# this point, in POST_INSTALL
import pytest


class CollectionPlugin:
    # see pytest_configure in pytest_pbt_analysis/plugin.py
    def pytest_configure(self, config):
        cov_plugin = config.pluginmanager.get_plugin("_cov")
        if cov_plugin is None:
            return

        cov_plugin.options.no_cov = True
        if cov_plugin.cov_controller:
            cov_plugin.cov_controller.pause()

    def __init__(self):
        self.nodeids = []
        self.other_nodeids = []
        self.nodes_source_code = {}
        self.nodes_is_stateful = {}

    def pytest_collection_finish(self, session):
        # defer import to avoid pytest warning about being unable to rewrite an
        # already-imported module (ie, _hypothesis_globals.py).
        from hypothesis import is_hypothesis_test

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

        for item in items:
            try:
                source = inspect.getsource(item.obj)
            except Exception as e:
                print(
                    f"WARNING: failed to get source for {item.nodeid}: {e}",
                    flush=True,
                )
                source = None
            self.nodes_source_code[item.nodeid] = source
            self.nodes_is_stateful[item.nodeid] = hasattr(
                item.obj, "_hypothesis_state_machine_class"
            )


# If this triggers, it will do so in the middle of pytest collection, which will be
# caught by pyest and then pytest will exit with exit code 2. Even though it might
# look like this is going to crash this script, it won't.
#
# We therefore track and report a separate timed_out state.
timed_out = False


def timeout_handler(_signum, _frame):
    global timed_out
    timed_out = True
    raise TimeoutError(
        f"pytest collection timed out after {PYTEST_COLLECTION_TIMEOUT} seconds"
    )


plugin = CollectionPlugin()
os.chdir("/app/repo")

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(PYTEST_COLLECTION_TIMEOUT)
collection_returncode = pytest.main(["--collect-only"], plugins=[plugin])
# clear the alarm
signal.alarm(0)

output = {
    "requirements": "\n".join(packages),
    "node_ids": plugin.nodeids,
    "other_node_ids": plugin.other_nodeids,
    "nodes_source_code": plugin.nodes_source_code,
    "nodes_is_stateful": plugin.nodes_is_stateful,
    "commit_hash": commit_hash,
    "collection_returncode": collection_returncode,
    "timed_out": timed_out,
}

with open("/app/_install_results.json", "w") as f:
    json.dump(output, f)
