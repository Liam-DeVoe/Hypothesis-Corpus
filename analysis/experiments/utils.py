import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def filepath_from_node(nodeid: str) -> Path:
    assert ".py" in nodeid
    return Path("/app/repo") / nodeid.split("::")[0]


# require a timeout so we don't forget to specify one and leave a trivial command
# hanging for silly reasons
def subprocess_run(
    args: list[Any],
    timeout: int,
    *,
    identifier=None,
    pre_print=False,
    log_all=False,
    **kwargs,
) -> subprocess.CompletedProcess:
    args = [str(v) for v in args]
    identifier = f"{identifier + ' ' if identifier is not None else ''}"
    # sometimes we want to debug commands before they finish running.
    if pre_print:
        print(f"[pre-printed] {identifier}{' '.join(args)}")

    r = subprocess.run(args, **kwargs, capture_output=True, text=True, timeout=timeout)
    print(f"{identifier}{describe_process(r, all=log_all)}")
    return r


def describe_process(process, *, all=False):
    command = " ".join([str(arg) for arg in process.args])

    def _result(process):
        return "success" if process.returncode == 0 else "failure"

    s = f"(returncode {process.returncode}) {command} result: {_result(process)}"
    if process.returncode != 0 or all:
        s += f"\n{command} stderr: {process.stderr}"
        s += f"\n{command} stdout: {process.stdout}"
    return s


def pip_install(args: list[str]):
    return subprocess_run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
        ]
        + args,
        timeout=60 * 15,
    )


def parse_observability_data(obs_dir: Path) -> dict[str, Any]:
    """Parse Hypothesis observability JSONL files."""
    data = {"coverage": {}, "test_cases": []}

    for jsonl_file in list(obs_dir.glob("**/*.jsonl")):
        with open(jsonl_file) as f:
            for line in f:
                if not line.strip():
                    continue

                entry = json.loads(line)
                if entry["type"] != "test_case":
                    continue

                data["test_cases"].append(entry)

                # Aggregate coverage across all test cases
                if entry["coverage"] is None:
                    entry["coverage"] = {}
                coverage = entry["coverage"]
                for file_path, lines in coverage.items():
                    if file_path not in data["coverage"]:
                        data["coverage"][file_path] = set()
                    data["coverage"][file_path].update(lines)

    # Convert sets to lists for JSON serialization
    for file_path in data["coverage"]:
        data["coverage"][file_path] = sorted(data["coverage"][file_path])

    return data
