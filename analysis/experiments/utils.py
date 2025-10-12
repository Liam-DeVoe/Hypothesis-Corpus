import subprocess
import sys
from pathlib import Path
from typing import Any


def filepath_from_node(nodeid: str) -> Path:
    assert ".py" in nodeid
    return Path(nodeid.split("::")[0])


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
