#!/usr/bin/env bash

# poor man's makefile. We could just use make here,
# but ./build.sh <args> matches hypothesis.

set -o xtrace
set -o errexit
set -o nounset

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

if [ "$1" = "image" ]; then
    docker build -f "$ROOT/analysis/Dockerfile" -t pbt-analysis "$ROOT"
elif [ "$1" = "dashboard" ] || [ "$1" = "collect" ] || [ "$1" = "install" ] || [ "$1" = "experiment" ] || [ "$1" = "task" ]; then
    # Forward recognized commands to run.py
    python3 "$ROOT/run.py" "$@"
else
    echo "Unknown build target $1. Available targets: image, dashboard, collect, install, experiment, task"
    exit 1
fi
