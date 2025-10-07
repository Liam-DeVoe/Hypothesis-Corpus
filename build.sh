#!/usr/bin/env bash

# poor man's makefile. We could just use make here,
# but ./build.sh <args> matches hypothesis.

set -o xtrace
set -o errexit
set -o nounset

ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

if [ "$1" = "dashboard" ]; then
    streamlit run "$ROOT/dashboard/Overview.py"
elif [ "$1" = "image" ]; then
    docker build -f "$ROOT/analyzer/Dockerfile" -t pbt-analyzer "$ROOT"
else
    echo "Unknown build target $1. Available targets: dashboard, image"
    exit 1
fi
