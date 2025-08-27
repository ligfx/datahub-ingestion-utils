#!/bin/sh

set -euxo pipefail

cd "$(dirname "$0")/.."
ruff format src
ruff check src --fix
mypy src
