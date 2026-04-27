#!/usr/bin/env bash
# install_system_deps.sh — Install system dependencies required to build Python via pyenv.
#
# Run once on a fresh Ubuntu/Debian system before `pyenv install`:
#   bash scripts/install_system_deps.sh
#
# Then rebuild Python and reinstall pyperun:
#   pyenv install 3.12.10 --force
#   pip install -e ".[dev]"

set -euo pipefail

echo ">>> Installing system dependencies for pyenv Python build..."
sudo apt-get update -qq
sudo apt-get install -y \
    libffi-dev \
    libbz2-dev \
    libncursesw5-dev \
    libreadline-dev \
    libssl-dev \
    libsqlite3-dev \
    liblzma-dev \
    zlib1g-dev \
    build-essential

echo ""
echo ">>> Done. Now run:"
echo "    pyenv install 3.12.10 --force"
echo "    pip install -e '.[dev]'"
