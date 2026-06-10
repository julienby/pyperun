#!/usr/bin/env python3
"""Thin shim around `pyperun seed-demo` — kept for muscle memory and CI.

The seeding logic now lives in the package (`pyperun/seed.py`) so it ships inside
the Docker image and is exposed as a first-class CLI command:

    pyperun seed-demo                          # seed into the current repo/instance
    pyperun seed-demo --target DIR             # seed into an instance data dir
    pyperun flow demo                          # run the pipeline on it

This script just forwards to that command, so `python scripts/seed_demo.py …`
keeps working from a source checkout.
"""
from __future__ import annotations

import sys

from pyperun.cli import main

if __name__ == "__main__":
    sys.argv = [sys.argv[0], "seed-demo", *sys.argv[1:]]
    main()
