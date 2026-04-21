"""Compute and persist quarter-over-quarter guru holding changes."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tracker.gurus import compute_and_store_changes, init_schema


def main() -> None:
    init_schema()
    summary = compute_and_store_changes(connection=None)
    print(summary)


if __name__ == '__main__':
    main()
