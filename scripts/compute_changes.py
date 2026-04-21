"""Compute and persist quarter-over-quarter guru holding changes."""

from __future__ import annotations

from tracker.gurus import compute_and_store_changes, init_schema


def main() -> None:
    init_schema()
    summary = compute_and_store_changes(connection=None)
    print(summary)


if __name__ == '__main__':
    main()
