"""Compute and persist quarter-over-quarter guru holding changes."""

from __future__ import annotations

import os

import psycopg2

from tracker.gurus import compute_and_store_changes, init_schema


def main() -> None:
    dsn = os.environ['DATABASE_URL']
    with psycopg2.connect(dsn) as connection:
        init_schema(connection)
        summary = compute_and_store_changes(connection)
    print(summary)


if __name__ == '__main__':
    main()
