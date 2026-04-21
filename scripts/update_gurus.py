"""Incrementally fetch the latest 13F filing data for tracked gurus."""

from __future__ import annotations

import os

import psycopg2

from tracker.gurus import SEC13FIngestion, ingest_guru_filings, init_schema


def main() -> None:
    dsn = os.environ['DATABASE_URL']
    with psycopg2.connect(dsn) as connection:
        init_schema(connection)
        pipeline = SEC13FIngestion()
        summary = ingest_guru_filings(connection=connection, pipeline=pipeline, per_guru_limit=2)
    print(summary)


if __name__ == '__main__':
    main()
