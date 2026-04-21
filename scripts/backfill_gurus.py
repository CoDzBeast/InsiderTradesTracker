"""Initialize schema and backfill tracked gurus + recent 13F filings."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg2

from tracker.gurus import BackfillOptions, SEC13FIngestion, ingest_guru_filings, init_schema


def main() -> None:
    parser = argparse.ArgumentParser(description='Backfill selected guru 13F filings safely.')
    parser.add_argument('--limit-gurus', type=int, default=None, help='Limit gurus processed in this run.')
    parser.add_argument('--latest-only', action='store_true', help='Process latest quarter only.')
    parser.add_argument(
        '--quarters',
        type=int,
        default=int(os.environ.get('SEC_INITIAL_QUARTERS', '2')),
        help='Number of latest quarters per guru (default from SEC_INITIAL_QUARTERS).',
    )
    parser.add_argument('--resume', action='store_true', help='Resume mode: skip completed filings.')
    parser.add_argument(
        '--config-path',
        default='config/tracked_gurus.json',
        help='Path to tracked gurus JSON config (default: config/tracked_gurus.json).',
    )
    args = parser.parse_args()

    per_guru_limit = 1 if args.latest_only else max(1, args.quarters)
    dsn = os.environ['DATABASE_URL']

    print('[backfill] Starting guru backfill run')
    print(f'[backfill] Config path: {args.config_path}')
    print(f'[backfill] Resume mode: {args.resume}')
    print(f'[backfill] Guru limit: {args.limit_gurus or int(os.environ.get("SEC_GURU_BATCH_SIZE", "20"))}')
    print(f'[backfill] Quarters per guru: {per_guru_limit}')

    with psycopg2.connect(dsn) as connection:
        init_schema(connection)
        pipeline = SEC13FIngestion(config_path=Path(args.config_path))
        summary = ingest_guru_filings(
            connection=connection,
            pipeline=pipeline,
            options=BackfillOptions(
                per_guru_limit=per_guru_limit,
                limit_gurus=args.limit_gurus or int(os.environ.get('SEC_GURU_BATCH_SIZE', '20')),
                resume=args.resume,
            ),
        )
    print(f'[backfill] Completed guru backfill run: {summary}')


if __name__ == '__main__':
    main()
