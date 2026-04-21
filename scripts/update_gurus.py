"""Incrementally fetch the latest 13F filing data for tracked gurus."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tracker.gurus import BackfillOptions, SEC13FIngestion, ingest_guru_filings, init_schema


def main() -> None:
    init_schema()
    pipeline = SEC13FIngestion()
    summary = ingest_guru_filings(
        connection=None,
        pipeline=pipeline,
        options=BackfillOptions(per_guru_limit=2, limit_gurus=None, resume=True),
    )
    print(summary)


if __name__ == '__main__':
    main()
