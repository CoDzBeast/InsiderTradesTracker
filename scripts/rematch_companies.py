"""Re-run canonical company matching for existing holdings."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from tracker.gurus import init_schema
from tracker.gurus.company_identity import CompanyIdentityService


def main() -> None:
    parser = argparse.ArgumentParser(description='Reprocess company identity matching for holdings.')
    parser.add_argument('--limit', type=int, default=None, help='Optional cap on unresolved holdings to process.')
    args = parser.parse_args()

    init_schema()
    service = CompanyIdentityService()
    summary = service.rematch_unresolved_holdings(limit=args.limit)
    print(f'[identity] Rematch summary: {summary}')


if __name__ == '__main__':
    main()
