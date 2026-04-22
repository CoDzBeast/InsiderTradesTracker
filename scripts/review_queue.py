"""Print unresolved holdings and companies needing manual review."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from tracker.gurus import GuruQueryService, init_schema


def main() -> None:
    parser = argparse.ArgumentParser(description='Show unresolved company matches/review queue for guru backend.')
    parser.add_argument('--holdings-limit', type=int, default=100, help='Number of unresolved holdings to show.')
    parser.add_argument('--companies-limit', type=int, default=100, help='Number of companies needing review to show.')
    args = parser.parse_args()

    init_schema()
    query = GuruQueryService()

    unresolved = query.get_unresolved_holdings(limit=max(1, args.holdings_limit))
    review_companies = query.get_companies_needing_review(limit=max(1, args.companies_limit))

    print(f'[review] Unresolved holdings: {len(unresolved)}')
    for row in unresolved:
        print(
            f"- holding_id={row.get('id')} guru_id={row.get('guru_id')} "
            f"issuer={row.get('issuer_name')} cusip={row.get('cusip')} "
            f"status={row.get('match_status')} confidence={row.get('match_confidence')}"
        )

    print(f'[review] Companies needing review: {len(review_companies)}')
    for row in review_companies:
        print(
            f"- company_id={row.get('id')} name={row.get('company_name')} "
            f"sector={row.get('sector_bucket')} status={row.get('classification_status')}"
        )


if __name__ == '__main__':
    main()
