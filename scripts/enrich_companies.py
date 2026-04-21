"""Enrich and classify held companies into internal sector taxonomy."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from tracker.gurus import CompanyEnrichmentService, GuruQueryService, init_schema


def main() -> None:
    parser = argparse.ArgumentParser(description='Enrich held companies from SEC and classify sectors.')
    parser.add_argument('--show-unmapped', action='store_true', help='Print unmapped companies after run.')
    parser.add_argument('--unmapped-limit', type=int, default=100, help='Max unmapped rows to print.')
    args = parser.parse_args()

    init_schema()
    service = CompanyEnrichmentService()
    summary = service.run()
    print(f'[enrich] Completed: {summary}')

    if args.show_unmapped:
        query = GuruQueryService()
        rows = query.get_unmapped_companies(limit=max(1, args.unmapped_limit))
        print(f'[enrich] Unmapped companies ({len(rows)}):')
        for row in rows:
            print(
                f"- {row.get('company_name')} | cusip={row.get('cusip')} | "
                f"sic={row.get('sic_code')} | sic_desc={row.get('sic_description')}"
            )


if __name__ == '__main__':
    main()
