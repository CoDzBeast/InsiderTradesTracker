"""Run the selected-guru 13F backend pipeline end-to-end."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse

from tracker.gurus import BackfillOptions, CompanyEnrichmentService, SEC13FIngestion, compute_and_store_changes, init_schema, ingest_guru_filings
from tracker.gurus.company_identity import CompanyIdentityService


def main() -> None:
    parser = argparse.ArgumentParser(description='Run DB init -> backfill -> rematch -> enrich -> changes for tracked gurus.')
    parser.add_argument('--quarters', type=int, default=2)
    parser.add_argument('--limit-gurus', type=int, default=None)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--config-path', default='config/tracked_gurus.json')
    parser.add_argument('--rematch-limit', type=int, default=500)
    args = parser.parse_args()

    init_schema()
    pipeline = SEC13FIngestion(config_path=Path(args.config_path))
    ingest_summary = ingest_guru_filings(
        connection=None,
        pipeline=pipeline,
        options=BackfillOptions(
            per_guru_limit=max(1, args.quarters),
            limit_gurus=args.limit_gurus,
            resume=args.resume,
        ),
    )

    identity_summary = CompanyIdentityService().rematch_unresolved_holdings(limit=max(1, args.rematch_limit))
    enrichment_summary = CompanyEnrichmentService().run()
    changes_summary = compute_and_store_changes(connection=None)

    print('[pipeline] ingest:', ingest_summary)
    print('[pipeline] rematch:', identity_summary)
    print('[pipeline] enrich:', enrichment_summary)
    print('[pipeline] changes:', changes_summary)


if __name__ == '__main__':
    main()
