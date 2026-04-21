"""Guru-focused 13F tracking package."""

from tracker.db import init_db as init_schema

__all__ = [
    'init_schema',
    'compute_and_store_changes',
    'GuruQueryService',
    'BackfillOptions',
    'SEC13FIngestion',
    'ingest_guru_filings',
    'CompanyEnrichmentService',
    'CompanyIdentityService',
]


def __getattr__(name):
    if name == 'compute_and_store_changes':
        from tracker.gurus.changes import compute_and_store_changes

        return compute_and_store_changes
    if name == 'GuruQueryService':
        from tracker.gurus.queries import GuruQueryService

        return GuruQueryService
    if name in {'BackfillOptions', 'SEC13FIngestion', 'ingest_guru_filings'}:
        from tracker.gurus.sec_13f import BackfillOptions, SEC13FIngestion, ingest_guru_filings

        return {
            'BackfillOptions': BackfillOptions,
            'SEC13FIngestion': SEC13FIngestion,
            'ingest_guru_filings': ingest_guru_filings,
        }[name]
    if name == 'CompanyEnrichmentService':
        from tracker.gurus.company_enrichment import CompanyEnrichmentService

        return CompanyEnrichmentService
    if name == 'CompanyIdentityService':
        from tracker.gurus.company_identity import CompanyIdentityService

        return CompanyIdentityService
    raise AttributeError(name)
