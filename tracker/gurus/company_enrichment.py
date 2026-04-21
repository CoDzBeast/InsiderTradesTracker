"""Company enrichment using SEC submissions + internal sector mapping."""

from __future__ import annotations

from tracker.gurus.classification import SectorClassifier
from tracker.gurus.repository import GuruRepository
from tracker.gurus.sec_13f import SEC13FIngestion


class CompanyEnrichmentService:
    """Enrich holding issuers from SEC data and map to internal sectors."""

    def __init__(self, repo: GuruRepository | None = None, pipeline: SEC13FIngestion | None = None):
        self.repo = repo or GuruRepository()
        self.pipeline = pipeline or SEC13FIngestion()
        self.classifier = SectorClassifier(exact_sic_map=self.repo.get_sic_sector_map())

    def run(self) -> dict[str, int]:
        summary = {'companies_processed': 0, 'classified': 0, 'unmapped': 0}
        companies = self.repo.list_distinct_holding_companies()

        for company in companies:
            issuer_name = str(company.get('issuer_name') or '').strip()
            cusip = (company.get('cusip') or '').strip() or None
            if not issuer_name:
                continue

            cik = self.pipeline.resolve_cik(issuer_name)
            ticker = None
            sec_company_name = issuer_name
            sic_code = None
            sic_description = None

            if cik:
                submissions = self.pipeline.fetch_submissions(cik)
                sec_company_name = str(submissions.get('name') or issuer_name)
                tickers = submissions.get('tickers') or []
                ticker = str(tickers[0]) if tickers else None
                sic_value = submissions.get('sic')
                sic_code = str(sic_value) if sic_value else None
                sic_description = submissions.get('sicDescription')

            classified = self.classifier.classify(
                sic_code=sic_code,
                sic_description=sic_description,
                company_name=sec_company_name,
            )
            self.repo.upsert_company(
                cik=cik,
                ticker=ticker,
                cusip=cusip,
                company_name=sec_company_name,
                sic_code=sic_code,
                sic_description=sic_description,
                sector_bucket=classified.sector_bucket,
                industry_bucket=classified.industry_bucket,
                source='sec_submissions',
                needs_classification=classified.needs_classification,
            )

            summary['companies_processed'] += 1
            if classified.needs_classification:
                summary['unmapped'] += 1
            else:
                summary['classified'] += 1

        return summary
