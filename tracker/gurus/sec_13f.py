"""SEC EDGAR 13F ingestion pipeline for tracked gurus."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as ET

from common.logging import Logger
from defs import PROJECT_PATH
from tracker.gurus.sec_client import SECRequestClient, SECRequestConfig

logger = Logger('gurus.13f').get_logger()


@dataclass(slots=True)
class FilingRecord:
    accession_number: str
    filing_date: date
    report_period: date | None
    form_type: str
    index_url: str
    information_table_url: str


@dataclass(slots=True)
class HoldingRecord:
    issuer_name: str
    cusip: str
    shares: Decimal
    value_usd: Decimal


@dataclass(slots=True)
class BackfillOptions:
    per_guru_limit: int = 2
    limit_gurus: int | None = None
    resume: bool = True


class SEC13FIngestion:
    """Ingest 13F filings and holdings for configured gurus."""

    def __init__(self, config_path: Path | None = None, request_config: SECRequestConfig | None = None):
        self.config_path = config_path or PROJECT_PATH / 'config' / 'tracked_gurus.json'
        self.request_config = request_config or SECRequestConfig(
            user_agent=os.environ.get('SEC_USER_AGENT', 'GuruTracker/0.1 your_email@example.com'),
            timeout_seconds=int(os.environ.get('SEC_TIMEOUT_SECONDS', '20')),
            max_retries=int(os.environ.get('SEC_MAX_RETRIES', '5')),
            base_delay_seconds=float(os.environ.get('SEC_BASE_DELAY_SECONDS', '1.0')),
            backoff_base_seconds=float(os.environ.get('SEC_BACKOFF_BASE_SECONDS', '2.0')),
            max_backoff_seconds=float(os.environ.get('SEC_MAX_BACKOFF_SECONDS', '60')),
            enable_cache=os.environ.get('SEC_ENABLE_CACHE', 'true').lower() == 'true',
        )
        self.client = SECRequestClient(config=self.request_config)

    def load_tracked_gurus(self) -> list[dict[str, str]]:
        with self.config_path.open(mode='r', encoding='utf-8') as handle:
            return json.load(handle)

    def resolve_cik(self, manager_name: str) -> str | None:
        """Resolve manager CIK via SEC company search endpoint."""
        query = manager_name.replace(' ', '+')
        url = (
            'https://www.sec.gov/cgi-bin/browse-edgar?'
            f'company={query}&owner=exclude&action=getcompany&output=atom'
        )
        text = self.client.get_content(url).decode('utf-8', errors='ignore')
        marker = '<cik>'
        start = text.find(marker)
        if start == -1:
            return None

        start += len(marker)
        end = text.find('</cik>', start)
        if end == -1:
            return None

        return text[start:end].strip().zfill(10)

    def fetch_submissions(self, cik: str) -> dict:
        return self.client.get_json(
            f'https://data.sec.gov/submissions/CIK{cik}.json',
            cache_key=f'submissions/CIK{cik}.json',
        )

    def latest_13f_filings(self, submissions: dict, limit: int = 2) -> list[FilingRecord]:
        recent = submissions.get('filings', {}).get('recent', {})
        forms = recent.get('form', [])
        accession_numbers = recent.get('accessionNumber', [])
        filing_dates = recent.get('filingDate', [])
        report_dates = recent.get('reportDate', [])

        filings: list[FilingRecord] = []
        cik = str(submissions.get('cik', '')).zfill(10)

        for idx, form_type in enumerate(forms):
            if form_type != '13F-HR':
                continue

            accession_number = accession_numbers[idx]
            accession_nodash = accession_number.replace('-', '')
            index_url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}/index.json'
            info_table_url = self.find_information_table(cik, accession_number, index_url)
            if info_table_url is None:
                continue

            report_period = None
            if report_dates[idx]:
                report_period = date.fromisoformat(report_dates[idx])

            filings.append(
                FilingRecord(
                    accession_number=accession_number,
                    filing_date=date.fromisoformat(filing_dates[idx]),
                    report_period=report_period,
                    form_type=form_type,
                    index_url=index_url,
                    information_table_url=info_table_url,
                )
            )

            if len(filings) >= limit:
                break

        return filings

    def find_information_table(self, cik: str, accession_number: str, filing_index_json_url: str) -> str | None:
        accession_nodash = accession_number.replace('-', '')
        payload = self.client.get_json(
            filing_index_json_url,
            cache_key=f'filings/{cik}/{accession_nodash}/index.json',
        )
        items = payload.get('directory', {}).get('item', [])
        for item in items:
            filename = item.get('name', '')
            lower_name = filename.lower()
            if 'informationtable' in lower_name and lower_name.endswith('.xml'):
                return filing_index_json_url.replace('index.json', filename)

        for item in items:
            filename = item.get('name', '')
            if filename.lower().endswith('.xml'):
                return filing_index_json_url.replace('index.json', filename)

        return None

    def parse_information_table(self, cik: str, accession_number: str, information_table_url: str) -> list[HoldingRecord]:
        xml_content = self.client.get_content(
            information_table_url,
            cache_key=f'filings/{cik}/{accession_number.replace("-", "")}/informationTable.xml',
        )

        root = ET.fromstring(xml_content)
        ns = {'n': 'http://www.sec.gov/edgar/document/thirteenf/informationtable'}

        records: list[HoldingRecord] = []

        for info_table in root.findall('n:infoTable', ns):
            issuer_name = _get_xml_text(info_table, 'n:nameOfIssuer', ns)
            cusip = _get_xml_text(info_table, 'n:cusip', ns)
            value_thousands = _get_xml_text(info_table, 'n:value', ns)
            shares = _get_xml_text(info_table, 'n:shrsOrPrnAmt/n:sshPrnamt', ns)

            records.append(
                HoldingRecord(
                    issuer_name=issuer_name,
                    cusip=cusip,
                    shares=Decimal(shares or '0'),
                    value_usd=Decimal(value_thousands or '0') * Decimal(1000),
                )
            )

        return records


# pylint: disable=too-many-branches,too-many-locals

def ingest_guru_filings(connection, pipeline: SEC13FIngestion, options: BackfillOptions | None = None) -> dict[str, int]:
    """Load gurus from config and store selected 13F filings + holdings safely."""

    from tracker.gurus.repository import GuruRepository

    backfill_options = options or BackfillOptions()
    repo = GuruRepository(connection)
    summary = {
        'gurus_processed': 0,
        'filings_fetched': 0,
        'holdings_upserted': 0,
        'cached_hits': 0,
        'failures': 0,
        'skipped': 0,
    }

    gurus = pipeline.load_tracked_gurus()
    if backfill_options.limit_gurus is not None:
        gurus = gurus[:backfill_options.limit_gurus]
    logger.info('Loaded %s tracked gurus from %s', len(gurus), pipeline.config_path)

    for guru_index, guru in enumerate(gurus, start=1):
        guru_name = guru['guru_name']
        manager_name = guru['manager_name']
        logger.info('Processing guru %s/%s: %s (%s)', guru_index, len(gurus), guru_name, manager_name)

        guru_id = repo.upsert_guru(guru_name=guru_name, manager_name=manager_name)

        cik = repo.get_guru_cik(guru_id)
        if cik is None:
            cik = pipeline.resolve_cik(manager_name)
            if cik is not None:
                repo.update_guru_cik(guru_id, cik)

        if cik is None:
            logger.warning('Skipping guru with unresolved CIK: %s', manager_name)
            summary['skipped'] += 1
            continue

        submissions = pipeline.fetch_submissions(cik)
        filings = pipeline.latest_13f_filings(submissions, limit=backfill_options.per_guru_limit)
        logger.info(
            'Found %s filings to evaluate for %s (requested latest %s quarters)',
            len(filings),
            guru_name,
            backfill_options.per_guru_limit,
        )
        summary['gurus_processed'] += 1

        for filing_index, filing in enumerate(filings, start=1):
            logger.info(
                'Filing %s/%s for %s: accession=%s filing_date=%s',
                filing_index,
                len(filings),
                guru_name,
                filing.accession_number,
                filing.filing_date,
            )
            progress = repo.get_filing_progress(guru_id, filing.accession_number)
            if (
                backfill_options.resume
                and progress is not None
                and progress['fetch_status'] == 'completed'
                and progress['parse_status'] == 'completed'
            ):
                logger.info('Skipping completed filing: %s %s', guru_name, filing.accession_number)
                summary['skipped'] += 1
                continue

            try:
                filing_id, inserted = repo.upsert_filing(guru_id, filing)
                if inserted:
                    summary['filings_fetched'] += 1
                repo.update_filing_status(
                    guru_id=guru_id,
                    accession_number=filing.accession_number,
                    fetch_status='in_progress',
                    parse_status='pending',
                    error_message=None,
                    raw_index_path=filing.index_url,
                    raw_xml_path=filing.information_table_url,
                )

                holdings = pipeline.parse_information_table(cik, filing.accession_number, filing.information_table_url)
                repo.delete_holdings_for_filing(filing_id)
                repo.insert_holdings(filing_id, holdings)
                summary['holdings_upserted'] += len(holdings)

                repo.update_filing_status(
                    guru_id=guru_id,
                    accession_number=filing.accession_number,
                    fetch_status='completed',
                    parse_status='completed',
                    error_message=None,
                    raw_index_path=filing.index_url,
                    raw_xml_path=filing.information_table_url,
                )
            except Exception as error:  # pylint: disable=broad-except
                summary['failures'] += 1
                repo.update_filing_status(
                    guru_id=guru_id,
                    accession_number=filing.accession_number,
                    fetch_status='failed',
                    parse_status='failed',
                    error_message=str(error),
                    raw_index_path=filing.index_url,
                    raw_xml_path=filing.information_table_url,
                )
                logger.exception('Failed filing for %s (%s): %s', guru_name, filing.accession_number, error)

    summary['cached_hits'] = int(getattr(pipeline.client, 'cache_hits', 0))
    logger.info('Backfill summary: %s', summary)
    return summary


def _get_xml_text(element: ET.Element, path: str, namespaces: dict[str, str]) -> str:
    child = element.find(path, namespaces)
    return child.text.strip() if child is not None and child.text else ''
