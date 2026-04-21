"""SEC EDGAR 13F ingestion pipeline for tracked gurus."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as ET

import requests

from defs import PROJECT_PATH

SEC_HEADERS = {
    'User-Agent': 'InsiderTradesTracker/13f-research (contact: config.yaml email)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
}


@dataclass(slots=True)
class FilingRecord:
    accession_number: str
    filing_date: date
    report_period: date | None
    form_type: str
    information_table_url: str


@dataclass(slots=True)
class HoldingRecord:
    issuer_name: str
    cusip: str
    shares: Decimal
    value_usd: Decimal


class SEC13FIngestion:
    """Ingest 13F filings and holdings for configured gurus."""

    def __init__(self, config_path: Path | None = None, timeout_seconds: int = 30):
        self.config_path = config_path or PROJECT_PATH / 'config' / 'tracked_gurus.json'
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(SEC_HEADERS)

    def load_tracked_gurus(self) -> list[dict[str, str]]:
        with self.config_path.open(mode='r', encoding='utf-8') as handle:
            return json.load(handle)

    def resolve_cik(self, manager_name: str) -> str | None:
        """Resolve manager CIK via SEC company search endpoint."""
        response = self.session.get(
            'https://www.sec.gov/cgi-bin/browse-edgar',
            params={'company': manager_name, 'owner': 'exclude', 'action': 'getcompany', 'output': 'atom'},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        text = response.text
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
        response = self.session.get(
            f'https://data.sec.gov/submissions/CIK{cik}.json',
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

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
            primary_url = (
                f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}/index.json'
            )
            info_table_url = self.find_information_table(primary_url)
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
                    information_table_url=info_table_url,
                )
            )

            if len(filings) >= limit:
                break

        return filings

    def find_information_table(self, filing_index_json_url: str) -> str | None:
        response = self.session.get(filing_index_json_url, timeout=self.timeout_seconds)
        if response.status_code == 404:
            return None
        response.raise_for_status()

        payload = response.json()
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

    def parse_information_table(self, information_table_url: str) -> list[HoldingRecord]:
        response = self.session.get(information_table_url, timeout=self.timeout_seconds)
        response.raise_for_status()

        root = ET.fromstring(response.content)
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


# pylint: disable=too-many-arguments

def ingest_guru_filings(connection, pipeline: SEC13FIngestion, per_guru_limit: int = 2) -> dict[str, int]:
    """Load gurus from config, resolve CIKs, and store latest 13F filings + holdings."""

    from tracker.gurus.repository import GuruRepository

    repo = GuruRepository(connection)
    summary = {'gurus': 0, 'filings': 0, 'holdings': 0}

    for guru in pipeline.load_tracked_gurus():
        guru_id = repo.upsert_guru(
            guru_name=guru['guru_name'],
            manager_name=guru['manager_name'],
        )

        cik = repo.get_guru_cik(guru_id)
        if cik is None:
            cik = pipeline.resolve_cik(guru['manager_name'])
            if cik is not None:
                repo.update_guru_cik(guru_id, cik)

        if cik is None:
            continue

        submissions = pipeline.fetch_submissions(cik)
        filings = pipeline.latest_13f_filings(submissions, limit=per_guru_limit)

        summary['gurus'] += 1

        for filing in filings:
            filing_id, inserted = repo.upsert_filing(guru_id, filing)
            if inserted:
                summary['filings'] += 1

            repo.delete_holdings_for_filing(filing_id)
            holdings = pipeline.parse_information_table(filing.information_table_url)
            repo.insert_holdings(filing_id, holdings)
            summary['holdings'] += len(holdings)

    return summary


def _get_xml_text(element: ET.Element, path: str, namespaces: dict[str, str]) -> str:
    child = element.find(path, namespaces)
    return child.text.strip() if child is not None and child.text else ''
