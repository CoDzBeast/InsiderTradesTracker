"""Canonical company identity matching for 13F holdings."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tracker.db import fetch_all

if TYPE_CHECKING:
    from tracker.gurus.repository import GuruRepository

_SUFFIX_TOKENS = {
    'inc',
    'incorporated',
    'corp',
    'corporation',
    'co',
    'company',
    'ltd',
    'limited',
    'plc',
    'holdings',
    'holding',
    'group',
    'sa',
    'ag',
    'nv',
    'llc',
    'lp',
    'class',
    'cl',
    'shares',
    'share',
    'common',
    'stock',
}


@dataclass(slots=True)
class MatchResult:
    company_id: int | None
    status: str
    confidence: float
    notes: str
    needs_review: bool


class CompanyIdentityService:
    """Deterministic identity resolution and canonical company assignment."""

    def __init__(self, repo: 'GuruRepository | None' = None):
        if repo is None:
            from tracker.gurus.repository import GuruRepository

            repo = GuruRepository()
        self.repo = repo

    def normalize_name(self, raw_name: str | None) -> str:
        value = (raw_name or '').strip().lower()
        if not value:
            return ''

        value = value.replace('&', ' and ')
        value = re.sub(r'[^a-z0-9 ]+', ' ', value)
        value = re.sub(r'\s+', ' ', value).strip()
        tokens = [token for token in value.split(' ') if token]

        while tokens and tokens[-1] in _SUFFIX_TOKENS:
            tokens.pop()

        normalized = ' '.join(tokens)
        return re.sub(r'\s+', ' ', normalized).strip()

    def apply_identity_for_filing(self, filing_id: int) -> dict[str, int]:
        holdings = self.repo.get_holdings_for_identity(filing_id=filing_id)
        summary = {'matched': 0, 'inferred': 0, 'unmapped': 0, 'review': 0}

        for holding in holdings:
            result = self._resolve_holding(
                raw_issuer_name=holding.get('raw_issuer_name') or holding.get('issuer_name'),
                cusip=holding.get('cusip'),
                ticker=holding.get('ticker'),
            )
            self.repo.update_holding_company_match(
                holding_id=int(holding['id']),
                company_id=result.company_id,
                normalized_issuer_name=self.normalize_name(str(holding.get('raw_issuer_name') or holding.get('issuer_name') or '')),
                match_status=result.status,
                match_confidence=result.confidence,
                match_notes=result.notes,
            )

            summary[result.status if result.status in summary else 'unmapped'] += 1
            if result.needs_review:
                summary['review'] += 1

        return summary

    def rematch_unresolved_holdings(self, limit: int | None = None) -> dict[str, int]:
        holdings = self.repo.get_unresolved_holdings(limit=limit)
        summary = {'processed': 0, 'matched': 0, 'inferred': 0, 'unmapped': 0, 'review': 0}

        for holding in holdings:
            result = self._resolve_holding(
                raw_issuer_name=holding.get('raw_issuer_name') or holding.get('issuer_name'),
                cusip=holding.get('cusip'),
                ticker=holding.get('ticker'),
            )
            self.repo.update_holding_company_match(
                holding_id=int(holding['id']),
                company_id=result.company_id,
                normalized_issuer_name=self.normalize_name(str(holding.get('raw_issuer_name') or holding.get('issuer_name') or '')),
                match_status=result.status,
                match_confidence=result.confidence,
                match_notes=result.notes,
            )

            summary['processed'] += 1
            summary[result.status if result.status in summary else 'unmapped'] += 1
            if result.needs_review:
                summary['review'] += 1

        return summary

    def _resolve_holding(self, raw_issuer_name: str | None, cusip: str | None, ticker: str | None) -> MatchResult:
        normalized_name = self.normalize_name(raw_issuer_name)
        cleaned_cusip = (cusip or '').strip() or None
        cleaned_ticker = (ticker or '').strip().upper() or None

        override = self.repo.find_identity_override(
            raw_issuer_name=raw_issuer_name,
            normalized_issuer_name=normalized_name,
            cusip=cleaned_cusip,
            ticker=cleaned_ticker,
        )
        if override:
            company_id = int(override['forced_company_id'])
            self.repo.mark_company_identity_status(
                company_id=company_id,
                classification_status='manual_override',
                needs_review=False,
            )
            return MatchResult(
                company_id=company_id,
                status='matched',
                confidence=1.0,
                notes='manual_override',
                needs_review=False,
            )

        if cleaned_cusip:
            by_cusip = self.repo.find_company_by_cusip(cleaned_cusip)
            if by_cusip:
                return MatchResult(
                    company_id=int(by_cusip['id']),
                    status='matched',
                    confidence=0.99,
                    notes='cusip_exact',
                    needs_review=False,
                )

        if normalized_name:
            by_name = self.repo.find_company_by_normalized_name(normalized_name)
            if by_name:
                confidence = 0.95
                notes = 'normalized_name_exact'
                if cleaned_cusip and by_name['cusip'] and str(by_name['cusip']).strip() != cleaned_cusip:
                    confidence = 0.4
                    notes = 'normalized_name_conflicting_cusip'
                    return MatchResult(
                        company_id=int(by_name['id']),
                        status='inferred',
                        confidence=confidence,
                        notes=notes,
                        needs_review=True,
                    )

                return MatchResult(
                    company_id=int(by_name['id']),
                    status='matched',
                    confidence=confidence,
                    notes=notes,
                    needs_review=False,
                )

        if cleaned_ticker:
            by_ticker = self.repo.find_company_by_ticker(cleaned_ticker)
            if by_ticker:
                return MatchResult(
                    company_id=int(by_ticker['id']),
                    status='inferred',
                    confidence=0.75,
                    notes='ticker_exact',
                    needs_review=False,
                )

        if normalized_name:
            fuzzy = self.repo.find_company_name_like(normalized_name)
            if fuzzy:
                return MatchResult(
                    company_id=int(fuzzy['id']),
                    status='inferred',
                    confidence=0.55,
                    notes='name_prefix_heuristic',
                    needs_review=True,
                )

        created_company_id = self.repo.ensure_canonical_company(
            company_name=(raw_issuer_name or '').strip() or normalized_name or 'Unknown Issuer',
            normalized_company_name=normalized_name,
            cusip=cleaned_cusip,
            ticker=cleaned_ticker,
            cik=None,
            sic_code=None,
            sic_description=None,
            source='13f_identity',
            classification_status='unmapped',
            needs_review=True,
            sector_bucket=None,
            industry_bucket=None,
            needs_classification=True,
        )
        return MatchResult(
            company_id=created_company_id,
            status='unmapped',
            confidence=0.0,
            notes='new_canonical_company_created_unmapped',
            needs_review=True,
        )


def build_sector_counts_by_company() -> list[dict]:
    rows = fetch_all(
        """
        SELECT c.sector_bucket, COUNT(1) AS positions_count
        FROM guru_holdings h
        JOIN companies c ON c.id = h.company_id
        WHERE c.sector_bucket IS NOT NULL
        GROUP BY c.sector_bucket
        ORDER BY positions_count DESC
        """
    )
    return [dict(row) for row in rows]
