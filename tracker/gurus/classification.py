"""SEC-driven company sector classification into internal canonical taxonomy."""

from __future__ import annotations

import re
from dataclasses import dataclass

CANONICAL_SECTORS = [
    'Healthcare Plans',
    'Media - Diversified',
    'Furnishings, Fixtures & Appliances',
    'Forest Products',
    'Conglomerates',
    'Telecommunication Services',
    'Real Estate',
    'Restaurants',
    'Beverages - Non-Alcoholic',
    'Medical Distribution',
    'Agriculture',
    'Farm & Heavy Construction Machinery',
    'Software',
    'Manufacturing - Apparel & Accessories',
    'Retail - Defensive',
    'Homebuilding & Construction',
    'Other Energy Sources',
    'Tobacco Products',
    'Beverages - Alcoholic',
    'Healthcare Providers & Services',
    'Steel',
    'Diversified Financial Services',
    'Education',
    'Personal Services',
    'Travel & Leisure',
    'Interactive Media',
    'Industrial Distribution',
    'Building Materials',
    'Construction',
    'Semiconductors',
    'Asset Management',
    'Waste Management',
    'Utilities - Independent Power Producers',
    'Drug Manufacturers',
    'Medical Diagnostics & Research',
    'Consumer Packaged Goods',
    'Metals & Mining',
    'Industrial Products',
    'Credit Services',
    'Chemicals',
    'REITs',
    'Aerospace & Defense',
    'Packaging & Containers',
    'Vehicles & Parts',
    'Utilities - Regulated',
    'Transportation',
    'Capital Markets',
    'Medical Devices & Instruments',
    'Retail - Cyclical',
    'Oil & Gas',
    'Business Services',
    'Insurance',
    'Hardware',
    'Banks',
    'Biotechnology',
]


DEFAULT_SIC_RULES = [
    {'sic_code': '3571', 'sector_bucket': 'Hardware', 'industry_bucket': 'Electronic Computers', 'notes': 'Exact SIC'},
    {'sic_code': '3576', 'sector_bucket': 'Hardware', 'industry_bucket': 'Computer Communications Equipment', 'notes': 'Exact SIC'},
    {'sic_code': '3577', 'sector_bucket': 'Semiconductors', 'industry_bucket': 'Semiconductors', 'notes': 'Exact SIC'},
    {'sic_code': '3674', 'sector_bucket': 'Semiconductors', 'industry_bucket': 'Semiconductors', 'notes': 'Exact SIC'},
    {'sic_code': '2834', 'sector_bucket': 'Drug Manufacturers', 'industry_bucket': 'Pharmaceutical Preparations', 'notes': 'Exact SIC'},
    {'sic_code': '2836', 'sector_bucket': 'Biotechnology', 'industry_bucket': 'Biological Products', 'notes': 'Exact SIC'},
    {'sic_code': '3841', 'sector_bucket': 'Medical Devices & Instruments', 'industry_bucket': 'Surgical and Medical Instruments', 'notes': 'Exact SIC'},
    {'sic_code': '3845', 'sector_bucket': 'Medical Devices & Instruments', 'industry_bucket': 'Electromedical Apparatus', 'notes': 'Exact SIC'},
    {'sic_code': '2835', 'sector_bucket': 'Drug Manufacturers', 'industry_bucket': 'In Vitro and In Vivo Diagnostics', 'notes': 'Exact SIC'},
    {'sic_code': '7372', 'sector_bucket': 'Software', 'industry_bucket': 'Prepackaged Software', 'notes': 'Exact SIC'},
    {'sic_code': '6021', 'sector_bucket': 'Banks', 'industry_bucket': 'National Commercial Banks', 'notes': 'Exact SIC'},
    {'sic_code': '6022', 'sector_bucket': 'Banks', 'industry_bucket': 'State Commercial Banks', 'notes': 'Exact SIC'},
    {'sic_code': '6211', 'sector_bucket': 'Capital Markets', 'industry_bucket': 'Security Brokers and Dealers', 'notes': 'Exact SIC'},
    {'sic_code': '6282', 'sector_bucket': 'Asset Management', 'industry_bucket': 'Investment Advice', 'notes': 'Exact SIC'},
    {'sic_code': '6798', 'sector_bucket': 'REITs', 'industry_bucket': 'Real Estate Investment Trusts', 'notes': 'Exact SIC'},
]

KEYWORD_RULES = [
    ('biotech', 'Biotechnology', 'Biotechnology'),
    ('pharmaceutical', 'Drug Manufacturers', 'Drug Manufacturers'),
    ('diagnostic', 'Medical Diagnostics & Research', 'Medical Diagnostics & Research'),
    ('medical device', 'Medical Devices & Instruments', 'Medical Devices & Instruments'),
    ('hospital', 'Healthcare Providers & Services', 'Healthcare Providers & Services'),
    ('health care', 'Healthcare Providers & Services', 'Healthcare Providers & Services'),
    ('insurance', 'Insurance', 'Insurance'),
    ('bank', 'Banks', 'Banks'),
    ('credit', 'Credit Services', 'Credit Services'),
    ('asset management', 'Asset Management', 'Asset Management'),
    ('investment', 'Capital Markets', 'Capital Markets'),
    ('reit', 'REITs', 'REITs'),
    ('real estate', 'Real Estate', 'Real Estate'),
    ('software', 'Software', 'Software'),
    ('semiconductor', 'Semiconductors', 'Semiconductors'),
    ('telecommunication', 'Telecommunication Services', 'Telecommunication Services'),
    ('oil', 'Oil & Gas', 'Oil & Gas'),
    ('gas', 'Oil & Gas', 'Oil & Gas'),
    ('electric', 'Utilities - Regulated', 'Utilities - Regulated'),
    ('power producer', 'Utilities - Independent Power Producers', 'Utilities - Independent Power Producers'),
    ('restaurant', 'Restaurants', 'Restaurants'),
    ('beverage', 'Beverages - Non-Alcoholic', 'Beverages'),
    ('alcohol', 'Beverages - Alcoholic', 'Beverages'),
    ('tobacco', 'Tobacco Products', 'Tobacco'),
    ('aerospace', 'Aerospace & Defense', 'Aerospace & Defense'),
    ('defense', 'Aerospace & Defense', 'Aerospace & Defense'),
    ('chemical', 'Chemicals', 'Chemicals'),
    ('steel', 'Steel', 'Steel'),
    ('mining', 'Metals & Mining', 'Metals & Mining'),
    ('transportation', 'Transportation', 'Transportation'),
    ('railroad', 'Transportation', 'Transportation'),
    ('waste', 'Waste Management', 'Waste Management'),
    ('packaging', 'Packaging & Containers', 'Packaging & Containers'),
    ('retail', 'Retail - Cyclical', 'Retail'),
]

NAME_HEURISTIC_RULES = [
    (r'\bbank\b', 'Banks', 'Name heuristic'),
    (r'\bpharma\b|\btherapeutics\b|\bbiologics\b', 'Drug Manufacturers', 'Name heuristic'),
    (r'\bbiotech\b', 'Biotechnology', 'Name heuristic'),
    (r'\bsoftware\b|\bsystems\b|\bcloud\b', 'Software', 'Name heuristic'),
    (r'\benergy\b|\boil\b|\bgas\b', 'Oil & Gas', 'Name heuristic'),
    (r'\brealty\b|\bproperties\b|\breit\b', 'REITs', 'Name heuristic'),
    (r'\binsurance\b', 'Insurance', 'Name heuristic'),
]


@dataclass(slots=True)
class ClassificationResult:
    sector_bucket: str | None
    industry_bucket: str | None
    needs_classification: bool
    notes: str


class SectorClassifier:
    """Deterministic classifier from SEC SIC inputs into canonical sectors."""

    def __init__(self, exact_sic_map: dict[str, tuple[str, str | None]] | None = None):
        self.exact_sic_map = exact_sic_map or {
            str(row['sic_code']): (str(row['sector_bucket']), row.get('industry_bucket'))
            for row in DEFAULT_SIC_RULES
            if row.get('sic_code')
        }

    def classify(self, sic_code: str | None, sic_description: str | None, company_name: str | None) -> ClassificationResult:
        if sic_code:
            normalized_code = ''.join(ch for ch in str(sic_code) if ch.isdigit())
            if normalized_code in self.exact_sic_map:
                sector, industry = self.exact_sic_map[normalized_code]
                return ClassificationResult(
                    sector_bucket=sector,
                    industry_bucket=industry,
                    needs_classification=False,
                    notes='exact_sic_match',
                )

        description = (sic_description or '').strip().lower()
        if description:
            for keyword, sector, industry in KEYWORD_RULES:
                if keyword in description:
                    return ClassificationResult(
                        sector_bucket=sector,
                        industry_bucket=industry,
                        needs_classification=False,
                        notes=f'sic_description_keyword:{keyword}',
                    )

        normalized_name = _normalize(company_name or '')
        if normalized_name:
            for pattern, sector, note in NAME_HEURISTIC_RULES:
                if re.search(pattern, normalized_name):
                    return ClassificationResult(
                        sector_bucket=sector,
                        industry_bucket='Name heuristic',
                        needs_classification=False,
                        notes=note,
                    )

        return ClassificationResult(
            sector_bucket=None,
            industry_bucket=None,
            needs_classification=True,
            notes='unmapped',
        )


def _normalize(value: str) -> str:
    lowered = value.lower().replace('&', ' and ')
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9 ]+', ' ', lowered)).strip()
