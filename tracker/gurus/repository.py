"""Persistence helpers for guru 13F data."""

from __future__ import annotations

from decimal import Decimal

from tracker.db import fetch_all, fetch_one, get_conn
from tracker.gurus.sec_13f import FilingRecord, HoldingRecord


class GuruRepository:
    """Repository for tracked guru data."""

    def __init__(self, connection=None):
        self.connection = connection

    def upsert_guru(self, guru_name: str, manager_name: str, cik: str | None = None, enabled: bool = True) -> int:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO tracked_gurus (guru_name, manager_name, cik, enabled)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (manager_name)
                DO UPDATE SET guru_name = excluded.guru_name,
                              cik = COALESCE(NULLIF(excluded.cik, ''), tracked_gurus.cik),
                              enabled = excluded.enabled
                """,
                (guru_name, manager_name, cik or None, 1 if enabled else 0),
            )
            row = conn.execute(
                'SELECT id FROM tracked_gurus WHERE manager_name = ?',
                (manager_name,),
            ).fetchone()
            conn.commit()
        return int(row['id'])

    def get_guru_cik(self, guru_id: int) -> str | None:
        row = fetch_one('SELECT cik FROM tracked_gurus WHERE id = ?', (guru_id,))
        return str(row['cik']) if row and row['cik'] else None

    def update_guru_cik(self, guru_id: int, cik: str) -> None:
        with get_conn() as conn:
            conn.execute('UPDATE tracked_gurus SET cik = ? WHERE id = ?', (cik, guru_id))
            conn.commit()

    def upsert_filing(self, guru_id: int, filing: FilingRecord) -> tuple[int, bool]:
        existing = fetch_one(
            'SELECT id FROM guru_filings WHERE guru_id = ? AND accession_number = ?',
            (guru_id, filing.accession_number),
        )
        inserted = existing is None

        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO guru_filings (guru_id, accession_number, filing_date, report_period, form_type)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (guru_id, accession_number)
                DO UPDATE SET filing_date = excluded.filing_date,
                              report_period = excluded.report_period,
                              form_type = excluded.form_type
                """,
                (
                    guru_id,
                    filing.accession_number,
                    filing.filing_date.isoformat(),
                    filing.report_period.isoformat() if filing.report_period else None,
                    filing.form_type,
                ),
            )
            row = conn.execute(
                'SELECT id FROM guru_filings WHERE guru_id = ? AND accession_number = ?',
                (guru_id, filing.accession_number),
            ).fetchone()
            conn.commit()
        return int(row['id']), inserted

    def update_filing_status(
        self,
        guru_id: int,
        accession_number: str,
        fetch_status: str,
        parse_status: str,
        error_message: str | None,
        raw_index_path: str | None,
        raw_xml_path: str | None,
    ) -> None:
        increment_retry = 0 if fetch_status == 'completed' and parse_status == 'completed' else 1
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE guru_filings
                SET fetch_status = ?,
                    parse_status = ?,
                    last_attempt_at = CURRENT_TIMESTAMP,
                    retry_count = CASE
                        WHEN ? = 1 THEN retry_count + 1
                        ELSE retry_count
                    END,
                    error_message = ?,
                    raw_index_path = COALESCE(?, raw_index_path),
                    raw_xml_path = COALESCE(?, raw_xml_path)
                WHERE guru_id = ? AND accession_number = ?
                """,
                (
                    fetch_status,
                    parse_status,
                    increment_retry,
                    error_message,
                    raw_index_path,
                    raw_xml_path,
                    guru_id,
                    accession_number,
                ),
            )
            conn.commit()

    def delete_holdings_for_filing(self, filing_id: int) -> None:
        with get_conn() as conn:
            conn.execute('DELETE FROM guru_holdings WHERE filing_id = ?', (filing_id,))
            conn.commit()

    def insert_holdings(self, filing_id: int, holdings: list[HoldingRecord]) -> None:
        if not holdings:
            return

        from tracker.gurus.company_identity import CompanyIdentityService

        identity = CompanyIdentityService(repo=self)
        with get_conn() as conn:
            args = [
                (
                    filing_id,
                    h.issuer_name,
                    h.issuer_name,
                    identity.normalize_name(h.issuer_name),
                    _normalize_cusip(h.cusip),
                    str(h.shares),
                    str(h.value_usd),
                    None,
                    None,
                )
                for h in holdings
            ]
            conn.executemany(
                """
                INSERT INTO guru_holdings (
                    filing_id, issuer_name, raw_issuer_name, normalized_issuer_name, cusip,
                    shares, value_usd, put_call, discretion
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (filing_id, issuer_name, cusip)
                DO UPDATE SET shares = excluded.shares,
                              value_usd = excluded.value_usd,
                              raw_issuer_name = COALESCE(guru_holdings.raw_issuer_name, excluded.raw_issuer_name),
                              normalized_issuer_name = excluded.normalized_issuer_name,
                              put_call = excluded.put_call,
                              discretion = excluded.discretion,
                              company_id = NULL,
                              match_status = 'unmapped',
                              match_confidence = NULL,
                              match_notes = NULL
                """,
                args,
            )
            conn.commit()

        identity.apply_identity_for_filing(filing_id=filing_id)

    def latest_two_filings(self, guru_id: int) -> list[tuple[int, str]]:
        rows = fetch_all(
            """
            SELECT id, accession_number
            FROM guru_filings
            WHERE guru_id = ? AND fetch_status = 'completed' AND parse_status = 'completed'
            ORDER BY report_period DESC, filing_date DESC
            LIMIT 2
            """,
            (guru_id,),
        )
        return [(int(row['id']), str(row['accession_number'])) for row in rows]

    def holdings_by_filing(self, filing_id: int) -> dict[str, tuple[str, Decimal]]:
        rows = fetch_all(
            """
            SELECT COALESCE(cusip, ''), issuer_name, COALESCE(shares, 0)
            FROM guru_holdings
            WHERE filing_id = ?
            """,
            (filing_id,),
        )
        result: dict[str, tuple[str, Decimal]] = {}
        for row in rows:
            result[str(row[0])] = (str(row[1]), Decimal(str(row[2])))
        return result

    def holdings_snapshot_by_filing(self, filing_id: int) -> list[dict]:
        rows = fetch_all(
            """
            SELECT id, issuer_name, COALESCE(cusip, '') AS cusip, COALESCE(shares, 0) AS shares, company_id
            FROM guru_holdings
            WHERE filing_id = ?
            """,
            (filing_id,),
        )
        return [dict(row) for row in rows]

    def clear_changes_for_guru(self, guru_id: int) -> None:
        with get_conn() as conn:
            conn.execute('DELETE FROM guru_changes WHERE guru_id = ?', (guru_id,))
            conn.commit()

    def insert_changes(
        self,
        guru_id: int,
        current_filing_id: int,
        previous_filing_id: int,
        changes: list[dict],
    ) -> None:
        if not changes:
            return

        with get_conn() as conn:
            conn.executemany(
                """
                INSERT INTO guru_changes (
                    guru_id, current_filing_id, previous_filing_id, company_id, issuer_name, cusip,
                    current_shares, previous_shares, delta_shares, delta_percent, change_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        guru_id,
                        current_filing_id,
                        previous_filing_id,
                        row.get('company_id'),
                        row['issuer_name'],
                        _normalize_cusip(row['cusip']),
                        str(row['current_shares']),
                        str(row['previous_shares']),
                        str(row['delta_shares']),
                        str(row['delta_percent']) if row['delta_percent'] is not None else None,
                        row['change_type'],
                    )
                    for row in changes
                ],
            )
            conn.commit()

    def enabled_gurus(self) -> list[tuple[int, str]]:
        rows = fetch_all('SELECT id, guru_name FROM tracked_gurus WHERE enabled = 1')
        return [(int(row['id']), str(row['guru_name'])) for row in rows]

    def get_filing_progress(self, guru_id: int, accession_number: str) -> dict | None:
        row = fetch_one(
            """
            SELECT id, fetch_status, parse_status, retry_count, last_attempt_at, error_message
            FROM guru_filings
            WHERE guru_id = ? AND accession_number = ?
            """,
            (guru_id, accession_number),
        )
        if row is None:
            return None
        return {
            'id': int(row['id']),
            'fetch_status': row['fetch_status'],
            'parse_status': row['parse_status'],
            'retry_count': int(row['retry_count']),
            'last_attempt_at': row['last_attempt_at'],
            'error_message': row['error_message'],
        }

    # Query helpers for future frontend
    def get_all_tracked_gurus(self, enabled_only: bool = True) -> list[dict]:
        query = (
            'SELECT id, guru_name, manager_name, cik, enabled, created_at '
            'FROM tracked_gurus '
            + ('WHERE enabled = 1 ' if enabled_only else '')
            + 'ORDER BY guru_name'
        )
        rows = fetch_all(query)
        return [dict(row) for row in rows]

    def get_latest_filings_for_guru(self, guru_id: int, limit: int = 8) -> list[dict]:
        rows = fetch_all(
            """
            SELECT id, guru_id, accession_number, filing_date, report_period, form_type,
                   fetch_status, parse_status, retry_count, error_message, raw_index_path, raw_xml_path
            FROM guru_filings
            WHERE guru_id = ?
            ORDER BY report_period DESC, filing_date DESC
            LIMIT ?
            """,
            (guru_id, limit),
        )
        return [dict(row) for row in rows]

    def get_latest_holdings_for_guru(self, guru_id: int, limit: int = 200) -> list[dict]:
        latest = fetch_one(
            """
            SELECT id
            FROM guru_filings
            WHERE guru_id = ? AND fetch_status = 'completed' AND parse_status = 'completed'
            ORDER BY report_period DESC, filing_date DESC
            LIMIT 1
            """,
            (guru_id,),
        )
        if latest is None:
            return []

        rows = fetch_all(
            """
            SELECT h.*, f.report_period, f.filing_date
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            WHERE h.filing_id = ?
            ORDER BY COALESCE(h.value_usd, 0) DESC
            LIMIT ?
            """,
            (int(latest['id']), limit),
        )
        return [dict(row) for row in rows]

    def get_changes_for_guru(self, guru_id: int, limit: int = 200) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.*, cf.report_period AS current_report_period, pf.report_period AS previous_report_period
            FROM guru_changes c
            JOIN guru_filings cf ON c.current_filing_id = cf.id
            JOIN guru_filings pf ON c.previous_filing_id = pf.id
            WHERE c.guru_id = ?
            ORDER BY ABS(COALESCE(c.delta_shares, 0)) DESC
            LIMIT ?
            """,
            (guru_id, limit),
        )
        return [dict(row) for row in rows]

    def get_biggest_changes_across_gurus(self, change_type: str, limit: int = 50) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.*, g.guru_name, g.manager_name
            FROM guru_changes c
            JOIN tracked_gurus g ON c.guru_id = g.id
            WHERE c.change_type = ?
            ORDER BY ABS(COALESCE(c.delta_shares, 0)) DESC
            LIMIT ?
            """,
            (change_type, limit),
        )
        return [dict(row) for row in rows]

    def find_gurus_holding(self, issuer_name: str | None = None, cusip: str | None = None) -> list[dict]:
        if not issuer_name and not cusip:
            return []

        clauses = []
        params: list[str] = []
        if issuer_name:
            clauses.append('LOWER(h.issuer_name) LIKE ?')
            params.append(f'%{issuer_name.lower()}%')
        if cusip:
            clauses.append('h.cusip = ?')
            params.append(cusip)

        where_clause = ' OR '.join(clauses)
        rows = fetch_all(
            f"""
            SELECT DISTINCT g.id AS guru_id, g.guru_name, g.manager_name, g.cik,
                   h.issuer_name, h.cusip, h.shares, h.value_usd,
                   f.report_period, f.filing_date
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            JOIN tracked_gurus g ON g.id = f.guru_id
            WHERE f.id IN (
                SELECT id FROM guru_filings gf
                WHERE gf.guru_id = f.guru_id
                  AND gf.fetch_status = 'completed'
                  AND gf.parse_status = 'completed'
                ORDER BY gf.report_period DESC, gf.filing_date DESC
                LIMIT 1
            )
              AND ({where_clause})
            ORDER BY g.guru_name
            """,
            params,
        )
        return [dict(row) for row in rows]

    def get_sic_sector_map(self) -> dict[str, tuple[str, str | None]]:
        rows = fetch_all(
            """
            SELECT sic_code, sector_bucket, industry_bucket
            FROM sic_sector_map
            WHERE sic_code IS NOT NULL AND TRIM(sic_code) <> ''
            """
        )
        return {
            str(row['sic_code']): (str(row['sector_bucket']), row['industry_bucket'])
            for row in rows
        }

    def list_distinct_holding_companies(self) -> list[dict]:
        rows = fetch_all(
            """
            SELECT DISTINCT h.issuer_name, h.cusip
            FROM guru_holdings h
            WHERE TRIM(COALESCE(h.issuer_name, '')) <> ''
            """
        )
        return [dict(row) for row in rows]

    def upsert_company(
        self,
        *,
        cik: str | None,
        ticker: str | None,
        cusip: str | None,
        company_name: str,
        normalized_company_name: str | None = None,
        sic_code: str | None,
        sic_description: str | None,
        sector_bucket: str | None,
        industry_bucket: str | None,
        source: str,
        needs_classification: bool,
        classification_status: str = 'matched',
        needs_review: bool = False,
    ) -> int:
        normalized_cusip = _normalize_cusip(cusip)
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO companies (
                    cik, ticker, cusip, company_name, sic_code, sic_description,
                    normalized_company_name, sector_bucket, industry_bucket,
                    classification_status, needs_review, source, needs_classification
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (company_name, cusip)
                DO UPDATE SET
                    cik = excluded.cik,
                    ticker = COALESCE(excluded.ticker, companies.ticker),
                    sic_code = excluded.sic_code,
                    sic_description = excluded.sic_description,
                    normalized_company_name = COALESCE(excluded.normalized_company_name, companies.normalized_company_name),
                    sector_bucket = excluded.sector_bucket,
                    industry_bucket = excluded.industry_bucket,
                    classification_status = excluded.classification_status,
                    needs_review = excluded.needs_review,
                    source = excluded.source,
                    needs_classification = excluded.needs_classification,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    cik,
                    ticker,
                    normalized_cusip,
                    company_name,
                    sic_code,
                    sic_description,
                    normalized_company_name,
                    sector_bucket,
                    industry_bucket,
                    classification_status,
                    1 if needs_review else 0,
                    source,
                    1 if needs_classification else 0,
                ),
            )
            row = conn.execute(
                'SELECT id FROM companies WHERE company_name = ? AND ((cusip = ?) OR (cusip IS NULL AND ? IS NULL))',
                (company_name, normalized_cusip, normalized_cusip),
            ).fetchone()
            conn.commit()
        return int(row['id'])

    def ensure_canonical_company(self, **kwargs) -> int:
        return self.upsert_company(**kwargs)

    def get_holdings_for_identity(self, filing_id: int) -> list[dict]:
        rows = fetch_all(
            """
            SELECT h.id, h.issuer_name, h.raw_issuer_name, h.cusip, c.ticker
            FROM guru_holdings h
            LEFT JOIN companies c ON c.id = h.company_id
            WHERE h.filing_id = ?
            """,
            (filing_id,),
        )
        return [dict(row) for row in rows]

    def update_holding_company_match(
        self,
        *,
        holding_id: int,
        company_id: int | None,
        normalized_issuer_name: str,
        match_status: str,
        match_confidence: float | None,
        match_notes: str | None,
    ) -> None:
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE guru_holdings
                SET company_id = ?,
                    normalized_issuer_name = ?,
                    match_status = ?,
                    match_confidence = ?,
                    match_notes = ?,
                    raw_issuer_name = COALESCE(raw_issuer_name, issuer_name)
                WHERE id = ?
                """,
                (company_id, normalized_issuer_name, match_status, match_confidence, match_notes, holding_id),
            )
            conn.commit()

    def find_company_by_cusip(self, cusip: str):
        return fetch_one(
            "SELECT * FROM companies WHERE cusip = ? ORDER BY updated_at DESC LIMIT 1",
            (_normalize_cusip(cusip),),
        )

    def find_company_by_normalized_name(self, normalized_name: str):
        return fetch_one(
            "SELECT * FROM companies WHERE normalized_company_name = ? ORDER BY updated_at DESC LIMIT 1",
            (normalized_name,),
        )

    def find_company_by_ticker(self, ticker: str):
        return fetch_one(
            "SELECT * FROM companies WHERE UPPER(COALESCE(ticker, '')) = ? ORDER BY updated_at DESC LIMIT 1",
            (ticker.upper(),),
        )

    def find_company_name_like(self, normalized_name: str):
        return fetch_one(
            """
            SELECT *
            FROM companies
            WHERE normalized_company_name LIKE ? OR ? LIKE normalized_company_name || ' %'
            ORDER BY LENGTH(normalized_company_name) DESC
            LIMIT 1
            """,
            (f'{normalized_name} %', normalized_name),
        )

    def find_identity_override(
        self,
        *,
        raw_issuer_name: str | None,
        normalized_issuer_name: str,
        cusip: str | None,
        ticker: str | None,
    ):
        return fetch_one(
            """
            SELECT *
            FROM company_identity_overrides
            WHERE (raw_issuer_name IS NOT NULL AND LOWER(raw_issuer_name) = LOWER(?))
               OR (normalized_issuer_name IS NOT NULL AND normalized_issuer_name = ?)
               OR (cusip IS NOT NULL AND cusip <> '' AND cusip = ?)
               OR (ticker IS NOT NULL AND ticker <> '' AND UPPER(ticker) = UPPER(?))
            ORDER BY id DESC
            LIMIT 1
            """,
            (raw_issuer_name, normalized_issuer_name, cusip, ticker),
        )

    def mark_company_identity_status(self, *, company_id: int, classification_status: str, needs_review: bool) -> None:
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE companies
                SET classification_status = ?,
                    needs_review = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (classification_status, 1 if needs_review else 0, company_id),
            )
            conn.commit()

    def get_unmapped_companies(self, limit: int = 200) -> list[dict]:
        rows = fetch_all(
            """
            SELECT *
            FROM companies
            WHERE needs_classification = 1 OR sector_bucket IS NULL
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    def get_sector_change_counts(self, change_type: str) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.sector_bucket, COUNT(1) AS positions_count
            FROM guru_changes gc
            JOIN companies c ON c.id = gc.company_id
            WHERE gc.change_type = ? AND c.sector_bucket IS NOT NULL
            GROUP BY c.sector_bucket
            ORDER BY positions_count DESC
            """,
            (change_type,),
        )
        return [dict(row) for row in rows]

    def get_sector_net_movement(self) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.sector_bucket,
                   SUM(CASE WHEN gc.change_type IN ('NEW', 'ADD') THEN 1 ELSE 0 END)
                 - SUM(CASE WHEN gc.change_type IN ('EXIT', 'REDUCE') THEN 1 ELSE 0 END)
                   AS net_movement
            FROM guru_changes gc
            JOIN companies c ON c.id = gc.company_id
            WHERE c.sector_bucket IS NOT NULL
            GROUP BY c.sector_bucket
            ORDER BY net_movement DESC
            """
        )
        return [dict(row) for row in rows]

    def get_top_sectors_by_guru(self, guru_id: int, limit: int = 10) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.sector_bucket, COUNT(1) AS position_count
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            JOIN companies c ON c.id = h.company_id
            WHERE f.guru_id = ?
              AND f.id = (
                SELECT id FROM guru_filings gf
                WHERE gf.guru_id = f.guru_id
                  AND gf.fetch_status = 'completed'
                  AND gf.parse_status = 'completed'
                ORDER BY gf.report_period DESC, gf.filing_date DESC
                LIMIT 1
              )
              AND c.sector_bucket IS NOT NULL
            GROUP BY c.sector_bucket
            ORDER BY position_count DESC
            LIMIT ?
            """,
            (guru_id, limit),
        )
        return [dict(row) for row in rows]

    def get_gurus_buying_sector(self, sector_bucket: str, limit: int = 200) -> list[dict]:
        rows = fetch_all(
            """
            SELECT DISTINCT g.id AS guru_id, g.guru_name, g.manager_name, c.sector_bucket
            FROM guru_changes gc
            JOIN tracked_gurus g ON g.id = gc.guru_id
            JOIN companies c ON c.id = gc.company_id
            WHERE gc.change_type IN ('NEW', 'ADD')
              AND c.sector_bucket = ?
            ORDER BY g.guru_name
            LIMIT ?
            """,
            (sector_bucket, limit),
        )
        return [dict(row) for row in rows]

    def get_canonical_company_by_holding(self, holding_id: int) -> dict | None:
        row = fetch_one(
            """
            SELECT c.*
            FROM guru_holdings h
            JOIN companies c ON c.id = h.company_id
            WHERE h.id = ?
            """,
            (holding_id,),
        )
        return dict(row) if row else None

    def get_holdings_by_company(self, company_id: int, limit: int = 500) -> list[dict]:
        rows = fetch_all(
            """
            SELECT h.*, f.guru_id, f.report_period, f.filing_date
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            WHERE h.company_id = ?
            ORDER BY f.report_period DESC, f.filing_date DESC
            LIMIT ?
            """,
            (company_id, limit),
        )
        return [dict(row) for row in rows]

    def get_unresolved_holdings(self, limit: int | None = 500) -> list[dict]:
        query = """
            SELECT h.*, f.guru_id, f.report_period
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            WHERE h.company_id IS NULL
               OR h.match_status IN ('unmapped')
               OR COALESCE(h.match_confidence, 0) < 0.6
            ORDER BY f.report_period DESC
        """
        params: tuple = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        rows = fetch_all(query, params)
        return [dict(row) for row in rows]

    def get_companies_needing_review(self, limit: int = 200) -> list[dict]:
        rows = fetch_all(
            """
            SELECT *
            FROM companies
            WHERE needs_review = 1 OR classification_status IN ('inferred', 'unmapped')
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    def get_gurus_holding_company(self, company_id: int) -> list[dict]:
        rows = fetch_all(
            """
            SELECT DISTINCT g.id AS guru_id, g.guru_name, g.manager_name
            FROM guru_holdings h
            JOIN guru_filings f ON f.id = h.filing_id
            JOIN tracked_gurus g ON g.id = f.guru_id
            WHERE h.company_id = ?
            ORDER BY g.guru_name
            """,
            (company_id,),
        )
        return [dict(row) for row in rows]

    def get_sector_counts_from_canonical_companies(self) -> list[dict]:
        rows = fetch_all(
            """
            SELECT c.sector_bucket, COUNT(1) AS holdings_count
            FROM guru_holdings h
            JOIN companies c ON c.id = h.company_id
            WHERE c.sector_bucket IS NOT NULL
            GROUP BY c.sector_bucket
            ORDER BY holdings_count DESC
            """
        )
        return [dict(row) for row in rows]


def _normalize_cusip(cusip: str | None) -> str | None:
    cleaned = (cusip or '').strip().upper()
    return cleaned or None
