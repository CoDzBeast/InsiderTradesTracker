"""Persistence helpers for guru 13F data."""

from __future__ import annotations

from decimal import Decimal

from tracker.db import fetch_all, fetch_one, get_conn
from tracker.gurus.sec_13f import FilingRecord, HoldingRecord


class GuruRepository:
    """Repository for tracked guru data."""

    def __init__(self, connection=None):
        self.connection = connection

    def upsert_guru(self, guru_name: str, manager_name: str) -> int:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO tracked_gurus (guru_name, manager_name, enabled)
                VALUES (?, ?, 1)
                ON CONFLICT (manager_name)
                DO UPDATE SET guru_name = excluded.guru_name
                """,
                (guru_name, manager_name),
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
        with get_conn() as conn:
            conn.execute(
                """
                UPDATE guru_filings
                SET fetch_status = ?,
                    parse_status = ?,
                    last_attempt_at = CURRENT_TIMESTAMP,
                    retry_count = CASE
                        WHEN ? = 'completed' AND ? = 'completed' THEN retry_count
                        ELSE retry_count + 1
                    END,
                    error_message = ?,
                    raw_index_path = COALESCE(?, raw_index_path),
                    raw_xml_path = COALESCE(?, raw_xml_path)
                WHERE guru_id = ? AND accession_number = ?
                """,
                (
                    fetch_status,
                    parse_status,
                    fetch_status,
                    parse_status,
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

        with get_conn() as conn:
            args = [
                (filing_id, h.issuer_name, h.cusip, str(h.shares), str(h.value_usd), None, None)
                for h in holdings
            ]
            conn.executemany(
                """
                INSERT INTO guru_holdings (filing_id, issuer_name, cusip, shares, value_usd, put_call, discretion)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (filing_id, issuer_name, cusip)
                DO UPDATE SET shares = excluded.shares,
                              value_usd = excluded.value_usd,
                              put_call = excluded.put_call,
                              discretion = excluded.discretion
                """,
                args,
            )
            conn.commit()

    def latest_two_filings(self, guru_id: int) -> list[tuple[int, str]]:
        rows = fetch_all(
            """
            SELECT id, accession_number
            FROM guru_filings
            WHERE guru_id = ?
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
                    guru_id, current_filing_id, previous_filing_id, issuer_name, cusip,
                    current_shares, previous_shares, delta_shares, delta_percent, change_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        guru_id,
                        current_filing_id,
                        previous_filing_id,
                        row['issuer_name'],
                        row['cusip'],
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
            SELECT fetch_status, parse_status, retry_count
            FROM guru_filings
            WHERE guru_id = ? AND accession_number = ?
            """,
            (guru_id, accession_number),
        )
        if row is None:
            return None
        return {
            'fetch_status': row['fetch_status'],
            'parse_status': row['parse_status'],
            'retry_count': row['retry_count'],
        }
