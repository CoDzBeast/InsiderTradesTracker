"""Persistence helpers for guru 13F data."""

from __future__ import annotations

from decimal import Decimal

from tracker.gurus.sec_13f import FilingRecord, HoldingRecord


class GuruRepository:
    """Postgres repository for tracked guru data."""

    def __init__(self, connection):
        self.connection = connection

    def upsert_guru(self, guru_name: str, manager_name: str) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tracked_gurus (guru_name, manager_name, enabled)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (manager_name)
                DO UPDATE SET guru_name = EXCLUDED.guru_name, updated_at = NOW()
                RETURNING id
                """,
                (guru_name, manager_name),
            )
            guru_id = cursor.fetchone()[0]
        self.connection.commit()
        return guru_id

    def get_guru_cik(self, guru_id: int) -> str | None:
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT cik FROM tracked_gurus WHERE id = %s', (guru_id,))
            row = cursor.fetchone()
        return row[0] if row else None

    def update_guru_cik(self, guru_id: int, cik: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                'UPDATE tracked_gurus SET cik = %s, updated_at = NOW() WHERE id = %s',
                (cik, guru_id),
            )
        self.connection.commit()

    def upsert_filing(self, guru_id: int, filing: FilingRecord) -> tuple[int, bool]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO guru_filings (guru_id, accession_number, filing_date, report_period, form_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (guru_id, accession_number)
                DO UPDATE SET filing_date = EXCLUDED.filing_date,
                              report_period = EXCLUDED.report_period,
                              form_type = EXCLUDED.form_type
                RETURNING id, xmax = 0 AS inserted
                """,
                (
                    guru_id,
                    filing.accession_number,
                    filing.filing_date,
                    filing.report_period,
                    filing.form_type,
                ),
            )
            filing_id, inserted = cursor.fetchone()
        self.connection.commit()
        return filing_id, inserted

    def delete_holdings_for_filing(self, filing_id: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM guru_holdings WHERE filing_id = %s', (filing_id,))
        self.connection.commit()

    def insert_holdings(self, filing_id: int, holdings: list[HoldingRecord]) -> None:
        if not holdings:
            return

        with self.connection.cursor() as cursor:
            args = [
                (filing_id, h.issuer_name, h.cusip, h.shares, h.value_usd)
                for h in holdings
            ]
            cursor.executemany(
                """
                INSERT INTO guru_holdings (filing_id, issuer_name, cusip, shares, value_usd)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (filing_id, issuer_name, cusip)
                DO UPDATE SET shares = EXCLUDED.shares,
                              value_usd = EXCLUDED.value_usd
                """,
                args,
            )
        self.connection.commit()

    def latest_two_filings(self, guru_id: int) -> list[tuple[int, str]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, accession_number
                FROM guru_filings
                WHERE guru_id = %s
                ORDER BY report_period DESC NULLS LAST, filing_date DESC
                LIMIT 2
                """,
                (guru_id,),
            )
            return cursor.fetchall()

    def holdings_by_filing(self, filing_id: int) -> dict[str, tuple[str, Decimal]]:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COALESCE(cusip, ''), issuer_name, COALESCE(shares, 0)
                FROM guru_holdings
                WHERE filing_id = %s
                """,
                (filing_id,),
            )
            rows = cursor.fetchall()

        result: dict[str, tuple[str, Decimal]] = {}
        for cusip, issuer_name, shares in rows:
            result[str(cusip)] = (issuer_name, Decimal(shares))
        return result

    def clear_changes_for_guru(self, guru_id: int) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM guru_changes WHERE guru_id = %s', (guru_id,))
        self.connection.commit()

    def insert_changes(self, guru_id: int, changes: list[dict]) -> None:
        if not changes:
            return

        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO guru_changes (
                    guru_id, issuer_name, cusip, current_shares, previous_shares,
                    delta_shares, delta_percent, change_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        guru_id,
                        row['issuer_name'],
                        row['cusip'],
                        row['current_shares'],
                        row['previous_shares'],
                        row['delta_shares'],
                        row['delta_percent'],
                        row['change_type'],
                    )
                    for row in changes
                ],
            )
        self.connection.commit()

    def enabled_gurus(self) -> list[tuple[int, str]]:
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT id, guru_name FROM tracked_gurus WHERE enabled = TRUE')
            return cursor.fetchall()

    def get_backfill_progress(self, guru_id: int, accession_number: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT fetch_status, parse_status, retry_count
                FROM guru_backfill_progress
                WHERE guru_id = %s AND accession_number = %s
                """,
                (guru_id, accession_number),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {'fetch_status': row[0], 'parse_status': row[1], 'retry_count': row[2]}

    def upsert_backfill_progress(
        self,
        guru_id: int,
        guru_name: str,
        manager_name: str,
        cik: str,
        accession_number: str,
        filing_date,
        fetch_status: str,
        parse_status: str,
        error_message: str | None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO guru_backfill_progress (
                    guru_id, guru_name, manager_name, cik, accession_number, filing_date,
                    fetch_status, parse_status, last_attempt_at, retry_count, error_message, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), 0, %s, NOW())
                ON CONFLICT (guru_id, accession_number)
                DO UPDATE SET
                    guru_name = EXCLUDED.guru_name,
                    manager_name = EXCLUDED.manager_name,
                    cik = EXCLUDED.cik,
                    filing_date = EXCLUDED.filing_date,
                    fetch_status = EXCLUDED.fetch_status,
                    parse_status = EXCLUDED.parse_status,
                    last_attempt_at = NOW(),
                    retry_count = CASE
                        WHEN EXCLUDED.fetch_status = 'completed' AND EXCLUDED.parse_status = 'completed'
                            THEN guru_backfill_progress.retry_count
                        ELSE guru_backfill_progress.retry_count + 1
                    END,
                    error_message = EXCLUDED.error_message,
                    updated_at = NOW()
                """,
                (
                    guru_id,
                    guru_name,
                    manager_name,
                    cik,
                    accession_number,
                    filing_date,
                    fetch_status,
                    parse_status,
                    error_message,
                ),
            )
        self.connection.commit()
