"""Database helpers for guru tracking persistence."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

from defs import PROJECT_PATH

try:
    from config import DATABASE
except ImportError:  # pragma: no cover
    DATABASE = {}


DB_ENGINE = str(DATABASE.get('engine', 'sqlite')).lower()
DB_PATH = str(DATABASE.get('path', 'data/gurudb.sqlite3'))


def _sqlite_path() -> Path:
    return PROJECT_PATH / DB_PATH


def get_conn() -> sqlite3.Connection:
    """Get a database connection for the configured engine."""

    if DB_ENGINE == 'sqlite':
        db_file = _sqlite_path()
        db_file.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        return conn

    if DB_ENGINE == 'postgres':
        raise NotImplementedError('Postgres engine is not implemented yet')

    raise ValueError(f'Unsupported database engine: {DB_ENGINE}')


def execute(query: str, params: Iterable[Any] | None = None) -> None:
    """Execute a write query and commit the transaction."""

    with get_conn() as conn:
        conn.execute(query, tuple(params or ()))
        conn.commit()


def fetch_all(query: str, params: Iterable[Any] | None = None) -> list[sqlite3.Row]:
    """Execute a read query and return all rows."""

    with get_conn() as conn:
        cursor = conn.execute(query, tuple(params or ()))
        return cursor.fetchall()


def fetch_one(query: str, params: Iterable[Any] | None = None) -> sqlite3.Row | None:
    """Execute a read query and return one row."""

    with get_conn() as conn:
        cursor = conn.execute(query, tuple(params or ()))
        return cursor.fetchone()


def init_db() -> None:
    """Initialize schema required for guru tracking."""

    if DB_ENGINE == 'postgres':
        raise NotImplementedError('Postgres initialization is not implemented yet')

    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tracked_gurus (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guru_name TEXT NOT NULL,
                manager_name TEXT NOT NULL,
                cik TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (manager_name)
            );

            CREATE TABLE IF NOT EXISTS guru_filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guru_id INTEGER NOT NULL,
                accession_number TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                report_period TEXT,
                form_type TEXT NOT NULL,
                fetch_status TEXT NOT NULL DEFAULT 'pending',
                parse_status TEXT NOT NULL DEFAULT 'pending',
                last_attempt_at TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                raw_index_path TEXT,
                raw_xml_path TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (guru_id) REFERENCES tracked_gurus(id) ON DELETE CASCADE,
                UNIQUE (guru_id, accession_number)
            );

            CREATE TABLE IF NOT EXISTS guru_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                issuer_name TEXT NOT NULL,
                raw_issuer_name TEXT,
                normalized_issuer_name TEXT,
                cusip TEXT,
                shares NUMERIC,
                value_usd NUMERIC,
                put_call TEXT,
                discretion TEXT,
                company_id INTEGER,
                match_status TEXT NOT NULL DEFAULT 'unmapped',
                match_confidence REAL,
                match_notes TEXT,
                FOREIGN KEY (filing_id) REFERENCES guru_filings(id) ON DELETE CASCADE,
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL,
                UNIQUE (filing_id, issuer_name, cusip)
            );

            CREATE TABLE IF NOT EXISTS guru_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guru_id INTEGER NOT NULL,
                current_filing_id INTEGER NOT NULL,
                previous_filing_id INTEGER NOT NULL,
                company_id INTEGER,
                issuer_name TEXT NOT NULL,
                cusip TEXT,
                current_shares NUMERIC,
                previous_shares NUMERIC,
                delta_shares NUMERIC,
                delta_percent NUMERIC,
                change_type TEXT NOT NULL,
                FOREIGN KEY (guru_id) REFERENCES tracked_gurus(id) ON DELETE CASCADE,
                FOREIGN KEY (current_filing_id) REFERENCES guru_filings(id) ON DELETE CASCADE,
                FOREIGN KEY (previous_filing_id) REFERENCES guru_filings(id) ON DELETE CASCADE,
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cik TEXT,
                ticker TEXT,
                cusip TEXT,
                company_name TEXT NOT NULL,
                normalized_company_name TEXT,
                sic_code TEXT,
                sic_description TEXT,
                sector_bucket TEXT,
                industry_bucket TEXT,
                classification_status TEXT NOT NULL DEFAULT 'unmapped',
                needs_review INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'sec',
                needs_classification INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (company_name, cusip)
            );

            CREATE TABLE IF NOT EXISTS company_identity_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_issuer_name TEXT,
                normalized_issuer_name TEXT,
                cusip TEXT,
                ticker TEXT,
                forced_company_id INTEGER NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (forced_company_id) REFERENCES companies(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sic_sector_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sic_code TEXT,
                sic_description TEXT,
                sector_bucket TEXT NOT NULL,
                industry_bucket TEXT,
                notes TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_guru_filings_guru_id ON guru_filings(guru_id);
            CREATE INDEX IF NOT EXISTS idx_guru_filings_report_period ON guru_filings(guru_id, report_period DESC);
            CREATE INDEX IF NOT EXISTS idx_guru_holdings_filing_id ON guru_holdings(filing_id);
            CREATE INDEX IF NOT EXISTS idx_guru_holdings_cusip ON guru_holdings(cusip);
            CREATE INDEX IF NOT EXISTS idx_guru_holdings_company_id ON guru_holdings(company_id);
            CREATE INDEX IF NOT EXISTS idx_guru_holdings_match_status ON guru_holdings(match_status);
            CREATE INDEX IF NOT EXISTS idx_guru_changes_guru_id ON guru_changes(guru_id);
            CREATE INDEX IF NOT EXISTS idx_guru_changes_company_id ON guru_changes(company_id);
            CREATE INDEX IF NOT EXISTS idx_companies_cusip ON companies(cusip);
            CREATE INDEX IF NOT EXISTS idx_companies_normalized_name ON companies(normalized_company_name);
            CREATE INDEX IF NOT EXISTS idx_companies_sector_bucket ON companies(sector_bucket);
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_companies_name_cusip_nonempty
                ON companies(normalized_company_name, cusip)
                WHERE normalized_company_name IS NOT NULL AND TRIM(normalized_company_name) <> ''
                  AND cusip IS NOT NULL AND TRIM(cusip) <> '';
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_companies_name_no_cusip
                ON companies(normalized_company_name)
                WHERE normalized_company_name IS NOT NULL AND TRIM(normalized_company_name) <> ''
                  AND (cusip IS NULL OR TRIM(cusip) = '');
            CREATE INDEX IF NOT EXISTS idx_sic_sector_map_code ON sic_sector_map(sic_code);
            CREATE INDEX IF NOT EXISTS idx_company_identity_override_lookup
                ON company_identity_overrides(normalized_issuer_name, cusip, ticker);
            """
        )

        _ensure_column(conn, 'guru_filings', 'created_at', "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, 'companies', 'needs_classification', "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, 'companies', 'normalized_company_name', "TEXT")
        _ensure_column(conn, 'companies', 'classification_status', "TEXT NOT NULL DEFAULT 'unmapped'")
        _ensure_column(conn, 'companies', 'needs_review', "INTEGER NOT NULL DEFAULT 0")

        _ensure_column(conn, 'guru_holdings', 'raw_issuer_name', "TEXT")
        _ensure_column(conn, 'guru_holdings', 'normalized_issuer_name', "TEXT")
        _ensure_column(conn, 'guru_holdings', 'company_id', "INTEGER REFERENCES companies(id) ON DELETE SET NULL")
        _ensure_column(conn, 'guru_holdings', 'match_status', "TEXT NOT NULL DEFAULT 'unmapped'")
        _ensure_column(conn, 'guru_holdings', 'match_confidence', "REAL")
        _ensure_column(conn, 'guru_holdings', 'match_notes', "TEXT")
        _ensure_column(conn, 'guru_changes', 'company_id', "INTEGER REFERENCES companies(id) ON DELETE SET NULL")

        _backfill_identity_columns(conn)
        _seed_sic_sector_map(conn)
        _ensure_metadata_table(conn)
        _upsert_schema_version(conn, version='1')

        conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
    columns = {row['name'] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
    if column not in columns:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {ddl_type}')


def _seed_sic_sector_map(conn: sqlite3.Connection) -> None:
    from tracker.gurus.classification import DEFAULT_SIC_RULES

    existing = conn.execute('SELECT COUNT(1) AS count FROM sic_sector_map').fetchone()
    if existing and int(existing['count']) > 0:
        return

    conn.executemany(
        """
        INSERT INTO sic_sector_map (sic_code, sic_description, sector_bucket, industry_bucket, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                row.get('sic_code'),
                row.get('sic_description'),
                row['sector_bucket'],
                row.get('industry_bucket'),
                row.get('notes'),
            )
            for row in DEFAULT_SIC_RULES
        ],
    )


def _backfill_identity_columns(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE guru_holdings
        SET raw_issuer_name = COALESCE(raw_issuer_name, issuer_name)
        WHERE raw_issuer_name IS NULL
        """
    )
    conn.execute(
        """
        UPDATE guru_holdings
        SET normalized_issuer_name = LOWER(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(COALESCE(raw_issuer_name, issuer_name, ''), '.', ' '),
                    ',', ' '),
                '&', ' and ')
            )
        )
        WHERE normalized_issuer_name IS NULL OR TRIM(normalized_issuer_name) = ''
        """
    )
    conn.execute(
        """
        UPDATE guru_holdings
        SET cusip = UPPER(TRIM(cusip))
        WHERE cusip IS NOT NULL
        """
    )


def _ensure_metadata_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _upsert_schema_version(conn: sqlite3.Connection, version: str) -> None:
    conn.execute(
        """
        INSERT INTO app_metadata (key, value, updated_at)
        VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
        ON CONFLICT (key)
        DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
        """,
        (version,),
    )
