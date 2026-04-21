"""Database schema and helper SQL for 13F guru tracking."""

from __future__ import annotations

from dataclasses import dataclass


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tracked_gurus (
    id BIGSERIAL PRIMARY KEY,
    guru_name TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    cik VARCHAR(10),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (manager_name)
);

CREATE TABLE IF NOT EXISTS guru_filings (
    id BIGSERIAL PRIMARY KEY,
    guru_id BIGINT NOT NULL REFERENCES tracked_gurus(id) ON DELETE CASCADE,
    accession_number TEXT NOT NULL,
    filing_date DATE NOT NULL,
    report_period DATE,
    form_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (guru_id, accession_number)
);

CREATE TABLE IF NOT EXISTS guru_holdings (
    id BIGSERIAL PRIMARY KEY,
    filing_id BIGINT NOT NULL REFERENCES guru_filings(id) ON DELETE CASCADE,
    issuer_name TEXT NOT NULL,
    cusip TEXT,
    shares NUMERIC(24, 4),
    value_usd NUMERIC(24, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (filing_id, issuer_name, cusip)
);

CREATE TABLE IF NOT EXISTS guru_changes (
    id BIGSERIAL PRIMARY KEY,
    guru_id BIGINT NOT NULL REFERENCES tracked_gurus(id) ON DELETE CASCADE,
    issuer_name TEXT NOT NULL,
    cusip TEXT,
    current_shares NUMERIC(24, 4),
    previous_shares NUMERIC(24, 4),
    delta_shares NUMERIC(24, 4),
    delta_percent NUMERIC(18, 6),
    change_type TEXT NOT NULL CHECK (change_type IN ('NEW', 'ADD', 'REDUCE', 'EXIT')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS guru_backfill_progress (
    id BIGSERIAL PRIMARY KEY,
    guru_id BIGINT NOT NULL REFERENCES tracked_gurus(id) ON DELETE CASCADE,
    guru_name TEXT NOT NULL,
    manager_name TEXT NOT NULL,
    cik VARCHAR(10),
    accession_number TEXT NOT NULL,
    filing_date DATE NOT NULL,
    fetch_status TEXT NOT NULL DEFAULT 'pending',
    parse_status TEXT NOT NULL DEFAULT 'pending',
    last_attempt_at TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (guru_id, accession_number)
);

CREATE INDEX IF NOT EXISTS idx_guru_filings_guru_id ON guru_filings(guru_id);
CREATE INDEX IF NOT EXISTS idx_guru_holdings_filing_id ON guru_holdings(filing_id);
CREATE INDEX IF NOT EXISTS idx_guru_changes_guru_id ON guru_changes(guru_id);
CREATE INDEX IF NOT EXISTS idx_guru_backfill_progress_guru_id ON guru_backfill_progress(guru_id);
"""


@dataclass(slots=True)
class TrackedGuru:
    """Tracked guru record."""

    guru_name: str
    manager_name: str
    cik: str | None = None
    enabled: bool = True


def init_schema(connection) -> None:
    """Initialize Postgres schema using an open DB-API connection."""

    with connection.cursor() as cursor:
        cursor.execute(SCHEMA_SQL)
    connection.commit()
