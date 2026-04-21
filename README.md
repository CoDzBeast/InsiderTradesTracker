# InsiderTradesTracker (Guru 13F Backend)

This project is being refactored into a focused backend for tracking selected institutional investors ("gurus") through SEC Form 13F filings.

## Scope

- Tracks only the managers configured in `config/tracked_gurus.json`
- Uses only SEC EDGAR endpoints (no paid APIs)
- Ingests 13F-HR filings and parses `informationTable.xml` holdings
- Computes quarter-over-quarter position changes (`NEW`, `ADD`, `REDUCE`, `EXIT`)
- Keeps legacy insider code in place but does not require broad insider ingestion

## Data model (SQLite now, Postgres-ready abstraction)

Core tables:
- `tracked_gurus`
- `guru_filings`
- `guru_holdings`
- `guru_changes`

Schema and DB bootstrap live in `tracker/db.py`.

## Scripts

- `python scripts/init_db.py` – create data folder + SQLite DB + all required tables.
- `python scripts/backfill_gurus.py --resume --quarters 2 --limit-gurus 5` – polite/resumable safe backfill.
- `python scripts/update_gurus.py` – incremental latest filings refresh.
- `python scripts/compute_changes.py` – compute and persist QoQ changes.

### Safe SEC backfill settings (environment variables)

Defaults are conservative if not provided:

- `SEC_USER_AGENT="GuruTracker/0.1 your_email@example.com"` (**must customize with real contact**)
- `SEC_TIMEOUT_SECONDS=20`
- `SEC_MAX_RETRIES=5`
- `SEC_BASE_DELAY_SECONDS=1.0`
- `SEC_BACKOFF_BASE_SECONDS=2.0`
- `SEC_MAX_BACKOFF_SECONDS=60`
- `SEC_ENABLE_CACHE=true`
- `SEC_INITIAL_QUARTERS=2`
- `SEC_GURU_BATCH_SIZE=20`
- `SEC_MAX_RETRIES_PER_FILING=5`

Backfill CLI options:

- `--latest-only` (forces one quarter)
- `--quarters N` (latest N quarters per guru)
- `--limit-gurus N`
- `--resume` (skip already-completed filings; retry only incomplete/failed)
- `--max-filing-retries N`

## Backend query helpers for frontend readiness

`tracker/gurus/queries.py` provides helper methods for:

- all tracked gurus
- latest filings for a guru
- latest holdings for a guru
- changes for a guru
- biggest adds/new/exits across gurus
- gurus holding a matching issuer/cusip

## Notes

- SEC raw responses are cached to `data/sec_cache/` with deterministic CIK/accession paths.
- `guru_filings` tracks `fetch_status`, `parse_status`, `retry_count`, `last_attempt_at`, and `error_message` so runs can resume safely.
