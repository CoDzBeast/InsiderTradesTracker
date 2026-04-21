# InsiderTradesTracker (Guru 13F Backend)

This project is being refactored into a focused backend for tracking selected institutional investors ("gurus") through SEC Form 13F filings.

## Scope

- Tracks only the managers configured in `config/tracked_gurus.json`
- Uses only SEC EDGAR endpoints (no paid APIs)
- Ingests 13F-HR filings and parses `informationTable.xml` holdings
- Computes quarter-over-quarter position changes (`NEW`, `ADD`, `REDUCE`, `EXIT`)

## Data model (Postgres)

Core tables:
- `tracked_gurus`
- `guru_filings`
- `guru_holdings`
- `guru_changes`
- `guru_backfill_progress` (resumable fetch/parse checkpoints per guru filing)

Schema SQL lives in `tracker/gurus/models.py`.

## Scripts

Set `DATABASE_URL` to a Postgres DSN and run:

- `python scripts/backfill_gurus.py --resume --quarters 2 --limit-gurus 20` – initialize schema + polite/resumable latest-2-quarters backfill
- `python scripts/update_gurus.py` – incremental update for latest 13F filings
- `python scripts/compute_changes.py` – compute and persist QoQ changes

### Safe SEC backfill settings (environment variables)

These default to conservative values if not provided:

- `SEC_USER_AGENT=\"GuruTracker/0.1 your_email@example.com\"`
- `SEC_TIMEOUT_SECONDS=20`
- `SEC_MAX_RETRIES=5`
- `SEC_BASE_DELAY_SECONDS=1.0`
- `SEC_BACKOFF_BASE_SECONDS=2.0`
- `SEC_MAX_BACKOFF_SECONDS=60`
- `SEC_ENABLE_CACHE=true`
- `SEC_INITIAL_QUARTERS=2`
- `SEC_GURU_BATCH_SIZE=20`

The backfill script also supports:

- `--latest-only` (forces one quarter)
- `--quarters N` (latest N quarters per guru)
- `--limit-gurus N`
- `--resume` (skip already-completed filings; retry only incomplete/failed)

## Notes

- Legacy Form 4 parsing code is retained but broad ingestion is disabled unless
  `ENABLE_FORM4_INGESTION=1` is explicitly set.
