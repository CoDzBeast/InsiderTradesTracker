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

Schema SQL lives in `tracker/gurus/models.py`.

## Scripts

Set `DATABASE_URL` to a Postgres DSN and run:

- `python scripts/backfill_gurus.py` – initialize schema + deeper filing history backfill
- `python scripts/update_gurus.py` – incremental update for latest 13F filings
- `python scripts/compute_changes.py` – compute and persist QoQ changes

## Notes

- Legacy Form 4 parsing code is retained but broad ingestion is disabled unless
  `ENABLE_FORM4_INGESTION=1` is explicitly set.
