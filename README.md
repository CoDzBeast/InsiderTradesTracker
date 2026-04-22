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
- `guru_changes` (includes `company_id` for robust canonical-sector rollups)
- `companies` (SEC enrichment + internal sector buckets)
- `sic_sector_map` (maintainable SEC-to-internal mapping rules)

Schema and DB bootstrap live in `tracker/db.py`.

## Scripts

- `python scripts/init_db.py` – create data folder + SQLite DB + all required tables.
- `python scripts/backfill_gurus.py --resume --quarters 2 --limit-gurus 5` – polite/resumable safe backfill.
- `python scripts/update_gurus.py` – incremental latest filings refresh.
- `python scripts/compute_changes.py` – compute and persist QoQ changes (keyed by canonical company identity when available).
- `python scripts/enrich_companies.py --show-unmapped` – enrich holdings with SEC SIC inputs and map into canonical sectors.
- `python scripts/rematch_companies.py --limit 500` – rerun canonical company identity matching on unresolved holdings.
- `python scripts/review_queue.py --holdings-limit 100 --companies-limit 100` – print unresolved/review queues.
- `python scripts/run_guru_backend.py --resume --quarters 2 --limit-gurus 5` – end-to-end backend run.

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


## Canonical sector taxonomy

Canonical sector values are centralized in `tracker/gurus/classification.py` as `CANONICAL_SECTORS`.
`companies.sector_bucket` stores one of these exact values (or `NULL` if unmapped).

Mapping flow:
1. Fetch SEC-native attributes (`sic`, `sicDescription`, issuer metadata) from `https://data.sec.gov/submissions/CIK##########.json`.
2. Map SEC SIC code/description into internal sectors via deterministic rules in `sic_sector_map` + classifier fallbacks.
3. Persist canonical output to `companies.sector_bucket` for frontend grouping.

Fallback order used by classifier:
1. exact SIC code match
2. SIC description keyword match
3. normalized issuer-name heuristic
4. mark as unmapped (`sector_bucket=NULL`, `needs_classification=1`)

## Sector aggregation query helpers

`GuruQueryService` includes helper methods for:
- NEW/ADD/EXIT counts by sector
- net sector movement
- top sectors for a guru
- all gurus buying in a given sector
- unmapped company review queue
