"""Quarter-over-quarter holding change detection."""

from __future__ import annotations

from decimal import Decimal

def _canonical_key(row: dict) -> str:
    company_id = row.get('company_id')
    if company_id is not None:
        return f'company:{int(company_id)}'
    cusip = str(row.get('cusip') or '').strip().upper()
    if cusip:
        return f'cusip:{cusip}'
    return f"name:{str(row.get('issuer_name') or '').strip().lower()}"


def classify_changes(current: list[dict], previous: list[dict]) -> list[dict]:
    """Classify position changes as NEW/ADD/REDUCE/EXIT."""

    current_by_key = {_canonical_key(row): row for row in current}
    previous_by_key = {_canonical_key(row): row for row in previous}
    all_keys = set(current_by_key) | set(previous_by_key)
    rows: list[dict] = []

    for key in sorted(all_keys):
        cur_row = current_by_key.get(key, {})
        prev_row = previous_by_key.get(key, {})

        cur_issuer = str(cur_row.get('issuer_name') or '')
        prev_issuer = str(prev_row.get('issuer_name') or '')
        cur_cusip = str(cur_row.get('cusip') or '').strip().upper()
        prev_cusip = str(prev_row.get('cusip') or '').strip().upper()
        cur_shares = Decimal(str(cur_row.get('shares') or '0'))
        prev_shares = Decimal(str(prev_row.get('shares') or '0'))

        issuer_name = cur_issuer or prev_issuer
        cusip = cur_cusip or prev_cusip
        company_id = cur_row.get('company_id') or prev_row.get('company_id')
        delta = cur_shares - prev_shares

        if prev_shares == 0 and cur_shares > 0:
            change_type = 'NEW'
        elif cur_shares == 0 and prev_shares > 0:
            change_type = 'EXIT'
        elif delta > 0:
            change_type = 'ADD'
        elif delta < 0:
            change_type = 'REDUCE'
        else:
            continue

        if prev_shares > 0:
            delta_percent = (delta / prev_shares) * Decimal('100')
        else:
            delta_percent = None

        rows.append(
            {
                'issuer_name': issuer_name,
                'cusip': cusip,
                'company_id': company_id,
                'current_shares': cur_shares,
                'previous_shares': prev_shares,
                'delta_shares': delta,
                'delta_percent': delta_percent,
                'change_type': change_type,
            }
        )

    return rows


def compute_and_store_changes(connection) -> dict[str, int]:
    """Compute QoQ changes for each enabled guru and write to guru_changes."""

    from tracker.gurus.repository import GuruRepository

    repo = GuruRepository(connection)
    summary = {'gurus': 0, 'changes': 0}

    for guru_id, _guru_name in repo.enabled_gurus():
        filings = repo.latest_two_filings(guru_id)
        if len(filings) < 2:
            continue

        latest_filing_id = filings[0][0]
        previous_filing_id = filings[1][0]

        current = repo.holdings_snapshot_by_filing(latest_filing_id)
        previous = repo.holdings_snapshot_by_filing(previous_filing_id)
        changes = classify_changes(current=current, previous=previous)

        repo.clear_changes_for_guru(guru_id)
        repo.insert_changes(
            guru_id=guru_id,
            current_filing_id=latest_filing_id,
            previous_filing_id=previous_filing_id,
            changes=changes,
        )

        summary['gurus'] += 1
        summary['changes'] += len(changes)

    return summary
