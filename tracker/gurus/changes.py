"""Quarter-over-quarter holding change detection."""

from __future__ import annotations

from decimal import Decimal

from tracker.gurus.repository import GuruRepository


def classify_changes(current: dict, previous: dict) -> list[dict]:
    """Classify position changes as NEW/ADD/REDUCE/EXIT."""

    all_cusips = set(current) | set(previous)
    rows: list[dict] = []

    for cusip in sorted(all_cusips):
        cur_issuer, cur_shares = current.get(cusip, ('', Decimal('0')))
        prev_issuer, prev_shares = previous.get(cusip, ('', Decimal('0')))

        issuer_name = cur_issuer or prev_issuer
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

    repo = GuruRepository(connection)
    summary = {'gurus': 0, 'changes': 0}

    for guru_id, _guru_name in repo.enabled_gurus():
        filings = repo.latest_two_filings(guru_id)
        if len(filings) < 2:
            continue

        latest_filing_id = filings[0][0]
        previous_filing_id = filings[1][0]

        current = repo.holdings_by_filing(latest_filing_id)
        previous = repo.holdings_by_filing(previous_filing_id)
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
