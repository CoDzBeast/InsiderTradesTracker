"""Read/query helpers for future frontend consumption."""

from __future__ import annotations

from tracker.gurus.repository import GuruRepository


class GuruQueryService:
    """Facade with clean read functions backed by GuruRepository."""

    def __init__(self):
        self.repo = GuruRepository()

    def get_all_tracked_gurus(self, enabled_only: bool = True) -> list[dict]:
        return self.repo.get_all_tracked_gurus(enabled_only=enabled_only)

    def get_latest_filings_for_guru(self, guru_id: int, limit: int = 8) -> list[dict]:
        return self.repo.get_latest_filings_for_guru(guru_id=guru_id, limit=limit)

    def get_latest_holdings_for_guru(self, guru_id: int, limit: int = 200) -> list[dict]:
        return self.repo.get_latest_holdings_for_guru(guru_id=guru_id, limit=limit)

    def get_changes_for_guru(self, guru_id: int, limit: int = 200) -> list[dict]:
        return self.repo.get_changes_for_guru(guru_id=guru_id, limit=limit)

    def get_biggest_adds(self, limit: int = 50) -> list[dict]:
        return self.repo.get_biggest_changes_across_gurus(change_type='ADD', limit=limit)

    def get_biggest_new_positions(self, limit: int = 50) -> list[dict]:
        return self.repo.get_biggest_changes_across_gurus(change_type='NEW', limit=limit)

    def get_biggest_exits(self, limit: int = 50) -> list[dict]:
        return self.repo.get_biggest_changes_across_gurus(change_type='EXIT', limit=limit)

    def find_gurus_holding(self, issuer_name: str | None = None, cusip: str | None = None) -> list[dict]:
        return self.repo.find_gurus_holding(issuer_name=issuer_name, cusip=cusip)

    def get_new_positions_by_sector(self) -> list[dict]:
        return self.repo.get_sector_change_counts(change_type='NEW')

    def get_add_positions_by_sector(self) -> list[dict]:
        return self.repo.get_sector_change_counts(change_type='ADD')

    def get_exit_positions_by_sector(self) -> list[dict]:
        return self.repo.get_sector_change_counts(change_type='EXIT')

    def get_net_sector_movement(self) -> list[dict]:
        return self.repo.get_sector_net_movement()

    def get_top_sectors_by_guru(self, guru_id: int, limit: int = 10) -> list[dict]:
        return self.repo.get_top_sectors_by_guru(guru_id=guru_id, limit=limit)

    def get_gurus_buying_sector(self, sector_bucket: str, limit: int = 200) -> list[dict]:
        return self.repo.get_gurus_buying_sector(sector_bucket=sector_bucket, limit=limit)

    def get_unmapped_companies(self, limit: int = 200) -> list[dict]:
        return self.repo.get_unmapped_companies(limit=limit)
