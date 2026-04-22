import importlib.util
from decimal import Decimal
from pathlib import Path
import sys


def _load_changes_module():
    module_path = Path(__file__).resolve().parents[2] / 'tracker' / 'gurus' / 'changes.py'
    spec = importlib.util.spec_from_file_location('changes', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_classify_changes_uses_company_identity_when_cusip_changes():
    module = _load_changes_module()
    current = [
        {'issuer_name': 'Acme Corp', 'cusip': '123456789', 'shares': Decimal('120'), 'company_id': 10},
    ]
    previous = [
        {'issuer_name': 'Acme Corporation', 'cusip': '000000000', 'shares': Decimal('100'), 'company_id': 10},
    ]

    rows = module.classify_changes(current=current, previous=previous)

    assert len(rows) == 1
    assert rows[0]['change_type'] == 'ADD'
    assert rows[0]['delta_shares'] == Decimal('20')
    assert rows[0]['company_id'] == 10


def test_classify_changes_falls_back_to_name_when_cusip_missing():
    module = _load_changes_module()
    current = [
        {'issuer_name': 'No Cusip Name', 'cusip': '', 'shares': Decimal('15'), 'company_id': None},
    ]
    previous = [
        {'issuer_name': 'No Cusip Name', 'cusip': '', 'shares': Decimal('25'), 'company_id': None},
    ]

    rows = module.classify_changes(current=current, previous=previous)

    assert len(rows) == 1
    assert rows[0]['change_type'] == 'REDUCE'
    assert rows[0]['cusip'] == ''
