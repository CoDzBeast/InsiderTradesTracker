import importlib.util
from pathlib import Path
import sys


def _load_db_module():
    module_path = Path(__file__).resolve().parents[2] / 'tracker' / 'db.py'
    spec = importlib.util.spec_from_file_location('db', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_init_db_has_company_id_on_guru_changes():
    module = _load_db_module()
    module.init_db()
    with module.get_conn() as conn:
        columns = {row['name'] for row in conn.execute("PRAGMA table_info('guru_changes')").fetchall()}

    assert 'company_id' in columns
