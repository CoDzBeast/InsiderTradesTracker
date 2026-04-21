import importlib.util
from pathlib import Path
import sys


def _load_classification_module():
    module_path = Path(__file__).resolve().parents[2] / 'tracker' / 'gurus' / 'classification.py'
    spec = importlib.util.spec_from_file_location('classification', module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_classify_exact_sic_match():
    module = _load_classification_module()
    classifier = module.SectorClassifier()
    result = classifier.classify(sic_code='3674', sic_description='SEMICONDUCTORS', company_name='Acme')
    assert result.sector_bucket == 'Semiconductors'
    assert result.needs_classification is False


def test_classify_keyword_fallback():
    module = _load_classification_module()
    classifier = module.SectorClassifier(exact_sic_map={})
    result = classifier.classify(sic_code=None, sic_description='State commercial banks', company_name='Example')
    assert result.sector_bucket == 'Banks'


def test_classify_name_heuristic_fallback():
    module = _load_classification_module()
    classifier = module.SectorClassifier(exact_sic_map={})
    result = classifier.classify(sic_code=None, sic_description=None, company_name='Example Energy Corp')
    assert result.sector_bucket == 'Oil & Gas'


def test_classify_unmapped():
    module = _load_classification_module()
    classifier = module.SectorClassifier(exact_sic_map={})
    result = classifier.classify(sic_code=None, sic_description='Unknown', company_name='Random Holdings')
    assert result.sector_bucket is None
    assert result.needs_classification is True
