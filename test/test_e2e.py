import json
from pathlib import Path
from unittest.mock import patch

import pytest

from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json


def discover_cases() -> list:
    e2e_path = Path(__file__).parent / 'e2e'
    return [
        pytest.param(yaml_path, id=str(yaml_path.relative_to(e2e_path).with_suffix('')))
        for yaml_path in e2e_path.rglob('*.yaml')
    ]


@pytest.mark.parametrize('case_yaml', discover_cases())
def test_e2e(case_yaml: Path, tmp_path: Path) -> None:
    """Full pipeline: JSONL → MCAP → kappe → MCAP → JSONL."""
    base = case_yaml.with_suffix('')  # strip ".yaml"
    input_jsonl = base.with_suffix('.input.jsonl')
    expected_jsonl = base.with_suffix('.expected.jsonl')

    in_mcap = tmp_path / 'input.mcap'
    json_to_mcap(in_mcap, input_jsonl)

    out_dir = tmp_path / 'out'
    out_dir.mkdir()

    # patch argv
    with patch(
        'sys.argv', ['kappe', '--progress=false', 'convert', '--config', str(case_yaml), str(in_mcap), str(out_dir)]
    ):
        kappe_main()

    out_mcap = out_dir / 'input.mcap'  # kappe keeps name
    actual_jsonl = tmp_path / 'actual.jsonl'
    with actual_jsonl.open('w', encoding='utf-8') as fp:
        mcap_to_json(out_mcap, fp)

    with actual_jsonl.open(encoding='utf-8') as a, expected_jsonl.open(encoding='utf-8') as e:
        actual_lines = [json.loads(line) for line in a]
        expected_lines = [json.loads(line) for line in e]
    assert actual_lines == expected_lines
