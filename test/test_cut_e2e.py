import json
from pathlib import Path
from unittest.mock import patch

import pytest

from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json


def discover_cut_cases() -> list:
    cut_e2e_path = Path(__file__).parent / 'cut_e2e'
    return [
        pytest.param(yaml_path, id=str(yaml_path.relative_to(cut_e2e_path).with_suffix('')))
        for yaml_path in cut_e2e_path.rglob('*.yaml')
    ]


@pytest.mark.parametrize('case_yaml', discover_cut_cases())
def test_cut_e2e(case_yaml: Path, tmp_path: Path) -> None:
    """Full pipeline: JSONL → MCAP → kappe cut → multiple MCAPs → JSONL verification."""
    base = case_yaml.with_suffix('')  # strip '.yaml'
    input_jsonl = base.with_suffix('.input.jsonl')
    expected_dir = base.parent / f'{base.name}.expected'

    # Create input MCAP
    in_mcap = tmp_path / 'input.mcap'
    json_to_mcap(in_mcap, input_jsonl)

    out_dir = tmp_path / 'out'

    # patch argv
    with patch(
        'sys.argv',
        [
            'kappe',
            '--progress=false',
            'cut',
            '--config',
            str(case_yaml),
            '--output',
            str(out_dir),
            str(in_mcap),
        ],
    ):
        kappe_main()

    # Verify outputs match expected
    expected_files = list(expected_dir.glob('*.jsonl'))
    actual_files = list(out_dir.glob('*.mcap'))

    assert len(actual_files) == len(expected_files), (
        f'Expected {len(expected_files)} files, got {len(actual_files)}'
    )

    for expected_file in expected_files:
        # Find corresponding actual file
        expected_name = expected_file.stem
        actual_mcap = None

        if expected_name.endswith('.mcap'):
            expected_name = expected_name.removesuffix('.mcap')

        for actual_file in actual_files:
            if actual_file.stem == expected_name:
                actual_mcap = actual_file
                break

        assert actual_mcap is not None, f'Could not find actual file for {expected_name}'

        # Convert actual MCAP to JSONL for comparison
        actual_jsonl = tmp_path / f'{expected_name}_actual.jsonl'
        with actual_jsonl.open('w', encoding='utf-8') as fp:
            mcap_to_json(actual_mcap, fp)

        # Compare with expected
        with actual_jsonl.open(encoding='utf-8') as a, expected_file.open(encoding='utf-8') as e:
            actual_lines = [json.loads(line) for line in a if line.strip()]
            expected_lines = [json.loads(line) for line in e if line.strip()]

        assert actual_lines == expected_lines, f'Content mismatch for {expected_name}'
