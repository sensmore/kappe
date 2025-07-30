import json
from pathlib import Path

import pytest

from kappe.utils.mcap_to_json import mcap_to_json

from .conftest import e2e_test_helper

# Malformation types for testing robustness
MALFORMATION_TYPES = [
    pytest.param({}, id='normal'),
    pytest.param({'skip_index': True}, id='missing_index'),
    # pytest.param({'skip_footer': True}, id='missing_footer'), # noqa: ERA001 TODO
]


def discover_cut_cases() -> list:
    """Discover cut E2E test cases from YAML config files."""
    cut_e2e_path = Path(__file__).parent / 'cut'
    return [
        pytest.param(yaml_path, id=str(yaml_path.relative_to(cut_e2e_path).with_suffix('')))
        for yaml_path in cut_e2e_path.rglob('*.yaml')
    ]


@pytest.mark.parametrize('case_yaml', discover_cut_cases())
@pytest.mark.parametrize('malformed_options', MALFORMATION_TYPES)
def test_cut_e2e(case_yaml: Path, malformed_options: dict, tmp_path: Path) -> None:
    """Full pipeline: JSONL → MCAP → kappe cut → multiple MCAPs → JSONL verification."""
    base = case_yaml.with_suffix('')  # strip '.yaml'
    input_jsonl = base.with_suffix('.input.jsonl')
    expected_dir = base.parent / f'{base.name}.expected'
    error_json = base.with_suffix('.error.json')

    assert expected_dir.exists() != error_json.exists()

    e2e_test_helper(
        input_jsonl,
        command=[
            'cut',
            '--config',
            str(case_yaml),
            '--output',
            str(tmp_path),
            '--overwrite=true',
        ],
        error_json=error_json,
        malformed_options=malformed_options,
    )

    expected_files = sorted(expected_dir.glob('*.jsonl'))
    actual_files = sorted(tmp_path.glob('*.mcap'))

    assert len(actual_files) == len(expected_files), (
        f'Expected {len(expected_files)} files, got {len(actual_files)}'
    )

    for expected_file, actual_mcap in zip(expected_files, actual_files, strict=True):
        # Find corresponding actual file
        expected_name = expected_file.stem

        assert actual_mcap is not None, f'Could not find actual file for {expected_name}'

        # Convert actual MCAP to JSONL for comparison
        actual_jsonl = tmp_path / f'{expected_name}_actual.jsonl'
        with actual_jsonl.open('w', encoding='utf-8') as fp:
            mcap_to_json(actual_mcap, fp)

        # Compare with expected
        with actual_jsonl.open(encoding='utf-8') as a, expected_file.open(encoding='utf-8') as e:
            actual_lines = [json.loads(line) for line in a if line.strip()]
            expected_lines = [json.loads(line) for line in e if line.strip()]

        assert actual_lines == expected_lines, (
            f'Content mismatch for {expected_name}. '
            f'Expected {len(expected_lines)} messages, got {len(actual_lines)}'
        )
