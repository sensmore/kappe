import json
from io import StringIO
from pathlib import Path

import pytest

from kappe.utils.mcap_to_json import mcap_to_json

from .conftest import e2e_test_helper

# Malformation types for testing robustness
MALFORMATION_TYPES = [
    pytest.param({}, id='normal'),
    pytest.param({'skip_index': True}, id='missing_index'),
    pytest.param({'destroy_footer': True}, id='broken_footer'),
]


def discover_cases() -> list:
    """Discover E2E test cases from YAML config files."""
    e2e_path = Path(__file__).parent / 'convert'
    return [
        pytest.param(yaml_path, id=str(yaml_path.relative_to(e2e_path).with_suffix('')))
        for yaml_path in e2e_path.rglob('*.yaml')
    ]


@pytest.mark.parametrize('case_yaml', discover_cases())
@pytest.mark.parametrize('malformed_options', MALFORMATION_TYPES)
def test_e2e(
    case_yaml: Path, malformed_options: dict, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Full pipeline: JSONL → MCAP → kappe → MCAP → JSONL."""
    base = case_yaml.with_suffix('')  # strip ".yaml"
    input_jsonl = base.with_suffix('.input.jsonl')
    expected_jsonl = base.with_suffix('.expected.jsonl')
    error_json = base.with_suffix('.error.json')

    assert error_json.exists() != expected_jsonl.exists()

    e2e_test_helper(
        input_jsonl,
        [
            'convert',
            '--config',
            str(case_yaml),
        ],
        post_input_command=[
            str(tmp_path),
        ],
        error_json=error_json,
        malformed_options=malformed_options,
    )
    if error_json.exists():
        return

    # Check for indexing warnings based on malformed_options
    if malformed_options.get('skip_index'):
        expected_warning = 'No chunk indexes found in summary.'
        assert expected_warning in caplog.text, (
            f'Expected "{expected_warning}" for {malformed_options}'
        )
    elif malformed_options.get('destroy_footer'):
        expected_warning = 'Broken MCAP, trying to read, CAN BE SLOW!'
        assert expected_warning in caplog.text, (
            f'Expected "{expected_warning}" for {malformed_options}'
        )
    else:
        # Normal MCAP should not have any warnings about indexing/broken MCAP
        broken_warnings = [
            'No chunk indexes found in summary.',
            'Broken MCAP, trying to read, CAN BE SLOW!',
        ]
        for warning in broken_warnings:
            assert warning not in caplog.text, f'Unexpected warning "{warning}" for normal MCAP'

    output_buffer = StringIO()
    mcap_to_json(tmp_path / 'input.mcap', output_buffer)

    # Parse results
    output_buffer.seek(0)
    actual_lines = [json.loads(line.strip()) for line in output_buffer.readlines()]

    # Read expected results
    with expected_jsonl.open(encoding='utf-8') as e:
        expected_lines = [json.loads(line.strip()) for line in e if line.strip()]

    assert actual_lines == expected_lines, f'Output mismatch for test case {case_yaml.stem}. '
