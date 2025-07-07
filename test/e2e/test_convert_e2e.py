import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable


def discover_cases() -> list:
    """Discover E2E test cases from YAML config files."""
    e2e_path = Path(__file__).parent / 'convert'
    return [
        pytest.param(yaml_path, id=str(yaml_path.relative_to(e2e_path).with_suffix('')))
        for yaml_path in e2e_path.rglob('*.yaml')
    ]


@pytest.mark.parametrize('case_yaml', discover_cases())
def test_e2e(case_yaml: Path, tmp_path: Path, e2e_test_helper: 'Callable') -> None:
    """Full pipeline: JSONL → MCAP → kappe → MCAP → JSONL."""
    base = case_yaml.with_suffix('')  # strip ".yaml"
    input_jsonl = base.with_suffix('.input.jsonl')
    expected_jsonl = base.with_suffix('.expected.jsonl')

    # Run E2E test using helper (passes file paths directly)
    actual_lines = e2e_test_helper(input_jsonl, case_yaml, tmp_path)

    # Read expected results
    with expected_jsonl.open(encoding='utf-8') as e:
        expected_lines = [json.loads(line.strip()) for line in e if line.strip()]

    assert actual_lines == expected_lines, (
        f'Output mismatch for test case {case_yaml.stem}. '
        f'Expected {len(expected_lines)} messages, got {len(actual_lines)}'
    )
