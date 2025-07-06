import json
from pathlib import Path

import pytest

from kappe.utils.json_to_mcap import json_to_mcap


def test_basic_conversion(tmp_path: Path):
    """Test basic JSONL to MCAP conversion."""
    # Create test JSONL file
    test_jsonl = tmp_path / 'test.jsonl'
    message_data = {
        'topic': '/test_topic',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'std_msgs/msg/Bool',
        'message': {'data': True},
    }
    test_jsonl.write_text(json.dumps(message_data))

    # Convert to MCAP
    output_mcap = tmp_path / 'output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_file_not_found(tmp_path: Path):
    """Test error handling for non-existent input file."""
    non_existent_jsonl = tmp_path / 'non_existent.jsonl'
    output_mcap = tmp_path / 'output.mcap'

    with pytest.raises(FileNotFoundError):
        json_to_mcap(output_mcap, non_existent_jsonl)


def test_invalid_message_definition(tmp_path: Path):
    """Test error handling for unknown message types."""
    test_jsonl = tmp_path / 'test.jsonl'
    message_data = {
        'topic': '/test_topic',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'unknown_msgs/msg/UnknownType',
        'message': {'data': True},
    }
    test_jsonl.write_text(json.dumps(message_data))

    output_mcap = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='Message definition for .* not found'):
        json_to_mcap(output_mcap, test_jsonl)


def test_multiple_message_types(tmp_path: Path):
    """Test conversion with multiple message types."""
    test_jsonl = tmp_path / 'test.jsonl'
    messages = [
        {
            'topic': '/bool_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': True},
        },
        {
            'topic': '/bool_topic',
            'log_time': 1234567891,
            'publish_time': 1234567891,
            'sequence': 1,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': False},
        },
    ]

    test_jsonl.write_text('\n'.join(json.dumps(msg) for msg in messages))

    output_mcap = tmp_path / 'output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_round_trip_conversion(tmp_path: Path):
    """Test round-trip conversion: JSONL -> MCAP -> JSONL."""
    # Use existing test data if available
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    # Convert to MCAP
    temp_mcap = tmp_path / 'temp.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert back to JSONL
    from io import StringIO

    from kappe.utils.mcap_to_json import mcap_to_json

    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify we get valid JSON output
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) > 0

    # Each line should be valid JSON with expected structure
    for line in lines:
        message = json.loads(line.strip())
        assert all(
            key in message
            for key in ['topic', 'log_time', 'publish_time', 'sequence', 'datatype', 'message']
        )
