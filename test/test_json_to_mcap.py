import json
from pathlib import Path

import pytest

from kappe.utils.json_to_mcap import json_to_mcap

from .conftest import (
    create_test_data_message,
    create_test_jsonl,
    pointcloud2_message_factory,
)


def test_basic_conversion(tmp_path: Path, sample_bool_message: dict) -> None:
    """Test basic JSONL to MCAP conversion."""
    # Create test JSONL file
    test_jsonl = tmp_path / 'test.jsonl'
    test_jsonl.write_text(json.dumps(sample_bool_message))

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
    message_data = create_test_data_message(
        datatype='unknown_msgs/msg/UnknownType', message_data={'data': True}
    )
    test_jsonl.write_text(json.dumps(message_data))

    output_mcap = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='Message definition for .* not found'):
        json_to_mcap(output_mcap, test_jsonl)


def test_multiple_message_types(tmp_path: Path) -> None:
    """Test conversion with multiple message types."""
    test_jsonl = tmp_path / 'test.jsonl'
    messages = [
        create_test_data_message(topic='/bool_topic', message_data={'data': True}, sequence=0),
        create_test_data_message(topic='/bool_topic', message_data={'data': False}, sequence=1),
    ]

    create_test_jsonl(messages, test_jsonl)

    output_mcap = tmp_path / 'output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_pointcloud2_conversion_error_handling(tmp_path: Path) -> None:
    """Test error handling in PointCloud2 conversion."""
    # Create a malformed PointCloud2 message that should trigger error handling
    test_jsonl = tmp_path / 'pointcloud2_malformed.jsonl'
    malformed_message = pointcloud2_message_factory(
        topic='/malformed_lidar', width=1, frame_id='lidar'
    )
    # Override with invalid point structure
    malformed_message['message']['points'] = [
        {'invalid_field': 'bad_data'}
    ]  # Invalid point structure

    test_jsonl.write_text(json.dumps(malformed_message))

    # Convert to MCAP - should raise error for invalid point structure
    output_mcap = tmp_path / 'pointcloud2_malformed_output.mcap'
    with pytest.raises(ValueError, match='Error converting PointCloud2 message'):
        json_to_mcap(output_mcap, test_jsonl)


def test_non_jsonl_file_extension(tmp_path: Path):
    """Test that non-JSONL files are rejected."""
    # Create a file with wrong extension
    json_file = tmp_path / 'test.json'
    json_file.write_text('{"test": "data"}')

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='File must be a JSONL file'):
        json_to_mcap(mcap_file, json_file)


def test_invalid_json_in_file(tmp_path: Path):
    """Test handling of invalid JSON in JSONL file."""
    # Create a file with invalid JSON
    jsonl_file = tmp_path / 'invalid.jsonl'
    jsonl_file.write_text('{"valid": "json"}\n{invalid json}\n')

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='Error parsing message data'):
        json_to_mcap(mcap_file, jsonl_file)


def test_invalid_message_structure(tmp_path: Path):
    """Test handling of invalid message structure."""
    # Create a file with invalid message structure (missing required fields)
    jsonl_file = tmp_path / 'invalid_structure.jsonl'
    jsonl_file.write_text('{"invalid": "message"}\n')

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='Error parsing message data'):
        json_to_mcap(mcap_file, jsonl_file)
