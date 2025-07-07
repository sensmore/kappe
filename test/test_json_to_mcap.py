import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from conftest import create_test_data_message

if TYPE_CHECKING:
    from collections.abc import Callable

from kappe.utils.json_to_mcap import json_to_mcap


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


def test_multiple_message_types(tmp_path: Path, create_test_jsonl: 'Callable') -> None:
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


def test_round_trip_conversion(
    tmp_path: Path, sample_bool_message: dict, mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test round-trip conversion: JSONL -> MCAP -> JSONL."""
    # Use mcap_roundtrip_helper for consistent testing
    result = mcap_roundtrip_helper(sample_bool_message, tmp_path)

    # Verify the result has expected structure
    assert all(
        key in result
        for key in ['topic', 'log_time', 'publish_time', 'sequence', 'datatype', 'message']
    )
    assert result['topic'] == sample_bool_message['topic']
    assert result['datatype'] == sample_bool_message['datatype']


def test_pointcloud2_json_to_mcap_conversion(
    tmp_path: Path, pointcloud2_message_factory: 'Callable'
) -> None:
    """Test PointCloud2 message conversion from JSON to MCAP."""
    # Create a PointCloud2 message with decoded points
    test_jsonl = tmp_path / 'pointcloud2_with_points.jsonl'
    pointcloud2_message = pointcloud2_message_factory(
        topic='/lidar_scan',
        width=2,
        points=[{'x': 1.0, 'y': 2.0, 'z': 3.0}, {'x': 4.0, 'y': 5.0, 'z': 6.0}],
    )

    test_jsonl.write_text(json.dumps(pointcloud2_message))

    # Convert to MCAP
    output_mcap = tmp_path / 'pointcloud2_output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_pointcloud2_json_to_mcap_without_points(
    tmp_path: Path, pointcloud2_message_factory: 'Callable'
) -> None:
    """Test PointCloud2 message conversion without decoded points."""
    # Create a PointCloud2 message without decoded points
    test_jsonl = tmp_path / 'pointcloud2_no_points.jsonl'
    pointcloud2_message = pointcloud2_message_factory(
        topic='/lidar_raw', width=1, include_points=False
    )
    # Override data field for raw data
    pointcloud2_message['message']['data'] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    test_jsonl.write_text(json.dumps(pointcloud2_message))

    # Convert to MCAP
    output_mcap = tmp_path / 'pointcloud2_raw_output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_pointcloud2_round_trip_with_points(
    tmp_path: Path, pointcloud2_message_factory: 'Callable', mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test round-trip conversion for PointCloud2 with decoded points."""
    # Create a PointCloud2 message with decoded points
    original_message = pointcloud2_message_factory(
        topic='/lidar_roundtrip', width=1, points=[{'x': 1.5, 'y': 2.5, 'z': 3.5}]
    )

    # Use roundtrip helper
    result = mcap_roundtrip_helper(original_message, tmp_path)

    assert result['topic'] == '/lidar_roundtrip'
    assert result['datatype'] == 'sensor_msgs/msg/PointCloud2'
    assert 'message' in result

    # Check that the message structure is preserved
    message = result['message']
    assert 'header' in message
    assert 'fields' in message
    assert 'points' in message  # After roundtrip, raw data becomes decoded points

    # The roundtrip should preserve the basic structure
    assert message['header']['frame_id'] == 'lidar_frame'
    assert len(message['fields']) == 3
    assert message['fields'][0]['name'] == 'x'

    # Check that the points are properly decoded
    assert len(message['points']) == 1
    assert message['points'][0]['x'] == 1.5
    assert message['points'][0]['y'] == 2.5
    assert message['points'][0]['z'] == 3.5


def test_pointcloud2_conversion_error_handling(
    tmp_path: Path, pointcloud2_message_factory: 'Callable'
) -> None:
    """Test error handling in PointCloud2 conversion."""
    # Create a malformed PointCloud2 message that should trigger error handling
    test_jsonl = tmp_path / 'pointcloud2_malformed.jsonl'
    malformed_message = pointcloud2_message_factory(
        topic='/malformed_lidar', width=1, frame_id='lidar'
    )
    # Override with invalid point structure
    malformed_message['message']['fields'] = [{'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1}]
    malformed_message['message']['point_step'] = 4
    malformed_message['message']['row_step'] = 4
    malformed_message['message']['data'] = [1, 2, 3, 4]
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


def test_empty_file(tmp_path: Path):
    """Test handling of empty JSONL file."""
    # Create an empty file
    jsonl_file = tmp_path / 'empty.jsonl'
    jsonl_file.write_text('')

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='No valid messages found in file'):
        json_to_mcap(mcap_file, jsonl_file)


def test_file_with_only_whitespace(tmp_path: Path):
    """Test handling of file with only whitespace."""
    # Create a file with only whitespace
    jsonl_file = tmp_path / 'whitespace.jsonl'
    jsonl_file.write_text('   \n  \t  \n')

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(ValueError, match='No valid messages found in file'):
        json_to_mcap(mcap_file, jsonl_file)


def test_unknown_message_type(tmp_path: Path):
    """Test handling of unknown message type."""
    # Create a file with unknown message type
    unknown_message = create_test_data_message(
        datatype='unknown_msgs/msg/UnknownType',
        topic='/unknown_topic',
        message_data={'data': 'test'},
        sequence=1,
    )

    jsonl_file = tmp_path / 'unknown_type.jsonl'
    jsonl_file.write_text(json.dumps(unknown_message))

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(
        ValueError, match='Message definition for unknown_msgs/msg/UnknownType not found'
    ):
        json_to_mcap(mcap_file, jsonl_file)
