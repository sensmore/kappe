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


def test_pointcloud2_json_to_mcap_conversion(tmp_path: Path):
    """Test PointCloud2 message conversion from JSON to MCAP."""
    # Create a PointCloud2 message with decoded points
    test_jsonl = tmp_path / 'pointcloud2_with_points.jsonl'

    pointcloud2_message = {
        'topic': '/lidar_scan',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1234567890, 'nanosec': 0}, 'frame_id': 'lidar_frame'},
            'height': 1,
            'width': 2,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 24,
            'data': [0] * 24,  # 2 points * 12 bytes per point
            'is_dense': True,
            # Add decoded points data
            'points': [{'x': 1.0, 'y': 2.0, 'z': 3.0}, {'x': 4.0, 'y': 5.0, 'z': 6.0}],
        },
    }

    test_jsonl.write_text(json.dumps(pointcloud2_message))

    # Convert to MCAP
    output_mcap = tmp_path / 'pointcloud2_output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_pointcloud2_json_to_mcap_without_points(tmp_path: Path):
    """Test PointCloud2 message conversion without decoded points."""
    # Create a PointCloud2 message without decoded points
    test_jsonl = tmp_path / 'pointcloud2_no_points.jsonl'

    pointcloud2_message = {
        'topic': '/lidar_raw',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1234567890, 'nanosec': 0}, 'frame_id': 'lidar_frame'},
            'height': 1,
            'width': 1,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 12,
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            'is_dense': True,
            # No 'points' field - should use raw data
        },
    }

    test_jsonl.write_text(json.dumps(pointcloud2_message))

    # Convert to MCAP
    output_mcap = tmp_path / 'pointcloud2_raw_output.mcap'
    json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


def test_pointcloud2_round_trip_with_points(tmp_path: Path):
    """Test round-trip conversion for PointCloud2 with decoded points."""
    # Create a PointCloud2 message with decoded points
    test_jsonl = tmp_path / 'pointcloud2_roundtrip.jsonl'

    original_message = {
        'topic': '/lidar_roundtrip',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1234567890, 'nanosec': 0}, 'frame_id': 'lidar_frame'},
            'height': 1,
            'width': 1,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 12,
            'data': [0] * 12,  # 1 point * 12 bytes
            'is_dense': True,
            'points': [{'x': 1.5, 'y': 2.5, 'z': 3.5}],
        },
    }

    test_jsonl.write_text(json.dumps(original_message))

    # Convert to MCAP
    temp_mcap = tmp_path / 'pointcloud2_roundtrip.mcap'
    json_to_mcap(temp_mcap, test_jsonl)

    # Convert back to JSON
    from io import StringIO

    from kappe.utils.mcap_to_json import mcap_to_json

    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify output
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1

    result = json.loads(lines[0].strip())
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


def test_pointcloud2_conversion_error_handling(tmp_path: Path):
    """Test error handling in PointCloud2 conversion."""
    # Create a malformed PointCloud2 message that should trigger error handling
    test_jsonl = tmp_path / 'pointcloud2_malformed.jsonl'

    malformed_message = {
        'topic': '/malformed_lidar',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1234567890, 'nanosec': 0}, 'frame_id': 'lidar'},
            'height': 1,
            'width': 1,
            'fields': [{'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1}],
            'is_bigendian': False,
            'point_step': 4,
            'row_step': 4,
            'data': [1, 2, 3, 4],
            'is_dense': True,
            'points': [{'invalid_field': 'bad_data'}],  # Invalid point structure
        },
    }

    test_jsonl.write_text(json.dumps(malformed_message))

    # Convert to MCAP - should not crash
    output_mcap = tmp_path / 'pointcloud2_malformed_output.mcap'
    with pytest.raises(ValueError, match='Error converting PointCloud2 message'):
        json_to_mcap(output_mcap, test_jsonl)

    # Verify MCAP file was created (fallback should work)
    assert output_mcap.exists()
    assert output_mcap.stat().st_size > 0


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
    unknown_message = {
        'topic': '/unknown_topic',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 1,
        'datatype': 'unknown_msgs/msg/UnknownType',
        'message': {'data': 'test'},
    }

    jsonl_file = tmp_path / 'unknown_type.jsonl'
    jsonl_file.write_text(json.dumps(unknown_message))

    mcap_file = tmp_path / 'output.mcap'

    with pytest.raises(
        ValueError, match='Message definition for unknown_msgs/msg/UnknownType not found'
    ):
        json_to_mcap(mcap_file, jsonl_file)
