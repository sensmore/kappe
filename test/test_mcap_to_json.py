import json
from io import StringIO
from pathlib import Path

import pytest

from kappe.utils.mcap_to_json import mcap_to_json


def test_file_not_found():
    """Test that FileNotFoundError is raised for non-existent files."""
    non_existent_file = Path('/non/existent/file.mcap')
    output_buffer = StringIO()

    with pytest.raises(FileNotFoundError, match='MCAP file not found'):
        mcap_to_json(non_existent_file, output_buffer)


def test_basic_conversion_with_valid_mcap(tmp_path: Path):
    """Test basic MCAP to JSONL conversion using existing test data."""
    # Use existing test data
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    # Create temporary MCAP file from existing JSONL
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert back to JSONL
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify output format
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) > 0

    # Each line should be valid JSON
    for line in lines:
        message = json.loads(line.strip())
        assert 'topic' in message
        assert 'log_time' in message
        assert 'publish_time' in message
        assert 'sequence' in message
        assert 'datatype' in message
        assert 'message' in message


def test_topic_filtering(tmp_path: Path):
    """Test topic filtering functionality."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Filter for specific topic
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=['/test_topic'])

    output_buffer.seek(0)
    lines = output_buffer.readlines()

    # All messages should be from the specified topic
    for line in lines:
        message = json.loads(line.strip())
        assert message['topic'] == '/test_topic'


def test_message_limit(tmp_path: Path):
    """Test message limit functionality."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Limit to 1 message
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=1)

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) <= 1


def test_empty_topics_list(tmp_path: Path):
    """Test behavior with empty topics list."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Empty topics list should return no messages (filters out all)
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=[])

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 0


def test_pointcloud2_conversion(tmp_path: Path):
    """Test PointCloud2 message conversion with decoded point data."""
    # Create a mock PointCloud2 message in JSON format
    test_jsonl = tmp_path / 'pointcloud2_test.jsonl'

    # Create a simplified PointCloud2 message structure
    pointcloud2_message = {
        'topic': '/lidar_points',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {'stamp': {'sec': 1234567890, 'nanosec': 0}, 'frame_id': 'lidar_frame'},
            'height': 1,
            'width': 3,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 36,
            'data': [0] * 36,  # 3 points * 12 bytes per point
            'is_dense': True,
        },
    }

    test_jsonl.write_text(json.dumps(pointcloud2_message))

    # Convert to MCAP
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'pointcloud2.mcap'
    json_to_mcap(temp_mcap, test_jsonl)

    # Convert back to JSON
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify output
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1

    result = json.loads(lines[0].strip())
    assert result['topic'] == '/lidar_points'
    assert result['datatype'] == 'sensor_msgs/msg/PointCloud2'
    assert 'message' in result

    # Check that the message contains the expected fields
    message = result['message']
    assert 'header' in message
    assert 'fields' in message
    assert 'points' in message  # mcap_to_json converts raw data to decoded points

    # Check that we have the expected number of points (3)
    assert isinstance(message['points'], list)
    assert len(message['points']) == 3

    # Check that all points have x, y, z coordinates
    for point in message['points']:
        assert 'x' in point
        assert 'y' in point
        assert 'z' in point


def test_pointcloud2_error_handling(tmp_path: Path):
    """Test error handling for malformed PointCloud2 messages."""
    # Create a malformed PointCloud2 message
    test_jsonl = tmp_path / 'malformed_pointcloud2.jsonl'

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
            'fields': [],  # Empty fields - should cause processing to fail gracefully
            'is_bigendian': False,
            'point_step': 12,
            'row_step': 12,
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],  # 1 point worth of data
            'is_dense': True,
        },
    }

    test_jsonl.write_text(json.dumps(malformed_message))

    # Convert to MCAP
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'malformed_pointcloud2.mcap'
    json_to_mcap(temp_mcap, test_jsonl)

    # Convert back to JSON - should not crash
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify we still get valid output
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1

    result = json.loads(lines[0].strip())
    assert result['topic'] == '/malformed_lidar'
    assert result['datatype'] == 'sensor_msgs/msg/PointCloud2'


def test_invalid_mcap_file(tmp_path: Path):
    """Test handling of invalid MCAP file."""
    # Create a file that's not a valid MCAP
    invalid_mcap = tmp_path / 'invalid.mcap'
    invalid_mcap.write_text('This is not a valid MCAP file')

    output_buffer = StringIO()

    # This should raise an error for invalid MCAP format
    with pytest.raises(RuntimeError, match='Error reading MCAP file'):
        mcap_to_json(invalid_mcap, output_buffer)


def test_empty_mcap_file(tmp_path: Path):
    """Test handling of empty MCAP file."""
    # Create an empty MCAP file
    empty_mcap = tmp_path / 'empty.mcap'
    empty_mcap.write_bytes(b'')

    output_buffer = StringIO()

    # This should raise an error for empty MCAP file
    with pytest.raises(RuntimeError, match='Error reading MCAP file'):
        mcap_to_json(empty_mcap, output_buffer)


def test_mcap_to_json_with_zero_limit(tmp_path: Path):
    """Test mcap_to_json with limit=0."""
    # Use existing test data
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    # Create MCAP from existing JSONL
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert with limit=0 - this actually means "no limit" in the implementation
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=0)

    # Should get all messages since limit=0 means no limit
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) >= 1  # Should have at least one message


def test_mcap_to_json_with_bytearray_limit(tmp_path: Path):
    """Test mcap_to_json with large bytearray data."""
    # Create a message with large data field
    large_data_message = {
        'topic': '/large_data',
        'log_time': 1234567890,
        'publish_time': 1234567890,
        'sequence': 1,
        'datatype': 'std_msgs/msg/UInt8MultiArray',
        'message': {
            'layout': {'dim': [], 'data_offset': 0},
            'data': [i % 256 for i in range(1000)],  # Large data array
        },
    }

    test_jsonl = tmp_path / 'large_data.jsonl'
    test_jsonl.write_text(json.dumps(large_data_message))

    # Convert to MCAP
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'large_data.mcap'
    json_to_mcap(temp_mcap, test_jsonl)

    # Convert back to JSON
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify output
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1

    result = json.loads(lines[0].strip())
    assert result['topic'] == '/large_data'
    assert 'data' in result['message']
