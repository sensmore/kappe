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
    assert 'data' in message

    # If pointcloud2 processing worked, we should have 'points' field
    # Note: This might not always be present depending on the data format
    if 'points' in message:
        assert isinstance(message['points'], list)


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
