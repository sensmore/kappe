import json
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from conftest import create_test_data_message

if TYPE_CHECKING:
    from collections.abc import Callable

from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json


def test_file_not_found():
    """Test that FileNotFoundError is raised for non-existent files."""
    non_existent_file = Path('/non/existent/file.mcap')
    output_buffer = StringIO()

    with pytest.raises(FileNotFoundError, match='MCAP file not found'):
        mcap_to_json(non_existent_file, output_buffer)


def test_basic_conversion_with_valid_mcap(
    tmp_path: Path, sample_bool_message: dict, mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test basic MCAP to JSONL conversion."""
    # Use mcap_roundtrip_helper for consistent testing
    result = mcap_roundtrip_helper(sample_bool_message, tmp_path)

    # Verify output format
    assert 'topic' in result
    assert 'log_time' in result
    assert 'publish_time' in result
    assert 'sequence' in result
    assert 'datatype' in result
    assert 'message' in result


def test_topic_filtering(tmp_path: Path, create_test_jsonl: 'Callable') -> None:
    """Test topic filtering functionality."""
    # Create test data with multiple topics
    messages = [
        create_test_data_message(topic='/test_topic', sequence=0),
        create_test_data_message(topic='/other_topic', sequence=1),
        create_test_data_message(topic='/test_topic', sequence=2),
    ]

    input_jsonl = tmp_path / 'multi_topic.jsonl'
    create_test_jsonl(messages, input_jsonl)

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Filter for specific topic
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=['/test_topic'])

    output_buffer.seek(0)
    lines = output_buffer.readlines()

    # Should have 2 messages from /test_topic
    assert len(lines) == 2
    for line in lines:
        message = json.loads(line.strip())
        assert message['topic'] == '/test_topic'


def test_message_limit(tmp_path: Path, create_test_jsonl: 'Callable') -> None:
    """Test message limit functionality."""
    # Create test data with multiple messages
    messages = [
        create_test_data_message(sequence=0),
        create_test_data_message(sequence=1),
        create_test_data_message(sequence=2),
    ]

    input_jsonl = tmp_path / 'multi_message.jsonl'
    create_test_jsonl(messages, input_jsonl)

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Limit to 1 message
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=1)

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1


def test_empty_topics_list(tmp_path: Path, sample_bool_message: dict) -> None:
    """Test behavior with empty topics list."""
    input_jsonl = tmp_path / 'test.jsonl'
    input_jsonl.write_text(json.dumps(sample_bool_message))

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Empty topics list should return no messages (filters out all)
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=[])

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 0


def test_pointcloud2_conversion(
    tmp_path: Path, pointcloud2_message_factory: 'Callable', mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test PointCloud2 message conversion with decoded point data."""
    # Create a PointCloud2 message
    pointcloud2_message = pointcloud2_message_factory(
        topic='/lidar_points',
        width=3,
        include_points=False,  # Start with raw data only
    )

    # Use roundtrip helper
    result = mcap_roundtrip_helper(pointcloud2_message, tmp_path)

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


def test_pointcloud2_error_handling(
    tmp_path: Path, pointcloud2_message_factory: 'Callable', mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test error handling for malformed PointCloud2 messages."""
    # Create a malformed PointCloud2 message
    malformed_message = pointcloud2_message_factory(
        topic='/malformed_lidar', width=1, frame_id='lidar', include_points=False
    )
    # Override with empty fields - should cause processing to fail gracefully
    malformed_message['message']['fields'] = []
    malformed_message['message']['data'] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    # Use roundtrip helper - should not crash
    result = mcap_roundtrip_helper(malformed_message, tmp_path)

    # Verify we still get valid output
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


def test_mcap_to_json_with_zero_limit(tmp_path: Path, create_test_jsonl: 'Callable') -> None:
    """Test mcap_to_json with limit=0."""
    # Create test data with multiple messages
    messages = [
        create_test_data_message(sequence=0),
        create_test_data_message(sequence=1),
        create_test_data_message(sequence=2),
    ]

    input_jsonl = tmp_path / 'multi_message.jsonl'
    create_test_jsonl(messages, input_jsonl)

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert with limit=0 - this actually means "no limit" in the implementation
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=0)

    # Should get all messages since limit=0 means no limit
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 3  # Should have all 3 messages


def test_mcap_to_json_with_bytearray_limit(
    tmp_path: Path, mcap_roundtrip_helper: 'Callable'
) -> None:
    """Test mcap_to_json with large bytearray data."""
    # Create a message with large data field
    large_data_message = create_test_data_message(
        datatype='std_msgs/msg/UInt8MultiArray',
        topic='/large_data',
        message_data={
            'layout': {'dim': [], 'data_offset': 0},
            'data': [i % 256 for i in range(1000)],  # Large data array
        },
        sequence=1,
    )

    # Use roundtrip helper
    result = mcap_roundtrip_helper(large_data_message, tmp_path)

    # Verify output
    assert result['topic'] == '/large_data'
    assert 'data' in result['message']
    assert len(result['message']['data']) == 1000
