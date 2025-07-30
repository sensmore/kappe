from pathlib import Path

from .conftest import (
    create_test_data_message,
    mcap_roundtrip_helper,
    pointcloud2_message_factory,
)


def test_pointcloud2_roundtrip_data_integrity(tmp_path: Path) -> None:
    """
    Tests that PointCloud2 data is correctly preserved during a full
    JSON -> MCAP -> JSON roundtrip conversion.
    """
    # Create an input PointCloud2 message with specific point data
    input_json_content = pointcloud2_message_factory(
        topic='/points',
        width=2,
        points=[{'x': 1.0, 'y': 2.0, 'z': 3.0}, {'x': 4.0, 'y': 5.0, 'z': 6.0}],
        frame_id='map',
    )
    # Override timestamp for this specific test
    input_json_content['log_time'] = 1672531200000000000
    input_json_content['publish_time'] = 1672531200000000000
    input_json_content['sequence'] = 1
    input_json_content['message']['header']['stamp'] = {'sec': 1672531200, 'nanosec': 0}

    # Use roundtrip helper
    output_data = mcap_roundtrip_helper(input_json_content, tmp_path)

    # The core of the test: ensure the point data is identical
    assert input_json_content['message']['points'] == output_data['message']['points'], (
        'PointCloud2 points should be preserved after roundtrip'
    )

    # Also check other fields to be safe
    assert input_json_content['topic'] == output_data['topic']
    assert input_json_content['datatype'] == output_data['datatype']
    assert input_json_content['message']['header'] == output_data['message']['header']


def test_basic_conversion_with_valid_mcap(tmp_path: Path, sample_bool_message: dict) -> None:
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


def test_round_trip_conversion(tmp_path: Path, sample_bool_message: dict) -> None:
    """Test round-trip conversion: JSONL -> MCAP -> JSONL."""
    # Use mcap_roundtrip_helper for consistent testing
    result = mcap_roundtrip_helper(sample_bool_message, tmp_path)

    assert result == sample_bool_message


def test_pointcloud2_conversion(tmp_path: Path) -> None:
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


def test_pointcloud2_error_handling(tmp_path: Path) -> None:
    """Test error handling for malformed PointCloud2 messages."""
    # Create a malformed PointCloud2 message
    malformed_message = pointcloud2_message_factory(
        topic='/malformed_lidar', width=1, frame_id='lidar', include_points=False
    )
    # Override with empty fields - should cause processing to fail gracefully
    malformed_message['message']['fields'] = []
    malformed_message['message']['data'] = list(range(12))

    # Use roundtrip helper - should not crash
    result = mcap_roundtrip_helper(malformed_message, tmp_path)

    # Verify we still get valid output
    assert result['topic'] == '/malformed_lidar'
    assert result['datatype'] == 'sensor_msgs/msg/PointCloud2'


def test_mcap_to_json_with_bytearray_limit(tmp_path: Path) -> None:
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
