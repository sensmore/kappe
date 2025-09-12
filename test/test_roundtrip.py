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

    # Use roundtrip helper
    output_data = mcap_roundtrip_helper(input_json_content, tmp_path)

    assert input_json_content == output_data


def test_round_trip_conversion(tmp_path: Path, sample_bool_message: dict) -> None:
    """Test round-trip conversion: JSONL -> MCAP -> JSONL."""
    result = mcap_roundtrip_helper(sample_bool_message, tmp_path)

    assert result == sample_bool_message


def test_large_data_array_roundtrip(tmp_path: Path) -> None:
    """Test roundtrip conversion with large data arrays."""
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
