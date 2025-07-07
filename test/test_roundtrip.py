from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


def test_pointcloud2_roundtrip_data_integrity(
    tmp_path: Path, pointcloud2_message_factory: 'Callable', mcap_roundtrip_helper: 'Callable'
) -> None:
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
