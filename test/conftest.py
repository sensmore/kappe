import json
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json

# Test constants
TEST_TIMESTAMP = 1234567890
TEST_TIMESTAMP_NS = TEST_TIMESTAMP * 1_000_000_000


@pytest.fixture
def sample_bool_message() -> dict[str, Any]:
    """Create a simple Bool message for testing."""
    return {
        'topic': '/test_topic',
        'log_time': TEST_TIMESTAMP_NS,
        'publish_time': TEST_TIMESTAMP_NS,
        'sequence': 0,
        'datatype': 'std_msgs/msg/Bool',
        'message': {'data': True},
    }


def pointcloud2_message_factory(
    topic: str = '/test_lidar',
    width: int = 2,
    points: list[dict[str, float]] | None = None,
    *,
    include_points: bool = True,
    frame_id: str = 'lidar_frame',
) -> dict[str, Any]:
    """Create a PointCloud2 message with optional point data."""
    if points is None:
        points = [{'x': 1.0, 'y': 2.0, 'z': 3.0}, {'x': 4.0, 'y': 5.0, 'z': 6.0}]

    message = {
        'topic': topic,
        'log_time': TEST_TIMESTAMP_NS,
        'publish_time': TEST_TIMESTAMP_NS,
        'sequence': 0,
        'datatype': 'sensor_msgs/msg/PointCloud2',
        'message': {
            'header': {
                'stamp': {'sec': TEST_TIMESTAMP, 'nanosec': 0},
                'frame_id': frame_id,
            },
            'height': 1,
            'width': width,
            'fields': [
                {'name': 'x', 'offset': 0, 'datatype': 7, 'count': 1},
                {'name': 'y', 'offset': 4, 'datatype': 7, 'count': 1},
                {'name': 'z', 'offset': 8, 'datatype': 7, 'count': 1},
            ],
            'is_bigendian': False,
            'point_step': 12,
            'row_step': width * 12,
            'data': [0] * (width * 12),
            'is_dense': False,
        },
    }

    if include_points:
        message['message']['points'] = points

    return message


def mcap_roundtrip_helper(message: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    """Perform a roundtrip conversion and return the result."""
    # Create input JSONL file
    input_jsonl = tmp_path / 'input.jsonl'
    input_jsonl.write_text(json.dumps(message))

    # Convert to MCAP
    temp_mcap = tmp_path / 'temp.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert back to JSON
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Parse the result
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 1, 'Expected exactly one message in output'

    return json.loads(lines[0].strip())


def create_test_jsonl(messages: list[dict[str, Any]], file_path: Path) -> None:
    """Create a JSONL file from a list of messages."""
    with file_path.open('w', encoding='utf-8') as f:
        for message in messages:
            f.write(json.dumps(message))
            f.write('\n')


def create_test_data_message(
    datatype: str = 'std_msgs/msg/Bool',
    topic: str = '/test_topic',
    message_data: dict[str, Any] | None = None,
    sequence: int = 0,
) -> dict[str, Any]:
    """Create a test message with the given parameters."""
    if message_data is None:
        assert datatype == 'std_msgs/msg/Bool', 'Default message data only applies to Bool messages'
        message_data = {'data': True}

    return {
        'topic': topic,
        'log_time': TEST_TIMESTAMP_NS,
        'publish_time': TEST_TIMESTAMP_NS,
        'sequence': sequence,
        'datatype': datatype,
        'message': message_data,
    }
