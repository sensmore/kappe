import json
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json

# Test constants
TEST_TIMESTAMP = 1234567890
TEST_TIMESTAMP_NS = 1234567890000000000


@pytest.fixture
def sample_bool_message() -> dict[str, Any]:
    """Create a simple Bool message for testing."""
    return {
        'topic': '/test_topic',
        'log_time': TEST_TIMESTAMP,
        'publish_time': TEST_TIMESTAMP,
        'sequence': 0,
        'datatype': 'std_msgs/msg/Bool',
        'message': {'data': True},
    }


@pytest.fixture
def pointcloud2_message_factory():
    """Factory fixture to create PointCloud2 messages with customizable parameters."""

    def _create_pointcloud2_message(
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
            'log_time': TEST_TIMESTAMP,
            'publish_time': TEST_TIMESTAMP,
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
                'is_dense': True,
            },
        }

        if include_points:
            message['message']['points'] = points

        return message

    return _create_pointcloud2_message


@pytest.fixture
def mcap_roundtrip_helper():
    """Helper fixture for JSON→MCAP→JSON roundtrip testing."""

    def _roundtrip_test(message: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
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

    return _roundtrip_test


@pytest.fixture
def create_test_jsonl():
    """Helper fixture to create JSONL files from message data."""

    def _create_jsonl(messages: list[dict[str, Any]], file_path: Path) -> None:
        """Create a JSONL file from a list of messages."""
        with file_path.open('w', encoding='utf-8') as f:
            for message in messages:
                f.write(json.dumps(message))
                f.write('\n')

    return _create_jsonl


@pytest.fixture
def e2e_test_helper():
    """Helper fixture for end-to-end testing with CLI."""

    def _run_e2e_test(
        input_messages: list[dict[str, Any]] | Path,
        config_content: str | Path,
        tmp_path: Path,
        command: str = 'convert',
    ) -> list[dict[str, Any]] | Path:
        """Run an end-to-end test with the given input and config.

        Args:
            input_messages: List of messages or path to input JSONL file
            config_content: Config content string or path to config file
            tmp_path: Temporary directory path
            command: Command to run ('convert' or 'cut')

        Returns:
            List of output messages for 'convert' command
        """
        # Handle input - can be messages or file path
        if isinstance(input_messages, Path):
            input_jsonl = input_messages
        else:
            input_jsonl = tmp_path / 'input.jsonl'
            with input_jsonl.open('w', encoding='utf-8') as f:
                for message in input_messages:
                    f.write(json.dumps(message))
                    f.write('\n')

        # Handle config - can be content string or file path
        if isinstance(config_content, Path):
            config_file = config_content
        else:
            config_file = tmp_path / 'config.yaml'
            config_file.write_text(config_content)

        # Convert to MCAP
        input_mcap = tmp_path / 'input.mcap'
        json_to_mcap(input_mcap, input_jsonl)

        # Run kappe command
        output_dir = tmp_path / 'output'
        output_dir.mkdir()

        if command == 'cut':
            argv = [
                'kappe',
                '--progress=false',
                'cut',
                '--config',
                str(config_file),
                '--output',
                str(output_dir),
                '--overwrite=true',
                str(input_mcap),
            ]
        else:
            argv = [
                'kappe',
                '--progress=false',
                command,
                '--config',
                str(config_file),
                str(input_mcap),
                str(output_dir),
            ]

        with patch('sys.argv', argv):
            kappe_main()

        # For cut command, return the output directory (caller handles verification)
        if command == 'cut':
            return output_dir

        # For convert command, read and return the output messages
        output_mcap = output_dir / 'input.mcap'
        output_buffer = StringIO()
        mcap_to_json(output_mcap, output_buffer)

        # Parse results
        output_buffer.seek(0)
        return [json.loads(line.strip()) for line in output_buffer.readlines()]

    return _run_e2e_test


def create_test_data_message(
    datatype: str = 'std_msgs/msg/Bool',
    topic: str = '/test_topic',
    message_data: dict[str, Any] | None = None,
    sequence: int = 0,
) -> dict[str, Any]:
    """Create a test message with the given parameters."""
    if message_data is None:
        message_data = {'data': True}

    return {
        'topic': topic,
        'log_time': TEST_TIMESTAMP,
        'publish_time': TEST_TIMESTAMP,
        'sequence': sequence,
        'datatype': datatype,
        'message': message_data,
    }
