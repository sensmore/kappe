import io
import json
import logging
from pathlib import Path

import numpy as np
from mcap.writer import IndexType
from pointcloud2 import create_cloud, dtype_from_fields
from pydantic import BaseModel

from kappe.settings import ROS2Distro
from kappe.utils.msg_def import get_message_definition
from kappe.writer import WrappedWriter

logger = logging.getLogger(__name__)


class _Message(BaseModel):
    topic: str
    log_time: int
    publish_time: int
    sequence: int
    datatype: str
    message: dict


class _McapJson(BaseModel):
    messages: list[_Message]


def _load_json_and_validate(file_path: Path) -> _McapJson:
    # TODO support non jsonl files
    if not file_path.exists():
        raise FileNotFoundError(f'File not found: {file_path}')
    if file_path.suffix != '.jsonl':
        raise ValueError('File must be a JSONL file (*.jsonl).')

    try:
        messages = [
            _Message(**json.loads(line))
            for line in file_path.read_text().splitlines()
            if line.strip()
        ]
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON in file: {e}') from e
    except Exception as e:
        raise ValueError(f'Error parsing message data: {e}') from e

    if not messages:
        raise ValueError('No valid messages found in file.')

    return _McapJson(messages=messages)


def _convert_json_to_pointcloud2(message_data: dict) -> dict:
    """Convert JSON message data back to PointCloud2 format if needed."""
    # Check if this has the structure of a PointCloud2 message with decoded points

    # Only process if we have decoded points
    if 'points' not in message_data:
        return message_data
    points = message_data['points']
    fields = message_data['fields']
    header = message_data['header']

    # Convert points list to numpy array
    if points:
        try:
            dtypes = dtype_from_fields(fields)
            # Convert list of dictionaries to list of tuples in field order
            field_names = [field['name'] for field in fields]
            points_tuples = [tuple(point[name] for name in field_names) for point in points]
            points_array = np.array(points_tuples, dtype=dtypes)

            # Create PointCloud2 message using pointcloud2 library
            pointcloud_msg = create_cloud(header=header, fields=fields, points=points_array)

            # Convert the created message to dict format
            return {
                'header': header,
                'height': pointcloud_msg.height,
                'width': pointcloud_msg.width,
                'fields': fields,
                'is_bigendian': pointcloud_msg.is_bigendian,
                'point_step': pointcloud_msg.point_step,
                'row_step': pointcloud_msg.row_step,
                'data': list(pointcloud_msg.data),
                'is_dense': pointcloud_msg.is_dense,
            }
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f'Error converting PointCloud2 message data: {e}') from e

    return message_data


def json_to_mcap(
    output_file: Path,
    json_path: Path,
    *,
    skip_index: bool = False,
    skip_footer: bool = False,
) -> None:
    """Convert JSONL to MCAP with optional malformation for testing.

    Args:
        output_file: Path to write MCAP file
        json_path: Path to input JSONL file
        skip_index: If True, creates unindexed MCAP (no summary/index sections)
        skip_footer: If True, creates MCAP without footer (for testing)
    """
    mcap_data = _load_json_and_validate(json_path)

    with (
        output_file.open('wb') as stream,
        WrappedWriter(
            stream, index_types=IndexType.ALL if not skip_index else IndexType.NONE
        ) as writer,
    ):
        schemas = {}

        for message in mcap_data.messages:
            if message.datatype not in schemas:
                msg_def = get_message_definition(message.datatype, ROS2Distro.HUMBLE)
                if msg_def is None:
                    raise ValueError(f'Message definition for {message.datatype} not found.')
                schema_id = writer.register_msgdef(
                    message.datatype,
                    msg_def,
                )
                schemas[message.datatype] = schema_id

            # Check if this is a PointCloud2 message and convert if needed
            message_data = message.message
            if message.datatype in {
                'sensor_msgs/msg/PointCloud2',
                'sensor_msgs/PointCloud2',
            }:
                message_data = _convert_json_to_pointcloud2(message_data)

            writer.write_message(
                message=message_data,
                topic=message.topic,
                schema=schemas[message.datatype],
                log_time=message.log_time,
                publish_time=message.publish_time,
                sequence=message.sequence,
            )

    if skip_footer:
        # destroy the footer
        with output_file.open('rb+') as f:
            f.seek(-(1), io.SEEK_END)
            f.truncate()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description='Convert JSON to MCAP.')
    parser.add_argument('file', type=Path, help='Path to the input JSONL file.')
    parser.add_argument('-o', '--output', type=Path, default=None, help='Path to the MCAP file.')
    parser.add_argument(
        '--skip-index', action='store_true', help='Create unindexed MCAP (for testing)'
    )
    parser.add_argument(
        '--skip-footer', action='store_true', help='Create MCAP without footer (for testing)'
    )

    args = parser.parse_args()
    json_file: Path = args.file
    output_file: Path = args.output
    if output_file is None:
        output_file = json_file.with_suffix('.mcap')

    json_to_mcap(
        output_file,
        json_file,
        skip_index=args.skip_index,
        skip_footer=args.skip_footer,
    )


if __name__ == '__main__':
    main()
