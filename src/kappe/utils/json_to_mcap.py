import json
from pathlib import Path

import numpy as np
from mcap_ros2.writer import Writer
from pointcloud2 import create_cloud, dtype_from_fields
from pydantic import BaseModel

from kappe.settings import ROS2Distro
from kappe.utils.msg_def import get_message_definition


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

    # If we have decoded points, reconstruct the PointCloud2 message
    points = message_data['points']
    fields = message_data['fields']
    header = message_data['header']

    # Convert points list to numpy array
    if points:
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

    return message_data


def json_to_mcap(output_file: Path, json_path: Path) -> None:
    mcap_data = _load_json_and_validate(json_path)

    with output_file.open('wb') as stream:
        writer = Writer(stream)

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
            is_pointcloud2 = message.datatype in [
                'sensor_msgs/msg/PointCloud2',
                'sensor_msgs/PointCloud2',
            ]

            if is_pointcloud2:
                message_data = _convert_json_to_pointcloud2(message_data)

            writer.write_message(
                message=message_data,
                topic=message.topic,
                schema=schemas[message.datatype],
                log_time=message.log_time,
                publish_time=message.publish_time,
                sequence=message.sequence,
            )

        writer.finish()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description='Convert JSON to MCAP.')
    parser.add_argument('file', type=Path, help='Path to the input JSONL file.')
    parser.add_argument('-o', '--output', type=Path, default=None, help='Path to the MCAP file.')

    args = parser.parse_args()
    json_file: Path = args.file
    output_file: Path = args.output
    if output_file is None:
        output_file = json_file.with_suffix('.mcap')

    json_to_mcap(output_file, json_file)


if __name__ == '__main__':
    main()
