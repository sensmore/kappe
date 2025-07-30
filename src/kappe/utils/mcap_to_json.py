import json
import sys
from collections.abc import Generator
from pathlib import Path
from typing import IO, Any

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory
from pointcloud2 import read_points


def _to_dict(obj: Any, *, limit_bytearray: bool = False) -> dict | None:
    if not hasattr(obj, '__slots__'):
        return obj
    ret = {}
    for slot in obj.__slots__:
        value = getattr(obj, slot)
        if isinstance(value, (list, tuple)):
            ret[slot] = [
                _to_dict(v, limit_bytearray=limit_bytearray) if v is not None else None
                for v in value
            ]
        elif isinstance(value, (bytearray, bytes)):
            # convert to int list
            if limit_bytearray:
                ret[slot] = f'bytes({len(value)})'
            else:
                ret[slot] = [int(b) for b in value]
        elif hasattr(value, '__slots__'):
            ret[slot] = _to_dict(value, limit_bytearray=limit_bytearray)
        else:
            ret[slot] = value
    return ret


def _convert_pointcloud2_to_json(obj: Any) -> dict:
    """Convert PointCloud2 message to JSON with decoded point data."""
    # Get the basic message structure
    result = _to_dict(obj)
    if result is None:
        return {}

    # If this is a PointCloud2 message, decode the point data
    if (
        hasattr(obj, 'fields')
        and hasattr(obj, 'data')
        and hasattr(obj, 'width')
        and hasattr(obj, 'height')
    ):
        try:
            json_points = [
                {name: point[name].item() for name in point.dtype.names}
                for point in read_points(obj)
            ]

            result['points'] = json_points
            del result['data']  # Remove raw data field if present
        except (ValueError, TypeError, AttributeError):
            # If point extraction fails, fall back to raw data
            pass
    return result


def _iter_jsonl(
    file_path: Path, topics: list[str] | None = None, limit: int = 0
) -> Generator[dict, None, None]:
    if not file_path.exists():
        raise FileNotFoundError(f'MCAP file not found: {file_path}')

    try:
        with file_path.open('rb') as f:
            reader = make_reader(f, decoder_factories=[Ros2DecoderFactory()])
            for i, record in enumerate(
                reader.iter_decoded_messages(
                    topics=topics,
                    log_time_order=False,  # Keep original order
                )
            ):
                if limit > 0 and i >= limit:
                    break

                # Check if this is a PointCloud2 message
                schema_name = record.schema.name if record.schema else None
                is_pointcloud2 = schema_name in [
                    'sensor_msgs/msg/PointCloud2',
                    'sensor_msgs/PointCloud2',
                ]

                # Use specialized conversion for PointCloud2 messages
                if is_pointcloud2:
                    message_data = _convert_pointcloud2_to_json(record.decoded_message)
                else:
                    message_data = _to_dict(record.decoded_message)

                yield {
                    'topic': record.channel.topic,
                    'log_time': record.message.log_time,
                    'publish_time': record.message.publish_time,
                    'sequence': record.message.sequence,
                    'datatype': schema_name,
                    'message': message_data,
                }
    except Exception as e:
        raise RuntimeError(f'Error reading MCAP file: {e}') from e


def mcap_to_json(
    file_path: Path, out_buffer: IO[str], topics: list[str] | None = None, limit: int = 0
) -> None:
    for message in _iter_jsonl(file_path, topics, limit):
        out_buffer.write(json.dumps(message))
        out_buffer.write('\n')


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description='Convert MCAP file to JSON format.')
    parser.add_argument('file', type=Path, help='Path to the MCAP file.')
    parser.add_argument(
        '-o', '--output', type=Path, default='-', help='Path to the output JSONL file.'
    )

    parser.add_argument('-t', '--topics', nargs='*', help='List of topics to filter messages by.')
    parser.add_argument(
        '-l',
        '--limit',
        type=int,
        default=0,
        help='Limit the number of messages to print (0 for no limit).',
    )

    args = parser.parse_args()

    io = None
    try:
        io = sys.stdout if args.output == '-' else Path(args.output).open('w')  # noqa: SIM115
        mcap_to_json(
            file_path=args.file,
            out_buffer=io,
            topics=args.topics,
            limit=args.limit,
        )
    finally:
        if io and io is not sys.stdout:
            io.close()


if __name__ == '__main__':
    main()
