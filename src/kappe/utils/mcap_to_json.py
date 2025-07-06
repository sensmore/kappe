import json
import sys
from collections.abc import Generator
from pathlib import Path
from typing import IO, Any

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory


def _to_dict(obj: Any, *, limit_bytearray: bool = False) -> dict | None:
    if not hasattr(obj, '__slots__'):
        return None
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


def _iter_jsonl(
    file_path: Path, topics: list[str] | None = None, limit: int = 0
) -> Generator[dict, None, None]:
    if not file_path.exists():
        raise FileNotFoundError(f'MCAP file not found: {file_path}')

    if topics:
        for topic in topics:
            if not topic.startswith('/'):
                raise ValueError(f'Topic must start with "/": {topic}')

    try:
        with file_path.open('rb') as f:
            reader = make_reader(f, decoder_factories=[Ros2DecoderFactory()])
            for i, record in enumerate(reader.iter_decoded_messages(topics=topics)):
                if limit > 0 and i >= limit:
                    break
                yield {
                    'topic': record.channel.topic,
                    'log_time': record.message.log_time,
                    'publish_time': record.message.publish_time,
                    'sequence': record.message.sequence,
                    'datatype': record.schema.name if record.schema else None,
                    'message': _to_dict(record.decoded_message),
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
    parser.add_argument('-t', '--topics', nargs='*', help='List of topics to filter messages by.')
    parser.add_argument(
        '-l',
        '--limit',
        type=int,
        default=0,
        help='Limit the number of messages to print (0 for no limit).',
    )

    args = parser.parse_args()

    mcap_to_json(args.file, sys.stdout, args.topics, args.limit)


if __name__ == '__main__':
    main()
