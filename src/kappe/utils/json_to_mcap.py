import json
from io import BytesIO
from pathlib import Path
from typing import IO
from zipfile import ZipFile

import platformdirs
from mcap_ros2.writer import Writer
from pydantic import BaseModel

from kappe.utils.msg_def import get_message_definition


class Message(BaseModel):
    topic: str
    log_time: int
    publish_time: int
    sequence: int
    datatype: str
    message: dict


class McapJson(BaseModel):
    messages: list[Message]


def load_json_and_validate(file_path: Path) -> McapJson:
    # TODO support non jsonl files
    if not file_path.exists():
        raise FileNotFoundError(f'File not found: {file_path}')
    if file_path.suffix != '.jsonl':
        raise ValueError('File must be a JSONL file (*.jsonl).')

    try:
        messages = [
            Message(**json.loads(line))
            for line in file_path.read_text().splitlines()
            if line.strip()
        ]
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON in file: {e}') from e
    except Exception as e:
        raise ValueError(f'Error parsing message data: {e}') from e

    if not messages:
        raise ValueError('No valid messages found in file.')

    return McapJson(messages=messages)


URLS = [
    ('rcl_interfaces', 'https://github.com/ros2/rcl_interfaces/archive/refs/heads/humble.zip'),
    (
        'common_interfaces',
        'https://github.com/ros2/common_interfaces/archive/refs/heads/humble.zip',
    ),
    ('geometry2', 'https://github.com/ros2/geometry2/archive/refs/heads/humble.zip'),
]


def _download(url: str, buffer: IO) -> None:
    from urllib.error import URLError
    from urllib.request import urlopen

    if not url.startswith(('http:', 'https:')):
        raise ValueError("URL must start with 'http:' or 'https:'")

    try:
        with urlopen(url) as response:  # noqa: S310
            if response.status != 200:
                msg = f'Failed to download {url}: {response.status} {response.reason}'
                raise ValueError(msg)
            buffer.write(response.read())
    except URLError as e:
        raise ValueError(f'Network error downloading {url}: {e}') from e


def _download_and_extract(url: str, target_dir: Path) -> None:
    if target_dir.exists():
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    buffer = BytesIO()
    _download(url, buffer)
    with ZipFile(buffer, 'r') as zip_ref:
        zip_ref.extractall(target_dir)


def _update_cache(cache_dir: Path) -> None:
    for name, url in URLS:
        _download_and_extract(url, cache_dir / name)


def json_to_mcap(output_file: Path, json_path: Path) -> None:
    mcap_data = load_json_and_validate(json_path)

    cache_dir = platformdirs.user_cache_path(
        appname='kappe_msg_def',
        ensure_exists=True,
        version='1',
    )
    _update_cache(cache_dir)

    with output_file.open('wb') as stream:
        writer = Writer(stream)

        schemas = {}

        for message in mcap_data.messages:
            if message.datatype not in schemas:
                msg_def = get_message_definition(message.datatype, cache_dir)
                if msg_def is None:
                    raise ValueError(f'Message definition for {message.datatype} not found.')
                schema_id = writer.register_msgdef(
                    message.datatype,
                    msg_def,
                )
                schemas[message.datatype] = schema_id

            writer.write_message(
                message=message.message,
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
