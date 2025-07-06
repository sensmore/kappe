import json
from pathlib import Path

from mcap_ros2.writer import Writer
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
