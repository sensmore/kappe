from collections.abc import Generator, Iterable
from typing import IO

from mcap.exceptions import McapError
from mcap.records import Channel, Message, Schema
from mcap.stream_reader import StreamReader


def read_message(
    stream: IO[bytes],
    topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> Generator[tuple[Schema | None, Channel, Message], None, None]:
    # TODO: support seeking stream
    reader = StreamReader(stream)

    _schemas: dict[int, Schema] = {}
    _channels: dict[int, Channel] = {}

    for record in reader.records:
        if isinstance(record, Schema):
            _schemas[record.id] = record
        if isinstance(record, Channel):
            if record.schema_id != 0 and record.schema_id not in _schemas:
                raise McapError(f'no schema record found with id {record.schema_id}')
            _channels[record.id] = record
        if isinstance(record, Message):
            if record.channel_id not in _channels:
                raise McapError(f'no channel record found with id {record.channel_id}')
            channel = _channels[record.channel_id]
            if topics is not None and channel.topic not in topics:
                continue
            if start_time is not None and record.log_time < start_time:
                continue
            if end_time is not None and record.log_time >= end_time:
                continue
            schema = None if channel.schema_id == 0 else _schemas[channel.schema_id]
            yield (schema, channel, record)
