import io
from collections.abc import Generator, Iterable, Iterator
from typing import IO

from mcap.data_stream import ReadDataStream
from mcap.exceptions import McapError
from mcap.reader import FOOTER_SIZE
from mcap.records import (
    AttachmentIndex,
    Channel,
    Chunk,
    ChunkIndex,
    Footer,
    McapRecord,
    Message,
    MetadataIndex,
    Schema,
    Statistics,
)
from mcap.stream_reader import MAGIC_SIZE, StreamReader, breakup_chunk
from mcap.summary import Summary


def _read_summary_from_stream_reader(stream_reader: StreamReader) -> Summary | None:
    """read summary records from an MCAP stream reader, collecting them into a Summary."""
    summary = Summary()
    for record in stream_reader.records:
        if isinstance(record, Statistics):
            summary.statistics = record
        elif isinstance(record, Schema):
            summary.schemas[record.id] = record
        elif isinstance(record, Channel):
            summary.channels[record.id] = record
        elif isinstance(record, AttachmentIndex):
            summary.attachment_indexes.append(record)
        elif isinstance(record, ChunkIndex):
            summary.chunk_indexes.append(record)
        elif isinstance(record, MetadataIndex):
            summary.metadata_indexes.append(record)
        elif isinstance(record, Footer):
            # There is no summary!
            if record.summary_start == 0:
                return None
            return summary
    return summary


def _get_summary(stream: IO[bytes]) -> Summary | None:
    """Get the start and end indexes of each chunk in the stream."""
    try:
        stream.seek(-(FOOTER_SIZE + MAGIC_SIZE), io.SEEK_END)
        footer = next(
            StreamReader(
                stream,
                skip_magic=True,
                record_size_limit=None,
            ).records
        )
        if not isinstance(footer, Footer):
            raise McapError(f'expected footer at end of MCAP file, found {type(footer)}')
        if footer.summary_start == 0:
            return None
        stream.seek(footer.summary_start, io.SEEK_SET)
        return _read_summary_from_stream_reader(
            StreamReader(stream, skip_magic=True, record_size_limit=None)
        )
    except:  # noqa: E722, TODO handle better
        return None


def _chunks_matching_topics(
    summary: Summary,
    topics: Iterable[str] | None,
    start_time: float | None,
    end_time: float | None,
) -> list[ChunkIndex]:
    out: list[ChunkIndex] = []
    for chunk_index in summary.chunk_indexes:
        if start_time is not None and chunk_index.message_end_time < start_time:
            continue
        if end_time is not None and chunk_index.message_start_time >= end_time:
            continue
        for channel_id in chunk_index.message_index_offsets:
            if topics is None or summary.channels[channel_id].topic in topics:
                out.append(chunk_index)
                break
    return out


def _read_inner(
    reader: Iterator[McapRecord],
    topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> Generator[tuple[Schema | None, Channel, Message], None, None]:
    _schemas: dict[int, Schema] = {}
    _channels: dict[int, Channel] = {}

    for record in reader:
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


def _read_message_seeking(
    stream: IO[bytes],
    topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> Generator[tuple[Schema | None, Channel, Message], None, None]:
    summary = _get_summary(stream)
    # No summary or chunk indexes exists
    if summary is None or not summary.chunk_indexes:
        # seek to start
        stream.seek(0, io.SEEK_SET)
        yield from _read_message_non_seeking(stream, topics, start_time, end_time)
        return

    chunk_indexes = _chunks_matching_topics(summary, topics, start_time, end_time)

    def reader() -> Generator[McapRecord, None, None]:
        for cidx in chunk_indexes:
            stream.seek(cidx.chunk_start_offset + 1 + 8, io.SEEK_SET)
            chunk = Chunk.read(ReadDataStream(stream))
            yield from breakup_chunk(chunk)

    yield from _read_inner(reader(), topics, start_time, end_time)


def _read_message_non_seeking(
    stream: IO[bytes],
    topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> Generator[tuple[Schema | None, Channel, Message], None, None]:
    # TODO: support seeking stream
    reader = StreamReader(stream)

    yield from _read_inner(reader.records, topics, start_time, end_time)


def read_message(
    stream: IO[bytes],
    topics: Iterable[str] | None = None,
    start_time: float | None = None,
    end_time: float | None = None,
) -> Generator[tuple[Schema | None, Channel, Message], None, None]:
    if stream.seekable():
        yield from _read_message_seeking(stream, topics, start_time, end_time)
    else:
        yield from _read_message_non_seeking(stream, topics, start_time, end_time)
