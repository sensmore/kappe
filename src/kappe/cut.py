from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mcap.reader import make_reader
from mcap.records import Channel, Schema
from mcap.writer import Writer
from pydantic import BaseModel, Extra, validator
from tqdm import tqdm


class CutSplits(BaseModel, extra=Extra.forbid):
    # TODO: validate that start < end
    # TODO: validate that start is >= file start
    # TODO: validate that end is <= file end
    start: float
    end: float
    name: str

    @validator('end')
    def validate_end(cls, v: float, values: dict[str, Any], **_kwargs: Any) -> float:
        if v < values['start']:
            raise ValueError('end must be greater than start')
        return v


class CutSettings(BaseModel, extra=Extra.forbid):
    keep_tf_tree: bool = False
    splits: list[CutSplits]


@dataclass
class SplitWriter:
    writer: Writer
    schema_lookup: dict[int, int] = field(default_factory=dict)
    channel_lookup: dict[int, int] = field(default_factory=dict)


def cutter(input_file: Path, output: Path, settings: CutSettings) -> None:
    """Cut a file into multiple files."""
    output.mkdir(parents=True, exist_ok=True)

    outputs: list[SplitWriter] = []

    def get_channel_id(i: int, schema: Schema, channel: Channel) -> int:
        w = outputs[i].writer
        schema_lookup = outputs[i].schema_lookup
        channel_lookup = outputs[i].channel_lookup
        channel_id = channel_lookup.get(channel.id, None)
        if channel_id is None:
            schema_id = schema_lookup.get(channel.schema_id, None)
            if schema_id is None:
                schema_id = w.register_schema(
                    schema.name,
                    schema.encoding,
                    schema.data,
                )
                schema_lookup[channel.schema_id] = schema_id
            channel_id = w.register_channel(
                channel.topic,
                channel.message_encoding,
                schema_id,
                channel.metadata,
            )
            channel_lookup[channel.id] = channel_id

        return channel_id

    min_start_time = int(min([split.start for split in settings.splits]) * 1e9)
    max_end_time = int(max([split.end for split in settings.splits]) * 1e9)

    with input_file.open('rb') as f:
        reader = make_reader(f)

        for split in settings.splits:
            out = output / split.name
            w = Writer(str(out))
            w.start()
            outputs.append(SplitWriter(
                writer=w,
            ))

        tf_static_msgs = []
        tf_static_schema: Schema | None = None
        tf_static_channel: Channel | None = None
        if settings.keep_tf_tree:
            for schema, channel, message in reader.iter_messages(
                topics=['/tf_static'],
            ):
                tf_static_msgs.append(message.data)
                tf_static_schema = schema
                tf_static_channel = channel

        fist_message = True
        for schema, channel, message in tqdm(reader.iter_messages(
            start_time=min_start_time,
            end_time=max_end_time,
        )):
            if schema is None:
                continue

            if fist_message and settings.keep_tf_tree:
                # TODO: validate tf_static_schema is not None
                for i in range(len(settings.splits)):
                    w = outputs[i].writer
                    channel_id = get_channel_id(i, tf_static_schema, tf_static_channel)
                    for data in tf_static_msgs:
                        w.add_message(
                            channel_id,
                            message.log_time,
                            data,
                            message.publish_time,
                            message.sequence,
                        )

                fist_message = False

            if settings.keep_tf_tree and channel.topic == '/tf_static':
                continue

            pub_time_sec = message.publish_time / 1e9
            for i, split in enumerate(settings.splits):
                if split.start <= pub_time_sec <= split.end:
                    channel_id = get_channel_id(i, schema, channel)
                    w = outputs[i].writer
                    w.add_message(
                        channel_id,
                        message.log_time,
                        message.data,
                        message.publish_time,
                        message.sequence,
                    )

    for split in outputs:
        split.writer.finish()
