import logging
from pathlib import Path

from mcap.reader import McapReader, make_reader
from mcap.records import Channel, Message, Schema
from mcap.writer import Writer
from pydantic import BaseModel, Extra, Field, validator
from tqdm import tqdm

logger = logging.getLogger(__name__)


class CutSplits(BaseModel, extra=Extra.forbid):
    start: float
    end: float
    name: str

    @validator('end')
    def validate_end(cls, value, values, **kwargs):  # noqa: ANN001, ANN003, ANN201, ARG003
        if value < values['start']:
            raise ValueError('end must be greater than start')
        return value


class CutSplitOn(BaseModel, extra=Extra.forbid):
    topic: str
    debounce: float = Field(
        description='Number of seconds to wait before splitting on the same topic',
        default=0.0,
    )


class CutSettings(BaseModel, extra=Extra.forbid):
    keep_tf_tree: bool = False
    splits: list[CutSplits] | None = None
    split_on_topic: CutSplitOn | None = None


class SplitWriter:
    def __init__(self, path: str, profile: str) -> None:
        self._schema_lookup: dict[int, int] = {}
        self._channel_lookup: dict[int, int] = {}

        self.static_tf_set = False
        self.static_tf_channel_id = None
        self.static_tf: list[bytes] | None = None

        self._writer = Writer(path)
        self._writer.start(profile=profile)

    def set_static_tf(self, schema: Schema, channel: Channel, data: list[bytes]):
        self.static_tf_set = True
        self.static_tf_channel_id = self.register_channel(schema, channel)
        self.static_tf = data

    def register_schema(self, schema: Schema) -> int:
        schema_id = self._schema_lookup.get(schema.id, None)
        if schema_id is None:
            schema_id = self._writer.register_schema(
                schema.name,
                schema.encoding,
                schema.data,
            )
            self._schema_lookup[schema.id] = schema_id

        return schema_id

    def register_channel(self, schema: Schema, channel: Channel) -> int:
        channel_id = self._channel_lookup.get(channel.id, None)
        if channel_id is None:
            schema_id = self.register_schema(schema)
            channel_id = self._writer.register_channel(
                channel.topic,
                channel.message_encoding,
                schema_id,
                channel.metadata,
            )
            self._channel_lookup[channel.id] = channel_id

        return channel_id

    def write_message(self, schema: Schema, channel: Channel, message: Message) -> None:
        if self.static_tf is not None and self.static_tf_channel_id is not None:
            for data in self.static_tf:
                self._writer.add_message(
                    self.static_tf_channel_id,
                    message.log_time,
                    data,
                    message.publish_time,
                    message.sequence,
                )
            self.static_tf = None
            self.static_tf_channel_id = None

        if self.static_tf_set and channel.topic == '/tf_static':
            return

        channel_id = self.register_channel(schema, channel)

        self._writer.add_message(
            channel_id,
            message.log_time,
            message.data,
            message.publish_time,
            message.sequence,
        )

    def finish(self) -> None:
        self._writer.finish()


def collect_tf(reader: McapReader) -> tuple[Schema, Channel, list[bytes]]:
    logger.info('Collecting static tf data')

    tf_static_msgs = []
    tf_static_schema: Schema | None = None
    tf_static_channel: Channel | None = None
    for schema, channel, message in reader.iter_messages(
        topics=['/tf_static'],
    ):
        tf_static_msgs.append(message.data)
        tf_static_schema = schema
        tf_static_channel = channel

    if tf_static_schema is None or tf_static_channel is None:
        raise ValueError('Could not find /tf_static topic in file')

    return tf_static_schema, tf_static_channel, tf_static_msgs


def cutter_split(input_file: Path, output: Path, settings: CutSettings) -> None:
    """Cut a file into multiple files."""
    if settings.splits is None:
        raise ValueError('splits must be set')

    # validate duplicate names
    names = [split.name for split in settings.splits]
    if len(names) != len(set(names)):
        raise ValueError('Duplicate split names')

    # append .mcap to outputs without it
    for split in settings.splits:
        if not split.name.endswith('.mcap'):
            split.name += '.mcap'

    output.mkdir(parents=True, exist_ok=True)

    outputs: list[SplitWriter] = []

    min_start_time = int(min([split.start for split in settings.splits]) * 1e9)
    max_end_time = int(max([split.end for split in settings.splits]) * 1e9)

    with input_file.open('rb') as f:
        reader = make_reader(f)

        profile = reader.get_header().profile
        for split in settings.splits:
            out = output / split.name
            outputs.append(SplitWriter(str(out), profile))

        if settings.keep_tf_tree:
            tf_static_schema, tf_static_channel, tf_static_msgs = collect_tf(reader)
            for w in outputs:
                w.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

        for schema, channel, message in tqdm(reader.iter_messages(
            start_time=min_start_time,
            end_time=max_end_time,
        )):
            if schema is None:
                continue
            pub_time_sec = message.publish_time / 1e9
            for i, split in enumerate(settings.splits):
                if split.start <= pub_time_sec <= split.end:
                    outputs[i].write_message(schema, channel, message)

    for split in outputs:
        split.finish()


def cutter_split_on(input_file: Path, output: Path, settings: CutSettings) -> None:
    """Cut a file into multiple files."""
    if settings.split_on_topic is None:
        raise ValueError('split_on must be set')

    output.mkdir(parents=True, exist_ok=True)

    with input_file.open('rb') as f:
        reader = make_reader(f)

        profile = reader.get_header().profile
        # last split time in nanoseconds
        last_split_time = 0
        debounce_ns = int(settings.split_on_topic.debounce * 1e9)

        counter = 0
        writer = SplitWriter(f'{output}/{counter:05}.mcap', profile=profile)

        tf_static_schema, tf_static_channel, tf_static_msgs = None, None, None
        if settings.keep_tf_tree:
            tf_static_schema, tf_static_channel, tf_static_msgs = collect_tf(reader)
            writer.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

        # TODO: check if topic exists
        for schema, channel, message in tqdm(reader.iter_messages()):
            if schema is None:
                continue

            if channel.topic == settings.split_on_topic.topic and \
                    message.publish_time - last_split_time > debounce_ns:
                logger.info('Found split point at %.2fs', message.publish_time / 1e9)
                last_split_time = message.publish_time

                writer.finish()
                counter += 1
                writer = SplitWriter(f'{output}/{counter:05}.mcap', profile=profile)
                if tf_static_schema is not None and tf_static_channel is not None and \
                        tf_static_msgs is not None:
                    writer.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

            writer.write_message(schema, channel, message)

        writer.finish()


def cutter(input_file: Path, output: Path, settings: CutSettings) -> None:
    """Cut a file into multiple files."""
    if not input_file.exists():
        raise FileNotFoundError(f'Input file {input_file} does not exist')

    if settings.splits is not None:
        cutter_split(input_file, output, settings)
    else:
        cutter_split_on(input_file, output, settings)
