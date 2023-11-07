import logging
from pathlib import Path

from mcap.reader import McapReader, make_reader
from mcap.records import Channel, Message, Schema
from mcap.writer import Writer
from pydantic import BaseModel, Field, model_validator
from tqdm import tqdm

logger = logging.getLogger(__name__)


class CutSplits(BaseModel):
    start: float
    end: float
    name: str

    @model_validator(mode='after')
    def validate(self) -> 'CutSplits':
        if self.end < self.start:
            raise ValueError('end must be grater than start')
        return self


class CutSplitOn(BaseModel):
    topic: str
    debounce: float = Field(
        description='Number of seconds to wait before splitting on the same topic',
        default=0.0,
    )


class CutSettings(BaseModel):
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

    def set_static_tf(self, schema: Schema, channel: Channel, data: list[bytes]) -> None:
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


def collect_tf(reader: McapReader) -> None | tuple[Schema, Channel, list[bytes]]:
    logger.info('Collecting static tf data')

    summary = reader.get_summary()
    assert summary is not None
    statistics = summary.statistics
    assert statistics is not None

    channels = list(filter(lambda x: x.topic == '/tf_static', summary.channels.values()))
    assert len(channels) > 0
    tf_static_channel = channels[0]
    if tf_static_channel is None:
        return None

    tf_static_amount = statistics.channel_message_counts.get(tf_static_channel.id)
    if tf_static_amount is None:
        # there will be no tf messages
        return None

    tf_static_schema: Schema | None = summary.schemas.get(tf_static_channel.schema_id)
    if tf_static_schema is None:
        return None

    logger.info('Found %d tf_static messages', tf_static_amount)

    tf_static_msgs = []
    for count, (_, _, message) in enumerate(
        reader.iter_messages(
            topics=['/tf_static'],
        )
    ):
        tf_static_msgs.append(message.data)

        # performance hack
        if count >= tf_static_amount:
            break

    logger.info('Collecting static tf data done')

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

        if settings.keep_tf_tree and (ret := collect_tf(reader)):
            tf_static_schema, tf_static_channel, tf_static_msgs = ret
            for w in outputs:
                w.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

        pbar = tqdm(total=int((max_end_time - min_start_time) / 1e9))

        for schema, channel, message in reader.iter_messages(
            start_time=min_start_time,
            end_time=max_end_time,
        ):
            if schema is None:
                continue
            pub_time_sec = message.publish_time / 1e9
            for i, split in enumerate(settings.splits):
                if split.start <= pub_time_sec <= split.end:
                    outputs[i].write_message(schema, channel, message)
            offset = int(pub_time_sec - min_start_time / 1e9)
            pbar.update(offset - pbar.n)

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
        if settings.keep_tf_tree and (ret := collect_tf(reader)):
            tf_static_schema, tf_static_channel, tf_static_msgs = ret
            writer.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

        # TODO: check if topic exists
        for schema, channel, message in tqdm(reader.iter_messages()):
            if schema is None:
                continue

            if (
                channel.topic == settings.split_on_topic.topic
                and message.publish_time - last_split_time > debounce_ns
            ):
                logger.info('Found split point at %.2fs', message.publish_time / 1e9)
                last_split_time = message.publish_time

                writer.finish()
                counter += 1
                writer = SplitWriter(f'{output}/{counter:05}.mcap', profile=profile)
                if (
                    tf_static_schema is not None
                    and tf_static_channel is not None
                    and tf_static_msgs is not None
                ):
                    writer.set_static_tf(tf_static_schema, tf_static_channel, tf_static_msgs)

            writer.write_message(schema, channel, message)

        writer.finish()


def cutter(input_file: Path, output: Path, settings: CutSettings) -> None:
    """Cut a file into multiple files."""
    if not input_file.exists():
        raise FileNotFoundError(f'Input file {input_file} does not exist')

    if settings.splits is not None:
        cutter_split(input_file, output, settings)
    elif settings.split_on_topic is not None:
        cutter_split_on(input_file, output, settings)
    else:
        raise ValueError('split or split_on_topic must be set')
