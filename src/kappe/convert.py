import logging
import time
import warnings
from collections.abc import Generator, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from mcap.reader import NonSeekingReader, make_reader
from mcap.records import Channel, Schema
from mcap.well_known import Profile, SchemaEncoding
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory
from pydantic_yaml import to_yaml_str
from tqdm import tqdm

from kappe import __version__
from kappe.module.pointcloud import point_cloud
from kappe.module.qos import DurabilityPolicy, Qos, dump_qos_list, parse_qos_list
from kappe.module.tf import (
    TF_SCHEMA_NAME,
    TF_SCHEMA_TEXT,
    tf_apply_offset,
    tf_remove,
    tf_static_insert,
)
from kappe.module.timing import fix_ros1_time, time_offset
from kappe.plugin import ConverterPlugin, load_plugin
from kappe.settings import Settings
from kappe.utils.msg_def import get_message_definition
from kappe.writer import WrappedDecodedMessage, WrappedWriter

if TYPE_CHECKING:
    from mcap.records import Statistics
    from mcap.summary import Summary

logger = logging.getLogger(__name__)


class Converter:
    def __init__(
        self, config: Settings, input_path: Path, output_path: Path, raw_config: str = ''
    ) -> None:
        self.config = config
        self.input_path = input_path
        self.output_path = output_path
        self.raw_config = raw_config

        self.drop_msg_count: dict[str, int] = {}

        self.tf_inserted = False

        # maps input topic to list of plugins
        self.plugin_conv: dict[str, list[tuple[ConverterPlugin, str]]] = {}

        # load plugins
        for conv in config.plugins:
            lst = self.plugin_conv.get(conv.input_topic, [])
            cls = load_plugin(self.config.plugin_folder, conv.name)

            lst.append((cls(**conv.settings), conv.output_topic))
            self.plugin_conv[conv.input_topic] = lst

        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.f_reader = input_path.open('rb')
        self.f_writer = output_path.open('wb')

        reader = make_reader(self.f_reader)
        self.writer = WrappedWriter(self.f_writer)

        self.mcap_header = reader.get_header()

        self.summary: Summary | None = None
        self.statistics: Statistics | None = None

        try:
            self.summary = reader.get_summary()
        except Exception:  # noqa: BLE001
            logger.warning('Unindexed/Broken MCAP, trying to read, CAN BE SLOW!')

        if self.summary:
            self.statistics = self.summary.statistics

        self.tf_static_channel_id: int | None = None

        # mapping of schema name to schema
        self.schema_list: dict[str, Schema] = {}
        # Set of schema ids already processed
        self.schema_original: dict[int, Schema] = {}
        # Set of channel ids already processed
        self.channel_seen: set[int] = set()

        self.tf_inserted = False

        self.init_schema()

        self.init_channel()

    def init_schema(self) -> None:
        if self.summary is not None:
            for schema in self.summary.schemas.values():
                self.add_schema(schema)

        # Register schemas for converters
        for conv_list in self.plugin_conv.values():
            for conv, _out_topic in conv_list:
                out_schema = conv.output_schema
                if out_schema in self.schema_list:
                    continue

                new_data = get_message_definition(
                    out_schema, self.config.ros_distro, self.config.msg_folder
                )

                if new_data is None:
                    raise ValueError(f'Converter: Output schema "{out_schema}" not found')
                self.schema_list[out_schema] = self.writer.register_msgdef(out_schema, new_data)

        if (self.config.tf or self.config.tf_static) and TF_SCHEMA_NAME not in self.schema_list:
            # insert tf schema
            self.schema_list[TF_SCHEMA_NAME] = self.writer.register_msgdef(
                TF_SCHEMA_NAME,
                TF_SCHEMA_TEXT,
            )

    def add_schema(self, schema: Schema) -> None:
        if schema.id in self.schema_original:
            # schema already processed
            return

        self.schema_original[schema.id] = schema

        if schema.encoding not in [SchemaEncoding.ROS1, SchemaEncoding.ROS2]:
            logger.warning(
                'Schema "%s" has unsupported encoding "%s", skipping.',
                schema.name,
                schema.encoding,
            )
            return

        schema_name = schema.name
        schema_def = schema.data.decode()

        # replace schema name if mapping is defined
        schema_name = self.config.msg_schema.mapping.get(schema_name, schema_name)

        if schema_name in self.config.msg_schema.definition:
            schema_def = self.config.msg_schema.definition[schema_name]
        elif (
            schema.encoding == SchemaEncoding.ROS1 or schema_name in self.config.msg_schema.mapping
        ):
            # if schema is not defined in the config, and it is a ROS1 schema or
            # or scheme name is mapped, try to get the schema definition
            # from ROS or disk

            new_data = get_message_definition(
                schema_name, self.config.ros_distro, self.config.msg_folder
            )

            if new_data is not None:
                schema_def = new_data
            else:
                msg = f'Scheme "{schema.name}" not found, skipping.'
                raise ValueError(msg)

        self.schema_list[schema.name] = self.writer.register_msgdef(schema_name, schema_def)

    def init_channel(self) -> None:
        if self.summary is not None:
            for channel in self.summary.channels.values():
                self.add_channel(channel)

        if self.config.tf and '/tf' not in self.writer._channel_ids:  # noqa: SLF001
            # insert tf schema
            self.writer._writer.register_channel(  # noqa: SLF001
                topic='/tf',
                message_encoding='cdr',
                schema_id=self.schema_list[TF_SCHEMA_NAME].id,
            )

        if self.config.tf_static and '/tf_static' not in self.writer._channel_ids:  # noqa: SLF001
            # insert tf schema
            self.writer._writer.register_channel(  # noqa: SLF001
                topic='/tf_static',
                message_encoding='cdr',
                schema_id=self.schema_list[TF_SCHEMA_NAME].id,
            )

    def add_channel(self, channel: Channel) -> None:
        if channel.id in self.channel_seen:
            # channel already processed
            return
        self.channel_seen.add(channel.id)

        metadata = channel.metadata
        topic = channel.topic
        org_schema: Schema = self.schema_original[channel.schema_id]
        if org_schema.name not in self.schema_list:
            logger.warning(
                'Channel "%s" missing schema "%s", skipping.',
                channel.topic,
                org_schema.name,
            )
            return

        # skip removed topics
        if channel.topic in self.config.topic.remove:
            return

        schema_id: int = self.schema_list[org_schema.name].id

        topic = self.config.topic.mapping.get(topic, topic)
        # Workaround, to set metadata for the channel
        if topic not in self.writer._channel_ids:  # noqa: SLF001
            if self.mcap_header.profile == Profile.ROS1:
                qos = Qos()
                qos.durability = (
                    DurabilityPolicy.TRANSIENT_LOCAL
                    if metadata.get('latching')
                    else DurabilityPolicy.VOLATILE
                )

                metadata = {'offered_qos_profiles': dump_qos_list(qos)}

            # TODO: make QoS configurable
            # TODO: add QoS enums
            if topic in ['/tf_static']:
                qos = Qos()
                if metadata.get('offered_qos_profiles'):
                    qos = parse_qos_list(metadata['offered_qos_profiles'])[0]
                qos.durability = DurabilityPolicy.VOLATILE
                metadata['offered_qos_profiles'] = dump_qos_list(qos)

            channel_id = self.writer._writer.register_channel(  # noqa: SLF001
                topic=topic,
                message_encoding='cdr',
                schema_id=schema_id,
                metadata=metadata,
            )
            self.writer._channel_ids[topic] = channel_id  # noqa: SLF001

    def get_selected_channels(self) -> set[str] | None:
        """Get a list of channels that should be converted."""
        if self.summary is None:
            return None
        filtered_channels: set[str] = set()
        for channel in self.summary.channels.values():
            keep = True
            # skip topics that are in the remove list
            if channel.topic in self.config.topic.remove:
                keep = False

            # skip topics that are not in the schema list
            schema_name = self.summary.schemas[channel.schema_id].name
            if schema_name not in self.schema_list:
                keep = False

            if channel.topic in self.plugin_conv:
                keep = True

            if keep:
                filtered_channels.add(channel.topic)

        return filtered_channels

    def read_ros_messaged(
        self,
        topics: Iterable[str] | None = None,
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> Generator[WrappedDecodedMessage, None, None]:
        """Read messages from the mcap file.

        Args:
            topics: List of topics to read. If None, all topics are read.
            start_time: Start time in seconds. If None, start at the beginning.
            end_time: End time in seconds. If None, read until the end.
        """
        self.f_reader.seek(0)
        decoder = Ros2DecoderFactory()
        if self.mcap_header.profile == Profile.ROS1:
            decoder = Ros1DecoderFactory()
        elif self.mcap_header.profile != Profile.ROS2:
            warnings.warn(
                f'Unsupported profile: {self.mcap_header.profile}, trying to read as ROS2',
                RuntimeWarning,
                stacklevel=1,
            )

        if self.summary:
            reader = make_reader(
                self.f_reader,
                decoder_factories=[decoder],
            )
        else:
            reader = NonSeekingReader(self.f_reader)

        decoder_cache = {}

        try:
            for schema, channel, message in reader.iter_messages(
                topics=topics,
                start_time=int(start_time * 1e9) if start_time else None,
                end_time=int(end_time * 1e9) if end_time else None,
                # set to true if summary is None,
                # otherwise the mcap is read into ram for some reason
                log_time_order=self.summary is not None,
            ):
                yield WrappedDecodedMessage(schema, channel, message, decoder_cache=decoder_cache)
        except Exception as e:
            if self.summary is None:
                logger.info('Unindex mcap file, stopped at first error: %s', e)
            else:
                raise

    def process_message(self, msg: WrappedDecodedMessage) -> None:  # noqa: PLR0912
        schema = msg.schema
        channel = msg.channel
        message = msg.message
        schema_name = schema.name
        topic = channel.topic

        if schema:
            self.add_schema(schema)
        self.add_channel(channel)

        if offset := (self.config.time_offset.get(topic) or self.config.time_offset.get('default')):
            time_offset(offset, msg)

        if not self.tf_inserted:
            self.tf_inserted = True
            # TODO: use the time of the current message
            insert_tf = tf_static_insert(self.config.tf_static, message.log_time)
            if insert_tf is not None:
                self.writer.write_message(
                    topic='/tf_static',
                    schema=self.schema_list[TF_SCHEMA_NAME],
                    message=insert_tf,
                    log_time=message.log_time,
                    publish_time=message.log_time,
                )

        # handling of converters
        conv_list = self.plugin_conv.get(topic, [])
        for conv, output_topic in conv_list:
            if conv_msg := conv.convert(msg.decoded_message):
                # TODO: pass this to process_message?
                self.writer.write_message(
                    topic=output_topic,
                    schema=self.schema_list[conv.output_schema],
                    message=conv_msg,
                    log_time=message.log_time,
                    publish_time=message.publish_time,
                    sequence=message.sequence,
                )

        # late remove topics which are required by a plugin
        if topic in self.config.topic.remove:
            return

        # drop every n-th message
        if topic in self.config.topic.drop:
            drop_count = self.drop_msg_count.get(topic, 0)
            self.drop_msg_count[topic] = drop_count + 1

            drop_cfg = self.config.topic.drop[topic]
            if drop_count % drop_cfg == 0:
                return

        if self.mcap_header.profile == Profile.ROS1:
            fix_ros1_time(msg.decoded_message)

        # drop messages that are not in the schema list
        msg_schema = self.schema_list.get(schema_name)
        if msg_schema is None:
            return

        if topic == '/tf':
            if not tf_remove(self.config.tf, msg):
                # remove empty tf messages
                return
            tf_apply_offset(self.config.tf, msg)
        elif topic == '/tf_static':
            if not tf_remove(self.config.tf_static, msg):
                # remove empty tf messages
                return
            tf_apply_offset(self.config.tf_static, msg)
        elif (
            schema_name in ['sensor_msgs/msg/PointCloud2', 'sensor_msgs/PointCloud2']
            and topic in self.config.point_cloud
        ):
            point_cloud(self.config.point_cloud[topic], msg)

        # Apply frame_id mapping
        if (
            (new_frame_id := self.config.frame_id_mapping.get(topic))
            and hasattr(msg.decoded_message, 'header')
            and hasattr(msg.decoded_message.header, 'frame_id')
        ):
            msg.decoded_message.header.frame_id = new_frame_id

        self.writer.write_message(
            topic=self.config.topic.mapping.get(topic, topic),
            schema=self.schema_list[schema_name],
            message=msg,
            log_time=message.log_time,
            publish_time=message.publish_time,
            sequence=message.sequence,
        )

    def collect_tf_static(self, start_time_sec: float) -> None:
        if self.summary is None:
            raise ValueError('Cannot collect tf_static without summary')
        if self.statistics is None:
            raise ValueError('Cannot collect tf_static without summary')

        start_time_ns = int(start_time_sec * 1e9)
        start_time_part_sec = int(start_time_sec)
        start_time_part_ns = int((start_time_sec - start_time_part_sec) * 1e9)

        tf_static_channel = filter(
            lambda x: x.topic == '/tf_static', self.summary.channels.values()
        )

        tf_static_channel = list(tf_static_channel)
        if len(tf_static_channel) != 1:
            raise ValueError(f'Found {len(tf_static_channel)} tf_static channels')

        tf_static_channel = tf_static_channel[0]
        tf_static_amount = self.statistics.channel_message_counts.get(tf_static_channel.id)
        if tf_static_amount is None:
            logger.info('Found NO tf_static messages')
            return
        logger.info('Found %d tf_static messages', tf_static_amount)
        # read all tf_static messages
        tf_static_iter = self.read_ros_messaged(topics=['/tf_static'])

        if tf_static_iter is None:
            raise ValueError('tf_static_iter is None')

        for count, msg in enumerate(tf_static_iter, 1):
            if msg.schema is None:
                continue
            # patch header stamp
            ros_msg = msg.decoded_message
            for transform in ros_msg.transforms:
                # foxglove does not tf msg with the exact same timestamp
                start_time_part_ns += 1
                start_time_ns += 1

                transform.header.stamp.sec = start_time_part_sec
                transform.header.stamp.nanosec = start_time_part_ns

            self.writer.write_message(
                topic=msg.channel.topic,
                schema=self.schema_list[msg.schema.name],
                message=ros_msg,
                log_time=start_time_ns,
                publish_time=start_time_ns,
                sequence=msg.message.sequence,
            )

            # performance hack
            if count == tf_static_amount:
                break

    def _calculate_start_end(
        self,
        start_time_ns: float,
        end_time_ns: float | None,
    ) -> tuple[float, float | None]:
        start_sec = start_time_ns / 1e9
        end_sec: float | None = (end_time_ns + 1) / 1e9 if end_time_ns is not None else None

        if (cfg_start := self.config.time_start) is not None:
            start_sec = max(start_sec, cfg_start)

        if (cfg_end := self.config.time_end) is not None:
            # Treat small numbers as "duration"
            cfg_end = start_sec + cfg_end if cfg_end < 100_000_000 else cfg_end
            end_sec = min(filter(None, (end_sec, cfg_end)))

        return start_sec, end_sec

    def process_file(self, tqdm_idx: int = 0) -> None:
        start_time_sec = None
        end_time_sec = None
        duration_sec = None
        if self.statistics:
            start_time_sec, end_time_sec = self._calculate_start_end(
                self.statistics.message_start_time, self.statistics.message_end_time
            )
            if end_time_sec:
                duration_sec = end_time_sec - start_time_sec

            if self.config.keep_all_static_tf:
                self.collect_tf_static(start_time_sec)

        filtered_channels = self.get_selected_channels()
        msg_iter = self.read_ros_messaged(
            topics=filtered_channels,
            start_time=start_time_sec if self.config.time_start else None,
            end_time=end_time_sec if self.config.time_end else None,
        )
        if msg_iter is None:
            raise ValueError('msg_iter is None')

        if filtered_channels and self.summary:
            logger.info(
                'Topics: %d, filtered topics: %d',
                len(self.summary.channels),
                len(filtered_channels),
            )
        logger.debug('Filtered topics: %s', filtered_channels)

        with tqdm(
            total=duration_sec,
            position=tqdm_idx,
            desc=f'{self.input_path.name}',
            unit='secs' if duration_sec else 'msgs',
            bar_format='{l_bar}{bar}| {n:.02f}/{total:.02f} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'  # noqa: E501
            if duration_sec
            else None,
            disable=not self.config.progress,
        ) as pbar:
            for msg in msg_iter:
                message = msg.message
                log_time = message.log_time
                if start_time_sec is None:
                    start_time_sec, end_time_sec = self._calculate_start_end(log_time, None)

                if start_time_sec * 1e9 > log_time:
                    continue
                if end_time_sec and end_time_sec * 1e9 < log_time:
                    break
                if duration_sec is not None:
                    pbar.update((log_time / 1e9 - start_time_sec) - pbar.n)
                else:
                    pbar.update(1)

                self.process_message(msg)

    def finish(self) -> None:
        if self.config.save_metadata:
            # save used convert config
            self.writer._writer.add_attachment(  # noqa: SLF001
                create_time=time.time_ns(),
                log_time=time.time_ns(),
                name='kappe_config.yaml',
                media_type='text/yaml',
                data=to_yaml_str(self.config).encode(),
            )

            # store useful information
            self.writer._writer.add_metadata(  # noqa: SLF001
                name='kappe_metadata',
                data={
                    'input_path': str(self.input_path),
                    'output_path': str(self.output_path),
                    'date': datetime.now(tz=timezone.utc).isoformat(),
                    'version': __version__,
                },
            )

        self.writer.finish()

        self.f_reader.close()
        self.f_writer.close()
