import logging
import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path

import strictyaml
from mcap.reader import DecodedMessageTuple, make_reader
from mcap.records import Schema, Statistics
from mcap.summary import Summary
from mcap.well_known import Profile, SchemaEncoding
from mcap_ros1.decoder import DecoderFactory as Ros1DecoderFactory
from mcap_ros2.decoder import DecoderFactory as Ros2DecoderFactory
from mcap_ros2.writer import Writer as McapWriter
from tqdm import tqdm

from kappe import __version__
from kappe.module.pointcloud import point_cloud
from kappe.module.tf import tf_remove, tf_static_insert
from kappe.module.timing import fix_ros1_time, time_offset
from kappe.plugin import ConverterPlugin, load_plugin
from kappe.settings import Settings
from kappe.utils.msg_def import get_message_definition

logger = logging.getLogger(__name__)


def generate_qos(config: dict) -> str:
    qos_default = {
        'history': 3,
        'depth': 0,
        'reliability': 1,
        'durability': 2,
        'deadline': {
            'sec': 9223372036,
            'nsec': 854775807,
        },
        'lifespan': {
            'sec': 9223372036,
            'nsec': 854775807,
        },
        'liveliness': 1,
        'liveliness_lease_duration': {
            'sec': 9223372036,
            'nsec': 854775807,
        },
        'avoid_ros_namespace_conventions': False,
    }
    return strictyaml.as_document([qos_default | config]).as_yaml()


class Converter:

    def __init__(self, config: Settings, input_path: Path, output_path: Path, raw_config: str = ''):
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

            lst.append((cls(**conv.config), conv.output_topic))
            self.plugin_conv[conv.input_topic] = lst

        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.f_reader = input_path.open('rb')
        self.f_writer = output_path.open('wb')

        self.reader = make_reader(self.f_reader)
        self.writer = McapWriter(self.f_writer)

        self.mcap_header = self.reader.get_header()
        if self.mcap_header.profile == Profile.ROS1 and self.config.msg_folder is None:
            logger.error('msg_folder is required for ROS1 mcap! See README for more information')

        summ = self.reader.get_summary()

        if summ is None:
            raise ValueError('Unindexed mcap!')

        self.summary: Summary = summ
        if self.summary.statistics is None:
            raise ValueError('Statistics not found in mcap!')

        self.statistics: Statistics = self.summary.statistics

        # mapping of schema name to schema
        self.schema_list: dict[str, Schema] = {}
        self.init_schema()

        self.init_channel()

    def init_schema(self):
        for schema in self.summary.schemas.values():
            if schema.encoding not in [SchemaEncoding.ROS1, SchemaEncoding.ROS2]:
                logger.warning(
                    'Schema "%s" has unsupported encoding "%s", skipping.',
                    schema.name,
                    schema.encoding)
                continue

            schema_name = schema.name
            schema_def = schema.data.decode()

            # replace schema name if mapping is defined
            schema_name = self.config.msg_schema.mapping.get(
                schema_name, schema_name)

            if schema_name in self.config.msg_schema.definition:
                schema_def = self.config.msg_schema.definition[schema_name]
            elif schema.encoding == SchemaEncoding.ROS1 or \
                    schema_name in self.config.msg_schema.mapping:
                # if schema is not defined in the config, and it is a ROS1 schema or
                # or scheme name is mapped, try to get the schema definition
                # from ROS or disk

                new_data = get_message_definition(schema_name, self.config.msg_folder)

                if new_data is not None:
                    schema_def = new_data
                else:
                    logger.warning(
                        'Schema "%s" not found, skipping.', schema.name)
                    continue

            self.schema_list[schema.name] = self.writer.register_msgdef(
                schema_name, schema_def)

        # Register schemas for converters
        for conv_list in self.plugin_conv.values():
            for conv, _out_topic in conv_list:
                out_schema = conv.output_schema
                if out_schema in self.schema_list:
                    continue

                new_data = get_message_definition(out_schema, self.config.msg_folder)

                if new_data is None:
                    raise ValueError(
                        f'Converter: Output schema "{out_schema}" not found')
                self.schema_list[out_schema] = self.writer.register_msgdef(
                    out_schema, new_data)

    def init_channel(self):
        for channel in self.summary.channels.values():
            metadata = channel.metadata
            topic = channel.topic
            org_schema: Schema = self.summary.schemas[channel.schema_id]
            if org_schema.name not in self.schema_list:
                logger.warning(
                    'Channel "%s" missing schema "%s", skipping.',
                    channel.topic,
                    org_schema.name,
                )
                continue

            # skip removed topics
            if channel.topic in self.config.topic.remove:
                continue

            schema_id: int = self.schema_list[org_schema.name].id

            topic = self.config.topic.mapping.get(topic, topic)
            # Workaround, to set metadata for the channel
            if topic not in self.writer._channel_ids:  # noqa: SLF001
                if self.mcap_header.profile == Profile.ROS1:
                    metadata = {
                        'offered_qos_profiles': generate_qos({
                            'durability': 1 if metadata.get('latching') else 2,
                        }),
                    }

                # TODO: make QoS configurable
                # TODO: add QoS enums
                if topic in ['/tf_static']:
                    old_qos = {}
                    if 'offered_qos_profiles' in metadata:
                        old_qos = strictyaml.load(
                            metadata['offered_qos_profiles']).data[0]
                    new_qos = {'durability': 1}
                    metadata['offered_qos_profiles'] = generate_qos(
                        old_qos | new_qos)

                channel_id = self.writer._writer.register_channel(  # noqa: SLF001
                    topic=topic,
                    message_encoding='cdr',
                    schema_id=schema_id,
                    metadata=metadata,
                )
                self.writer._channel_ids[topic] = channel_id  # noqa: SLF001

    def get_selected_channels(self) -> set[str]:
        """Get a list of channels that should be converted."""
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
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> Iterator[DecodedMessageTuple]:
        self.f_reader.seek(0)
        if self.mcap_header.profile == Profile.ROS1:
            self.reader = make_reader(
                self.f_reader, decoder_factories=[Ros1DecoderFactory()],
            )

            msg_iter = self.reader.iter_decoded_messages(
                topics=topics,
                start_time=start_time,
                end_time=end_time,
            )
        elif self.mcap_header.profile == Profile.ROS2:
            self.reader = make_reader(
                self.f_reader, decoder_factories=[Ros2DecoderFactory()],
            )

            msg_iter = self.reader.iter_decoded_messages(
                topics=topics,
                start_time=start_time,
                end_time=end_time,
            )
        else:
            raise ValueError(f'Unsupported profile {self.mcap_header.profile}')

        return msg_iter

    def process_message(self, msg: DecodedMessageTuple):
        schema, channel, message, ros_msg = msg
        schema_name = schema.name
        topic = channel.topic

        # handling of converters
        conv_list = self.plugin_conv.get(topic, [])
        for conv, output_topic in conv_list:
            if conv_msg := conv.convert(ros_msg):
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
            fix_ros1_time(ros_msg)

        # drop messages that are not in the schema list
        msg_schema = self.schema_list.get(schema_name)
        if msg_schema is None:
            return

        if topic in ['/tf', '/tf_static']:
            tf_remove(self.config.tf_static, msg)

        if topic in ['/tf_static'] and not self.tf_inserted:
            self.tf_inserted = tf_static_insert(self.config.tf_static, msg)

        if topic in self.config.time_offset:
            time_offset(self.config.time_offset[topic], msg)

        if schema_name in [
            'sensor_msgs/msg/PointCloud2',
                'sensor_msgs/PointCloud2',
        ] and topic in self.config.point_cloud:
            point_cloud(self.config.point_cloud[topic], msg)

        self.writer.write_message(
            topic=self.config.topic.mapping.get(topic, topic),
            schema=self.schema_list[schema_name],
            message=ros_msg,
            log_time=message.log_time,
            publish_time=message.publish_time,
            sequence=message.sequence,
        )

    def process_file(self, tqdm_idx: int = 0):

        start_time = self.statistics.message_start_time / 1e9
        if self.config.time_start is not None:
            start_time = max(start_time, self.config.time_start.timestamp())

        end_time = self.statistics.message_end_time / 1e9
        if self.config.time_end is not None:
            end_time = min(end_time, self.config.time_end.timestamp())

        # TODO: make better!
        if self.config.keep_all_static_tf:
            tf_static_channel = filter(
                lambda x: x.topic == '/tf_static',
                self.summary.channels.values())

            tf_static_channel = list(tf_static_channel)
            if len(tf_static_channel) != 1:
                raise ValueError(f'Found {len(tf_static_channel)} tf_static channels')

            tf_static_channel = tf_static_channel[0]
            tf_static_amount = self.statistics.channel_message_counts[tf_static_channel.id]
            logger.info('Found %d tf_static messages', tf_static_amount)
            # read all tf_static messages
            tf_static_iter = self.read_ros_messaged(topics=['/tf_static'])

            if tf_static_iter is None:
                raise ValueError('tf_static_iter is None')

            secs = int(start_time)
            nsecs = int((start_time - secs) * 1e9)
            nano_int = int(start_time * 1e9)

            for count, msg in enumerate(tf_static_iter, 1):
                # patch header stamp
                for transform in msg.ros_msg.transforms:
                    # foxglove does not tf msg with the exact same timestamp
                    nsecs += 1
                    nano_int += 1

                    transform.header.stamp.sec = secs
                    transform.header.stamp.nanosec = nsecs

                self.writer.write_message(
                    topic=msg.channel.topic,
                    schema=self.schema_list[msg.schema.name],
                    message=msg.ros_msg,
                    log_time=nano_int,
                    publish_time=nano_int,
                    sequence=msg.sequence_count,
                )

                # performance hack
                if count == tf_static_amount:
                    break

        filtered_channels = self.get_selected_channels()

        msg_iter = self.read_ros_messaged(
            topics=filtered_channels,
            start_time=self.config.time_start,
            end_time=self.config.time_end)
        if msg_iter is None:
            raise ValueError('msg_iter is None')

        logger.info('Topics: %d, filtered topics: %d', len(
            self.summary.channels), len(filtered_channels))
        logger.debug('Filtered topics: %s', filtered_channels)
        start_time = int(start_time * 1e3)  # convert to ms
        end_time = int(end_time * 1e3)  # convert to ms

        duration = end_time - start_time

        with tqdm(
            total=duration,
            position=tqdm_idx,
            desc=f'{self.input_path.name}',
            unit='secs',
        ) as pbar:
            for msg in msg_iter:
                message = msg[2]
                pbar.update((message.log_time // 1e6 - start_time) - pbar.n)
                self.process_message(msg)

    def finish(self):
        # save used convert config
        self.writer._writer.add_attachment(  # noqa: SLF001
            create_time=time.time_ns(),
            log_time=time.time_ns(),
            name='convert_config.yaml',
            media_type='text/yaml',
            data=self.config.raw_text.encode(),
        )

        # store useful information
        self.writer._writer.add_metadata(  # noqa: SLF001
            name='convert_metadata',
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
