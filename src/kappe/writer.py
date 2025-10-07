import json
import time
from dataclasses import dataclass
from io import BufferedWriter
from typing import IO, Any

import mcap
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message import Message as ProtobufMessage
from mcap.exceptions import McapError
from mcap.records import Channel, Message, Schema
from mcap.well_known import MessageEncoding, SchemaEncoding
from mcap.writer import CompressionType, IndexType
from mcap.writer import Writer as McapWriter
from mcap_protobuf.decoder import DecoderFactory as ProtobufDecoderFactory

# TODO: vendor these
from mcap_ros1._vendor.genpy import dynamic as ros1_dynamic
from mcap_ros2._dynamic import (
    DecoderFunction,
    EncoderFunction,
    generate_dynamic,
    serialize_dynamic,
)

from kappe import __version__


class ROS1DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS1 message."""


class ROS2DecodeError(McapError):
    """Raised if a MCAP message record cannot be decoded as a ROS2 message."""


class ROS2EncodeError(McapError):
    """Raised if a ROS2 message cannot be encoded."""


class ProtobufDecodeError(McapError):
    """Raised if an MCAP message record cannot be decoded as a Protobuf message."""


class ProtobufEncodeError(McapError):
    """Raised if an MCAP message record cannot be encoded as a Protobuf message."""


class JsonDecodeError(McapError):
    """Raised if an MCAP message record cannot be decoded as a Json message."""


class JsonEncodeError(McapError):
    """Raised if an MCAP message record cannot be encoded as a Json message."""


def _get_decoder_ros1(schema: Schema) -> DecoderFunction:
    if schema.encoding != SchemaEncoding.ROS1:
        raise ROS1DecodeError(f'can\'t parse schema with encoding "{schema}"')

    type_dict: dict[str, type[Any]] = ros1_dynamic.generate_dynamic(
        schema.name, schema.data.decode()
    )
    if schema.name not in type_dict:
        raise ROS1DecodeError(f'schema parsing failed for "{schema.name}"')
    generated_type = type_dict[schema.name]

    def decoder(data: bytes):  # noqa: ANN202
        ros_msg = generated_type()
        ros_msg.deserialize(data)
        return ros_msg

    return decoder


def _get_decoder_ros2(schema: Schema) -> DecoderFunction:
    if schema.encoding != SchemaEncoding.ROS2:
        raise ROS2DecodeError(f'can\'t parse schema with encoding "{schema}"')

    type_dict = generate_dynamic(schema.name, schema.data.decode())
    if schema.name not in type_dict:
        raise ROS2DecodeError(f'schema parsing failed for "{schema.name}"')
    return type_dict[schema.name]


_protobuf_decoder_factory = ProtobufDecoderFactory()


def build_file_descriptor_set(descriptor: Descriptor) -> FileDescriptorSet:
    file_descriptor_set = FileDescriptorSet()
    seen_dependencies: set[str] = set()

    def append_file_descriptor(file_descriptor: FileDescriptor) -> None:
        for dep in file_descriptor.dependencies:
            if dep.name not in seen_dependencies:
                seen_dependencies.add(dep.name)
                append_file_descriptor(dep)
        file_descriptor.CopyToProto(file_descriptor_set.file.add())

    append_file_descriptor(descriptor.file)
    return file_descriptor_set


def _get_decoder_protobuf(schema: Schema) -> DecoderFunction:
    decoder = _protobuf_decoder_factory.decoder_for(SchemaEncoding.Protobuf, schema)
    assert decoder is not None
    return decoder


# def _get_decoder_json(schema: Schema) -> DecoderFunction:
#     def decoder(data: bytes) -> dict:
#         return json.loads(data)
#
#     return decoder


_decoder_cache: dict[int, DecoderFunction] = {}


def get_decoder(schema: Schema) -> DecoderFunction:
    cache_key = hash((schema.id, schema.name, schema.data))
    decoder = _decoder_cache.get(cache_key)
    if decoder is not None:
        return decoder

    if schema.encoding == SchemaEncoding.ROS2:
        decoder = _get_decoder_ros2(schema)
    elif schema.encoding == SchemaEncoding.ROS1:
        decoder = _get_decoder_ros1(schema)
    elif schema.encoding == SchemaEncoding.Protobuf:
        decoder = _get_decoder_protobuf(schema)
    # elif schema.encoding == SchemaEncoding.JSONSchema:
    #     decoder = _get_decoder_json(schema)
    else:
        raise ROS2DecodeError(f'can\'t parse schema with encoding "{schema.encoding}"')
    _decoder_cache[cache_key] = decoder
    return decoder


_encoder_cache: dict[int, EncoderFunction] = {}


def get_encoder(schema: Schema) -> EncoderFunction:
    cache_key = hash((schema.id, schema.name, schema.data))
    encoder = _encoder_cache.get(cache_key)

    if encoder:
        return encoder

    match schema.encoding:
        case SchemaEncoding.ROS2:
            type_dict = serialize_dynamic(schema.name, schema.data.decode())
            # Check if schema.name is in type_dict
            if schema.name not in type_dict:
                raise ROS2EncodeError(f'schema parsing failed for "{schema.name}"')
            encoder = type_dict[schema.name]
        case SchemaEncoding.Protobuf:

            def _encoder(decoded_message: ProtobufMessage) -> bytes:
                return decoded_message.SerializeToString()

            encoder = _encoder
        case _:
            raise ROS2EncodeError(f'can\'t parse schema with encoding "{schema.encoding}"')

    _encoder_cache[cache_key] = encoder
    return encoder


@dataclass
class WrappedDecodedMessage:
    schema: Schema | None
    channel: Channel
    message: Message

    _decoded_message: Any | None = None

    def decode(self) -> Any:
        assert self.schema is not None
        if self._decoded_message is None:
            decoder = get_decoder(self.schema)
            self._decoded_message = decoder(self.message.data)
        return self._decoded_message

    @property
    def decoded_message(self) -> Any:
        return self.decode()

    def encode(self) -> bytes:
        # NOTE: this uses the original schema, which might have an colliding schema.id with
        # the output
        if self._decoded_message is None:
            return self.message.data
        assert self.schema is not None
        encoder = get_encoder(self.schema)
        return encoder(self._decoded_message)


class ROS2WriteError(McapError):
    """Raised if a ROS2 message cannot be encoded to CDR with a given schema."""


def _library_identifier() -> str:
    mcap_version = getattr(mcap, '__version__', '<=0.0.10')
    return f'kappe-ros2 {__version__}; mcap {mcap_version}'


class WrappedWriter:
    def __init__(
        self,
        output: str | (IO[Any] | BufferedWriter),
        *,
        chunk_size: int = 1024 * 1024,
        compression: CompressionType = CompressionType.ZSTD,
        enable_crcs: bool = True,
        index_types: IndexType = IndexType.ALL,
    ) -> None:
        self._writer = McapWriter(
            output=output,
            chunk_size=chunk_size,
            compression=compression,
            enable_crcs=enable_crcs,
            index_types=index_types,
        )
        self._channel_ids: dict[str, int] = {}
        self._writer.start(profile='ros2', library=_library_identifier())
        self._finished = False

        self._encoders_cache: dict[int, EncoderFunction] = {}

    def finish(self) -> None:
        """Finishes writing to the MCAP stream. This must be called before the stream is closed."""
        if not self._finished:
            self._writer.finish()
            self._finished = True

    def register_msgdef(self, datatype: str, msgdef_text: str) -> Schema:
        """Write a Schema record for a ROS2 message definition."""
        msgdef_data = msgdef_text.encode()
        schema_id = self._writer.register_schema(datatype, SchemaEncoding.ROS2, msgdef_data)
        return Schema(id=schema_id, name=datatype, encoding=SchemaEncoding.ROS2, data=msgdef_data)

    def register_schema(self, schema: Schema) -> Schema:
        schema_id = self._writer.register_schema(
            name=schema.name, encoding=schema.encoding, data=schema.data
        )
        return Schema(id=schema_id, name=schema.name, encoding=schema.encoding, data=schema.data)

    def register_protobuf(self, name: str, descriptor: Descriptor) -> Schema:
        """Writer a Schema record for a Protobuf message definition."""
        file_descriptor_set = build_file_descriptor_set(descriptor=descriptor)

        schema_id = self._writer.register_schema(
            name=descriptor.full_name,
            encoding='protobuf',
            data=file_descriptor_set.SerializeToString(),
        )
        return Schema(
            id=schema_id,
            name=name,
            encoding=SchemaEncoding.Protobuf,
            data=file_descriptor_set.SerializeToString(),
        )

    def write_message(  # noqa: PLR0913
        self,
        topic: str,
        schema: Schema,
        message: Any | WrappedDecodedMessage,
        log_time: int | None = None,
        publish_time: int | None = None,
        sequence: int = 0,
    ) -> None:
        """
        Write a ROS2 Message record, automatically registering a channel as needed.

        :param topic: The topic of the message.
        :param message: The message to write.
        :param log_time: The time at which the message was logged as a nanosecond UNIX timestamp.
            Will default to the current time if not specified.
        :param publish_time: The time at which the message was published as a nanosecond UNIX
            timestamp. Will default to ``log_time`` if not specified.
        :param sequence: An optional sequence number.
        """

        if isinstance(message, WrappedDecodedMessage):
            data = message.encode()
        else:
            encoder = get_encoder(schema)
            data = encoder(message)

        if topic not in self._channel_ids:
            message_encoding = MessageEncoding.CDR
            if schema.encoding == SchemaEncoding.Protobuf:
                message_encoding = MessageEncoding.Protobuf
            channel_id = self._writer.register_channel(
                topic=topic,
                message_encoding=message_encoding,
                schema_id=schema.id,
            )
            self._channel_ids[topic] = channel_id
        channel_id = self._channel_ids[topic]

        if log_time is None:
            log_time = time.time_ns()
        if publish_time is None:
            publish_time = log_time
        self._writer.add_message(
            channel_id=channel_id,
            log_time=log_time,
            publish_time=publish_time,
            sequence=sequence,
            data=data,
        )

    def __enter__(self) -> 'WrappedWriter':
        """Context manager support."""
        return self

    def __exit__(self, exc_: object, exc_type_: object, tb_: object) -> None:
        """Call finish() on exit."""
        self.finish()
