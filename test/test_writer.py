"""Tests for the writer module's protobuf encoding/decoding support."""

from io import BytesIO

import pytest
from google.protobuf import descriptor_pb2
from mcap.records import Schema
from mcap.well_known import SchemaEncoding

from kappe.writer import (
    ROS2EncodeError,
    WrappedWriter,
    build_file_descriptor_set,
    get_decoder,
    get_encoder,
)


def _make_protobuf_schema() -> Schema:
    """Create a Schema record for google.protobuf.FileDescriptorProto."""
    descriptor = descriptor_pb2.FileDescriptorProto.DESCRIPTOR
    fds = build_file_descriptor_set(descriptor)
    return Schema(
        id=1,
        name=descriptor.full_name,
        encoding=SchemaEncoding.Protobuf,
        data=fds.SerializeToString(),
    )


def test_build_file_descriptor_set_basic():
    """Test that build_file_descriptor_set creates a valid FileDescriptorSet."""
    descriptor = descriptor_pb2.FileDescriptorProto.DESCRIPTOR
    fds = build_file_descriptor_set(descriptor)

    assert len(fds.file) >= 1
    # The descriptor's own file should be the last entry
    file_names = [f.name for f in fds.file]
    assert descriptor.file.name in file_names


def test_build_file_descriptor_set_with_dependencies():
    """Test that build_file_descriptor_set includes all transitive dependencies."""
    # FileDescriptorSet has dependencies (e.g. google/protobuf/descriptor.proto)
    descriptor = descriptor_pb2.FileDescriptorSet.DESCRIPTOR
    fds = build_file_descriptor_set(descriptor)

    file_names = [f.name for f in fds.file]
    assert descriptor.file.name in file_names
    # All dependencies should be included
    for dep in descriptor.file.dependencies:
        assert dep.name in file_names


def test_get_decoder_protobuf():
    """Test that get_decoder returns a working decoder for protobuf schemas."""
    schema = _make_protobuf_schema()
    decoder = get_decoder(schema)
    assert callable(decoder)

    # Create a sample protobuf message and encode it
    msg = descriptor_pb2.FileDescriptorProto()
    msg.name = 'test.proto'
    encoded = msg.SerializeToString()

    # Decode using our decoder
    decoded = decoder(encoded)
    assert decoded.name == 'test.proto'


def test_get_decoder_protobuf_caching():
    """Test that get_decoder caches decoders for protobuf schemas."""
    schema = _make_protobuf_schema()
    decoder1 = get_decoder(schema)
    decoder2 = get_decoder(schema)
    # Same decoder object should be returned from cache
    assert decoder1 is decoder2


def test_get_encoder_protobuf():
    """Test that get_encoder returns a working encoder for protobuf schemas."""
    schema = _make_protobuf_schema()
    encoder = get_encoder(schema)
    assert callable(encoder)

    # Create a sample protobuf message
    msg = descriptor_pb2.FileDescriptorProto()
    msg.name = 'test.proto'

    # Encode using our encoder
    encoded = encoder(msg)
    assert isinstance(encoded, bytes)

    # Verify it's valid protobuf by decoding
    decoded = descriptor_pb2.FileDescriptorProto()
    decoded.ParseFromString(encoded)
    assert decoded.name == 'test.proto'


def test_get_encoder_protobuf_roundtrip():
    """Test that encoding and decoding a protobuf message yields the original message."""
    schema = _make_protobuf_schema()
    encoder = get_encoder(schema)
    decoder = get_decoder(schema)

    msg = descriptor_pb2.FileDescriptorProto()
    msg.name = 'test.proto'
    msg.syntax = 'proto3'

    encoded = encoder(msg)
    decoded = decoder(encoded)

    assert decoded.name == msg.name
    assert decoded.syntax == msg.syntax


def test_get_encoder_unsupported_encoding():
    """Test that get_encoder raises for unsupported encodings."""
    schema = Schema(
        id=99,
        name='unknown/schema',
        encoding=SchemaEncoding.JSONSchema,
        data=b'{}',
    )
    with pytest.raises(ROS2EncodeError):
        get_encoder(schema)


def test_wrapped_writer_register_protobuf():
    """Test that WrappedWriter.register_protobuf creates the correct Schema."""
    buf = BytesIO()
    with WrappedWriter(buf) as writer:
        descriptor = descriptor_pb2.FileDescriptorProto.DESCRIPTOR
        schema = writer.register_protobuf(descriptor.full_name, descriptor)

    assert schema.name == descriptor.full_name
    assert schema.encoding == SchemaEncoding.Protobuf
    assert len(schema.data) > 0


def test_wrapped_writer_write_protobuf_message():
    """Test that WrappedWriter can write a protobuf message."""
    buf = BytesIO()
    with WrappedWriter(buf) as writer:
        descriptor = descriptor_pb2.FileDescriptorProto.DESCRIPTOR
        schema = writer.register_protobuf(descriptor.full_name, descriptor)

        msg = descriptor_pb2.FileDescriptorProto()
        msg.name = 'test.proto'
        writer.write_message('/test_topic', schema, msg, log_time=1000, publish_time=1000)

    # Should have written something
    assert len(buf.getvalue()) > 0


def test_wrapped_writer_register_schema_passthrough():
    """Test that WrappedWriter.register_schema passes through the schema."""
    buf = BytesIO()
    with WrappedWriter(buf) as writer:
        descriptor = descriptor_pb2.FileDescriptorProto.DESCRIPTOR
        fds = build_file_descriptor_set(descriptor)
        original_schema = Schema(
            id=42,
            name=descriptor.full_name,
            encoding=SchemaEncoding.Protobuf,
            data=fds.SerializeToString(),
        )
        registered = writer.register_schema(original_schema)

    assert registered.name == original_schema.name
    assert registered.encoding == original_schema.encoding
    assert registered.data == original_schema.data
