import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor

from kappe.plugin import ConverterPlugin, ProtobufConverterPlugin, load_plugin, module_get_plugins


class _OneConverterPlugin(ConverterPlugin):
    @property
    def output_schema(self) -> str:
        return 'test_schema'

    def convert(self, ros_msg: Any) -> Any:
        return {'converted': ros_msg}


class _AnotherTestPlugin(ConverterPlugin):
    @property
    def output_schema(self) -> str:
        return 'another_schema'

    def convert(self, ros_msg: Any) -> Any:
        return {'another': ros_msg}


def test_converter_plugin_abstract():
    """Test that ConverterPlugin is abstract and requires implementation."""
    with pytest.raises(TypeError):
        ConverterPlugin()


def test_converter_plugin_implementation():
    """Test that ConverterPlugin can be properly implemented."""
    plugin = _OneConverterPlugin()
    assert plugin.output_schema == 'test_schema'
    assert plugin.convert({'test': 'data'}) == {'converted': {'test': 'data'}}
    assert plugin.logger.name == '_OneConverterPlugin'


def test_module_get_plugins():
    """Test getting plugins from a module."""
    # Create a mock module with our test classes
    mock_module = MagicMock()
    mock_module.__dict__ = {
        'OneConverterPlugin': _OneConverterPlugin,
        'AnotherTestPlugin': _AnotherTestPlugin,
        'ConverterPlugin': ConverterPlugin,
        'SomeOtherClass': str,
        'not_a_class': 'string_value',
    }

    # Mock dir() to return the keys
    with (
        patch('kappe.plugin.dir', return_value=list(mock_module.__dict__.keys())),
        patch('kappe.plugin.getattr', side_effect=lambda _, k: mock_module.__dict__[k]),
    ):
        plugins = module_get_plugins(mock_module)

    assert 'OneConverterPlugin' in plugins
    assert 'AnotherTestPlugin' in plugins
    assert 'ConverterPlugin' not in plugins  # Should be excluded
    assert 'SomeOtherClass' not in plugins  # Not a ConverterPlugin subclass
    assert 'not_a_class' not in plugins  # Not a class


def test_load_plugin_with_class_name(tmp_path: Path):
    """Test loading a plugin with explicit class name."""
    # Create a test plugin file
    plugin_file = tmp_path / 'test_plugin.py'
    plugin_file.write_text("""
from kappe.plugin import ConverterPlugin

class MyConverter(ConverterPlugin):
    @property
    def output_schema(self):
        return 'my_schema'

    def convert(self, ros_msg):
        return {'my_converted': ros_msg}
""")

    # Load the plugin
    plugin_class = load_plugin(tmp_path, 'test_plugin.MyConverter')
    plugin = plugin_class()

    assert plugin.output_schema == 'my_schema'
    assert isinstance(plugin.logger, logging.Logger)
    assert plugin.logger.name == 'MyConverter'
    assert plugin.convert({'test': 'data'}) == {'my_converted': {'test': 'data'}}
    assert isinstance(plugin, ConverterPlugin)
    assert plugin_class.__name__ == 'MyConverter'


def test_load_plugin_default_class_name(tmp_path: Path):
    """Test loading a plugin with default class name."""
    # Create a test plugin file
    plugin_file = tmp_path / 'test_plugin.py'
    plugin_file.write_text("""
from kappe.plugin import ConverterPlugin

class Converter(ConverterPlugin):
    @property
    def output_schema(self):
        return 'default_schema'

    def convert(self, ros_msg):
        return {'default_converted': ros_msg}
""")

    # Load the plugin
    plugin_class = load_plugin(tmp_path, 'test_plugin')
    plugin = plugin_class()

    # Test all plugin outputs for 100% coverage
    assert plugin.output_schema == 'default_schema'
    assert plugin.convert({'test': 'data'}) == {'default_converted': {'test': 'data'}}
    assert isinstance(plugin.logger, logging.Logger)
    assert plugin.logger.name == 'Converter'
    assert isinstance(plugin, ConverterPlugin)
    assert plugin_class.__name__ == 'Converter'


def test_load_plugin_file_not_found(tmp_path: Path):
    """Test loading a plugin that doesn't exist."""
    with pytest.raises(ValueError, match='Plugin file nonexistent does not exist'):
        load_plugin(tmp_path, 'nonexistent')


def test_load_plugin_import_error(tmp_path: Path):
    """Test loading a plugin with import errors."""
    # Create a plugin file with import errors
    plugin_file = tmp_path / 'broken_plugin.py'
    plugin_file.write_text("""
import nonexistent_module
from kappe.plugin import ConverterPlugin

class Converter(ConverterPlugin):
    pass
""")

    with pytest.raises(ValueError, match='Plugin file broken_plugin does not exist'):
        load_plugin(tmp_path, 'broken_plugin')


def test_load_plugin_missing_class(tmp_path: Path):
    """Test loading a plugin that doesn't have the expected class."""
    # Create a plugin file without the expected class
    plugin_file = tmp_path / 'no_class_plugin.py'
    plugin_file.write_text("""
from kappe.plugin import ConverterPlugin

class WrongName(ConverterPlugin):
    @property
    def output_schema(self):
        return 'wrong_schema'

    def convert(self, ros_msg):
        return ros_msg
""")

    with pytest.raises(ValueError, match='Plugin file no_class_plugin does not exist'):
        load_plugin(tmp_path, 'no_class_plugin')


def test_load_plugin_from_builtin_plugins():
    """Test loading plugins from the built-in plugins directory."""
    # This test depends on the actual plugin directory structure
    # We'll mock the path to test the logic
    with patch('kappe.plugin.Path') as mock_path:
        mock_builtin_path = MagicMock()
        mock_builtin_path.exists.return_value = True
        mock_path.return_value.parent = MagicMock()
        mock_path.return_value.parent.__truediv__.return_value = mock_builtin_path

        plugin_file = mock_builtin_path.__truediv__.return_value
        plugin_file.exists.return_value = True

        with (
            patch('kappe.plugin.SourceFileLoader') as mock_loader,
            patch('kappe.plugin.spec_from_loader') as mock_spec_from_loader,
            patch('kappe.plugin.module_from_spec') as mock_module_from_spec,
        ):
            mock_spec = MagicMock()
            mock_spec.loader = mock_loader.return_value
            mock_spec_from_loader.return_value = mock_spec
            mock_module = MagicMock()
            mock_module.Converter = _OneConverterPlugin
            mock_module_from_spec.return_value = mock_module

            plugin_class = load_plugin(None, 'test_plugin')
            assert plugin_class == _OneConverterPlugin
            assert plugin_class.__name__ == '_OneConverterPlugin'


def test_load_plugin_spec_creation_failure(tmp_path: Path):
    """Test handling when spec_from_loader returns None."""
    plugin_file = tmp_path / 'spec_fail_plugin.py'
    plugin_file.write_text("""
from kappe.plugin import ConverterPlugin

class Converter(ConverterPlugin):
    @property
    def output_schema(self):
        return 'test_schema'

    def convert(self, ros_msg):
        return ros_msg
""")

    with patch('kappe.plugin.spec_from_loader') as mock_spec_from_loader:
        mock_spec_from_loader.return_value = None

        with pytest.raises(ValueError, match='Plugin file spec_fail_plugin does not exist'):
            load_plugin(tmp_path, 'spec_fail_plugin')


def test_load_plugin_spec_loader_none(tmp_path: Path):
    """Test handling when spec.loader is None."""
    plugin_file = tmp_path / 'loader_none_plugin.py'
    plugin_file.write_text("""
from kappe.plugin import ConverterPlugin

class Converter(ConverterPlugin):
    @property
    def output_schema(self):
        return 'test_schema'

    def convert(self, ros_msg):
        return ros_msg
""")

    with patch('kappe.plugin.spec_from_loader') as mock_spec_from_loader:
        mock_spec = MagicMock()
        mock_spec.loader = None
        mock_spec_from_loader.return_value = mock_spec

        with pytest.raises(ValueError, match='Plugin file loader_none_plugin does not exist'):
            load_plugin(tmp_path, 'loader_none_plugin')


class _ConcreteProtobufPlugin(ProtobufConverterPlugin):
    """Concrete protobuf converter plugin for testing."""

    @property
    def descriptor(self) -> Descriptor:
        return descriptor_pb2.FileDescriptorProto.DESCRIPTOR

    def convert(self, ros_msg: Any) -> Any:
        return ros_msg


def test_protobuf_converter_plugin_abstract():
    """Test that ProtobufConverterPlugin is abstract and requires descriptor implementation."""
    with pytest.raises(TypeError):
        ProtobufConverterPlugin()  # type: ignore[abstract]


def test_protobuf_converter_plugin_output_schema():
    """Test that ProtobufConverterPlugin.output_schema returns descriptor full_name."""
    plugin = _ConcreteProtobufPlugin()
    assert plugin.output_schema == descriptor_pb2.FileDescriptorProto.DESCRIPTOR.full_name
    assert plugin.output_schema == 'google.protobuf.FileDescriptorProto'


def test_protobuf_converter_plugin_is_subclass():
    """Test that ProtobufConverterPlugin is a subclass of ConverterPlugin."""
    assert issubclass(ProtobufConverterPlugin, ConverterPlugin)
    plugin = _ConcreteProtobufPlugin()
    assert isinstance(plugin, ConverterPlugin)
    assert isinstance(plugin, ProtobufConverterPlugin)


def test_protobuf_converter_plugin_convert_with_times():
    """Test that convert_with_times delegates to convert by default."""
    plugin = _ConcreteProtobufPlugin()
    msg = {'test': 'data'}
    result = plugin.convert_with_times(msg, log_time_ns=100, publish_time_ns=200)
    assert result == msg


def test_converter_plugin_convert_with_times_default():
    """Test that ConverterPlugin.convert_with_times calls convert by default."""
    plugin = _OneConverterPlugin()
    msg = {'test': 'data'}
    result = plugin.convert_with_times(msg, log_time_ns=100, publish_time_ns=200)
    assert result == {'converted': msg}


def test_module_get_plugins_excludes_abstract_classes():
    """Test that module_get_plugins excludes ProtobufConverterPlugin base class."""
    mock_module = MagicMock()
    mock_module.__dict__ = {
        'ConcreteProtobufPlugin': _ConcreteProtobufPlugin,
        'ConverterPlugin': ConverterPlugin,
        'ProtobufConverterPlugin': ProtobufConverterPlugin,
    }

    with (
        patch('kappe.plugin.dir', return_value=list(mock_module.__dict__.keys())),
        patch('kappe.plugin.getattr', side_effect=lambda _, k: mock_module.__dict__[k]),
    ):
        plugins = module_get_plugins(mock_module)

    assert 'ConcreteProtobufPlugin' in plugins
    assert 'ConverterPlugin' not in plugins
    assert 'ProtobufConverterPlugin' not in plugins
