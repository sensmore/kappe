from io import BytesIO
from pathlib import Path

import pytest

from kappe.settings import ROS2Distro
from kappe.utils.msg_def import _download, get_message_definition


@pytest.mark.parametrize(
    'url',
    [
        pytest.param('http://example.com/file.zip', id='invalid_domain'),
        pytest.param('http://github.com/user/repo/file.zip', id='invalid_protocol'),
        pytest.param('https://evil.com/malicious.zip', id='malicious_url'),
        pytest.param('ftp://github.com/file.zip', id='ftp_protocol'),
        pytest.param('file:///local/file.zip', id='file_protocol'),
        pytest.param('https://github.evil.com/file.zip', id='subdomain_attack'),
        pytest.param('https://notgithub.com/file.zip', id='different_domain'),
    ],
)
def test_download_various_invalid_urls(url: str):
    """Test _download with various invalid URL patterns."""
    buffer = BytesIO()
    with pytest.raises(ValueError, match='must start with'):
        _download(url, buffer)


def test_get_message_definition_nonexistent():
    """Test getting a non-existent message definition returns None."""
    result = get_message_definition('nonexistent_pkg/msg/NonExistentMessage', ROS2Distro.HUMBLE)
    assert result is None


def test_get_message_definition_with_dependencies():
    """Test getting a message with dependencies includes them."""
    # Header has a dependency on builtin_interfaces/Time
    result = get_message_definition('std_msgs/msg/Header', ROS2Distro.HUMBLE)
    assert result is not None
    assert 'builtin_interfaces/Time stamp' in result


@pytest.mark.parametrize(
    'distro',
    [
        pytest.param(ROS2Distro.HUMBLE, id='humble'),
        pytest.param(ROS2Distro.JAZZY, id='jazzy'),
    ],
)
def test_get_message_definition_different_distros(distro: ROS2Distro):
    """Test that message definitions work across different ROS2 distros."""
    result = get_message_definition('std_msgs/msg/String', distro)
    assert result is not None
    assert 'string data' in result


def test_get_message_definition_with_custom_folder(tmp_path: Path):
    """Test getting message definition from a custom folder."""
    # Create a custom message definition
    msg_dir = tmp_path / 'custom_pkg' / 'msg'
    msg_dir.mkdir(parents=True)

    msg_file = msg_dir / 'CustomMessage.msg'
    msg_file.write_text('int32 value\nstring name\n')

    # Should find the message in the custom folder
    result = get_message_definition('custom_pkg/CustomMessage', ROS2Distro.HUMBLE, folder=tmp_path)
    assert result is not None
    assert 'int32 value' in result
    assert 'string name' in result


def test_get_message_definition_custom_with_std_msgs_dep(tmp_path: Path):
    """Test custom message that references std_msgs resolves dependencies."""
    # Create a custom message that depends on std_msgs/Header
    msg_dir = tmp_path / 'custom_pkg' / 'msg'
    msg_dir.mkdir(parents=True)

    msg_file = msg_dir / 'CustomWithHeader.msg'
    msg_file.write_text('std_msgs/Header header\nstring name\n')

    result = get_message_definition(
        'custom_pkg/CustomWithHeader', ROS2Distro.HUMBLE, folder=tmp_path
    )
    assert result is not None
    # Custom message content
    assert 'std_msgs/Header header' in result
    assert 'string name' in result
    # Resolved dependency (std_msgs/Header definition)
    assert 'MSG: std_msgs/Header' in result
    assert 'builtin_interfaces/Time stamp' in result
