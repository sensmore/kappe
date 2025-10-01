from unittest.mock import MagicMock

import pytest

from kappe.plugins.camera_info import InsertCameraInfo


def test_insert_camera_info_missing_header():
    """Test InsertCameraInfo raises error when input message lacks header attribute."""
    # Create plugin
    plugin = InsertCameraInfo(
        camera_info={
            'image_height': 480,
            'image_width': 640,
            'distortion_model': 'plumb_bob',
        }
    )

    # Create mock message without header attribute
    mock_msg = MagicMock(spec=[])  # Empty spec means no attributes

    # Should raise AttributeError
    with pytest.raises(AttributeError, match="Input message must have a 'header' attribute"):
        plugin.convert(mock_msg)
