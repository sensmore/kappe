from unittest.mock import MagicMock

import pytest

from kappe.module.tf import SettingTF, SettingTFOffset, tf_apply_offset
from kappe.utils.settings import SettingRotation, SettingTranslation
from kappe.writer import WrappedDecodedMessage


def test_setting_tf_offset_creation():
    """Test SettingTFOffset model creation with default values."""
    offset = SettingTFOffset(child_frame_id='test_frame')

    assert offset.child_frame_id == 'test_frame'
    assert offset.translation.x == 0.0
    assert offset.translation.y == 0.0
    assert offset.translation.z == 0.0
    assert offset.rotation.quaternion == (0.0, 0.0, 0.0, 1.0)


def test_setting_tf_offset_with_values():
    """Test SettingTFOffset model creation with custom values."""
    translation = SettingTranslation(x=1.0, y=2.0, z=3.0)
    rotation = SettingRotation(quaternion=(0.0, 0.0, 0.7071068, 0.7071068))

    offset = SettingTFOffset(
        child_frame_id='custom_frame', translation=translation, rotation=rotation
    )

    assert offset.child_frame_id == 'custom_frame'
    assert offset.translation.x == 1.0
    assert offset.translation.y == 2.0
    assert offset.translation.z == 3.0
    assert offset.rotation.quaternion == (0.0, 0.0, 0.7071068, 0.7071068)


def test_setting_tf_with_offset():
    """Test SettingTF model with offset configuration."""
    offset = SettingTFOffset(child_frame_id='test_frame')
    tf_setting = SettingTF(offset=[offset])

    assert tf_setting.offset is not None
    assert len(tf_setting.offset) == 1
    assert tf_setting.offset[0].child_frame_id == 'test_frame'


def test_tf_apply_offset_no_offset_config():
    """Test tf_apply_offset when no offset configuration is provided."""
    cfg = SettingTF()  # No offset configured
    msg = MagicMock()

    # Should not raise any errors and should not modify anything
    tf_apply_offset(cfg, msg)

    # msg should not have been accessed since there's no offset config
    assert not msg.decoded_message.called


def test_tf_apply_offset_translation_only():
    """Test tf_apply_offset with translation offset only."""
    # Create mock transform
    mock_transform = MagicMock()
    mock_transform.child_frame_id = 'test_frame'
    mock_transform.transform.translation.x = 1.0
    mock_transform.transform.translation.y = 2.0
    mock_transform.transform.translation.z = 3.0
    # Need to mock rotation attributes even for translation-only test
    mock_transform.transform.rotation.x = 0.0
    mock_transform.transform.rotation.y = 0.0
    mock_transform.transform.rotation.z = 0.0
    mock_transform.transform.rotation.w = 1.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configuration
    translation = SettingTranslation(x=0.5, y=0.1, z=0.2)
    offset = SettingTFOffset(child_frame_id='test_frame', translation=translation)
    cfg = SettingTF(offset=[offset])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that translation was modified
    assert mock_transform.transform.translation.x == 1.5  # 1.0 + 0.5
    assert mock_transform.transform.translation.y == 2.1  # 2.0 + 0.1
    assert mock_transform.transform.translation.z == 3.2  # 3.0 + 0.2


def test_tf_apply_offset_rotation_only():
    """Test tf_apply_offset with rotation offset only."""
    # Create mock transform with identity rotation
    mock_transform = MagicMock()
    mock_transform.child_frame_id = 'test_frame'
    mock_transform.transform.rotation.x = 0.0
    mock_transform.transform.rotation.y = 0.0
    mock_transform.transform.rotation.z = 0.0
    mock_transform.transform.rotation.w = 1.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configuration with 90-degree Z rotation
    rotation = SettingRotation(quaternion=(0.0, 0.0, 0.7071067811865476, 0.7071067811865476))
    offset = SettingTFOffset(child_frame_id='test_frame', rotation=rotation)
    cfg = SettingTF(offset=[offset])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that rotation was modified (should now be the offset rotation)
    assert abs(mock_transform.transform.rotation.z - 0.7071067811865476) < 1e-10
    assert abs(mock_transform.transform.rotation.w - 0.7071067811865476) < 1e-10


def test_tf_apply_offset_combined():
    """Test tf_apply_offset with both translation and rotation offset."""
    # Create mock transform
    mock_transform = MagicMock()
    mock_transform.child_frame_id = 'test_frame'
    mock_transform.transform.translation.x = 1.0
    mock_transform.transform.translation.y = 0.0
    mock_transform.transform.translation.z = 0.0
    mock_transform.transform.rotation.x = 0.0
    mock_transform.transform.rotation.y = 0.0
    mock_transform.transform.rotation.z = 0.0
    mock_transform.transform.rotation.w = 1.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configuration with both translation and rotation
    translation = SettingTranslation(x=0.1, y=0.2, z=0.3)
    rotation = SettingRotation(quaternion=(0.0, 0.0, 0.38268343236509, 0.9238795325113))
    offset = SettingTFOffset(
        child_frame_id='test_frame', translation=translation, rotation=rotation
    )
    cfg = SettingTF(offset=[offset])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that both translation and rotation were modified
    assert mock_transform.transform.translation.x == 1.1  # 1.0 + 0.1
    assert mock_transform.transform.translation.y == 0.2  # 0.0 + 0.2
    assert mock_transform.transform.translation.z == 0.3  # 0.0 + 0.3
    assert abs(mock_transform.transform.rotation.z - 0.38268343236509) < 1e-10
    assert abs(mock_transform.transform.rotation.w - 0.9238795325113) < 1e-10


def test_tf_apply_offset_multiple_transforms():
    """Test tf_apply_offset with multiple transforms, only affecting matching child_frame_id."""
    # Create mock transforms
    mock_transform1 = MagicMock()
    mock_transform1.child_frame_id = 'frame1'
    mock_transform1.transform.translation.x = 1.0
    mock_transform1.transform.translation.y = 2.0
    mock_transform1.transform.translation.z = 3.0
    mock_transform1.transform.rotation.x = 0.0
    mock_transform1.transform.rotation.y = 0.0
    mock_transform1.transform.rotation.z = 0.0
    mock_transform1.transform.rotation.w = 1.0

    mock_transform2 = MagicMock()
    mock_transform2.child_frame_id = 'frame2'
    mock_transform2.transform.translation.x = 4.0
    mock_transform2.transform.translation.y = 5.0
    mock_transform2.transform.translation.z = 6.0
    mock_transform2.transform.rotation.x = 0.0
    mock_transform2.transform.rotation.y = 0.0
    mock_transform2.transform.rotation.z = 0.0
    mock_transform2.transform.rotation.w = 1.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform1, mock_transform2]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configuration for only frame1
    translation = SettingTranslation(x=0.5, y=0.1, z=0.2)
    offset = SettingTFOffset(child_frame_id='frame1', translation=translation)
    cfg = SettingTF(offset=[offset])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that only frame1 was modified
    assert mock_transform1.transform.translation.x == 1.5  # Modified
    assert mock_transform1.transform.translation.y == 2.1  # Modified
    assert mock_transform1.transform.translation.z == 3.2  # Modified

    # frame2 should remain unchanged
    assert mock_transform2.transform.translation.x == 4.0  # Unchanged
    assert mock_transform2.transform.translation.y == 5.0  # Unchanged
    assert mock_transform2.transform.translation.z == 6.0  # Unchanged


def test_tf_apply_offset_multiple_offsets():
    """Test tf_apply_offset with multiple offset configurations."""
    # Create mock transforms
    mock_transform1 = MagicMock()
    mock_transform1.child_frame_id = 'frame1'
    mock_transform1.transform.translation.x = 1.0
    mock_transform1.transform.translation.y = 2.0
    mock_transform1.transform.translation.z = 3.0
    mock_transform1.transform.rotation.x = 0.0
    mock_transform1.transform.rotation.y = 0.0
    mock_transform1.transform.rotation.z = 0.0
    mock_transform1.transform.rotation.w = 1.0

    mock_transform2 = MagicMock()
    mock_transform2.child_frame_id = 'frame2'
    mock_transform2.transform.translation.x = 4.0
    mock_transform2.transform.translation.y = 5.0
    mock_transform2.transform.translation.z = 6.0
    mock_transform2.transform.rotation.x = 0.0
    mock_transform2.transform.rotation.y = 0.0
    mock_transform2.transform.rotation.z = 0.0
    mock_transform2.transform.rotation.w = 1.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform1, mock_transform2]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configurations for both frames
    translation1 = SettingTranslation(x=0.1, y=0.1, z=0.1)
    offset1 = SettingTFOffset(child_frame_id='frame1', translation=translation1)

    translation2 = SettingTranslation(x=0.2, y=0.2, z=0.2)
    offset2 = SettingTFOffset(child_frame_id='frame2', translation=translation2)

    cfg = SettingTF(offset=[offset1, offset2])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that both frames were modified with their respective offsets
    assert mock_transform1.transform.translation.x == 1.1  # 1.0 + 0.1
    assert mock_transform1.transform.translation.y == 2.1  # 2.0 + 0.1
    assert mock_transform1.transform.translation.z == 3.1  # 3.0 + 0.1

    assert mock_transform2.transform.translation.x == 4.2  # 4.0 + 0.2
    assert mock_transform2.transform.translation.y == 5.2  # 5.0 + 0.2
    assert mock_transform2.transform.translation.z == 6.2  # 6.0 + 0.2


def test_tf_apply_offset_no_matching_frame():
    """Test tf_apply_offset when no transforms match the configured child_frame_id."""
    # Create mock transform
    mock_transform = MagicMock()
    mock_transform.child_frame_id = 'different_frame'
    mock_transform.transform.translation.x = 1.0
    mock_transform.transform.translation.y = 2.0
    mock_transform.transform.translation.z = 3.0

    # Create mock ROS message
    mock_ros_msg = MagicMock()
    mock_ros_msg.transforms = [mock_transform]

    # Create mock wrapped message
    msg = MagicMock(spec=WrappedDecodedMessage)
    msg.decoded_message = mock_ros_msg

    # Create offset configuration for a frame that doesn't exist
    translation = SettingTranslation(x=0.5, y=0.1, z=0.2)
    offset = SettingTFOffset(child_frame_id='nonexistent_frame', translation=translation)
    cfg = SettingTF(offset=[offset])

    # Apply offset
    tf_apply_offset(cfg, msg)

    # Check that nothing was modified
    assert mock_transform.transform.translation.x == 1.0  # Unchanged
    assert mock_transform.transform.translation.y == 2.0  # Unchanged
    assert mock_transform.transform.translation.z == 3.0  # Unchanged


@pytest.mark.parametrize(
    ('euler_deg', 'expected_quat'),
    [
        pytest.param((0.0, 0.0, 0.0), (0.0, 0.0, 0.0, 1.0), id='identity'),
        pytest.param(
            (90.0, 0.0, 0.0), (0.7071067811865476, 0.0, 0.0, 0.7071067811865475), id='roll_90'
        ),
        pytest.param(
            (0.0, 90.0, 0.0), (0.0, 0.7071067811865476, 0.0, 0.7071067811865475), id='pitch_90'
        ),
        pytest.param(
            (0.0, 0.0, 90.0), (0.0, 0.0, 0.7071067811865476, 0.7071067811865475), id='yaw_90'
        ),
        pytest.param((180.0, 0.0, 0.0), (1.0, 0.0, 0.0, 0.0), id='roll_180'),
        pytest.param((0.0, 180.0, 0.0), (0.0, 1.0, 0.0, 0.0), id='pitch_180'),
        pytest.param((0.0, 0.0, 180.0), (0.0, 0.0, 1.0, 0.0), id='yaw_180'),
        pytest.param(
            (45.0, 0.0, 0.0), (0.3826834323650898, 0.0, 0.0, 0.9238795325112867), id='roll_45'
        ),
        pytest.param(
            (0.0, 45.0, 0.0), (0.0, 0.3826834323650898, 0.0, 0.9238795325112867), id='pitch_45'
        ),
        pytest.param(
            (0.0, 0.0, 45.0), (0.0, 0.0, 0.3826834323650898, 0.9238795325112867), id='yaw_45'
        ),
        pytest.param(
            (-180.0, 0.0, 93.8),
            (
                -0.6832737736807991,
                0.7301622766207524,
                4.4709544746087435e-17,
                4.183845199397618e-17,
            ),
            id='real_world_1',
        ),
        # Multi-axis rotations that differ significantly between extrinsic (xyz) and intrinsic (XYZ)
        # These test cases ensure we're using intrinsic rotations (XYZ) not extrinsic (xyz)
        pytest.param(
            (30.0, 45.0, 60.0),
            (0.3919038373291199, 0.20056212114657512, 0.5319756951821668, 0.7233174113647118),
            id='multi_axis_30_45_60',
        ),
        pytest.param(
            (90.0, 45.0, 30.0),
            (0.7010573846499778, 0.09229595564125731, 0.43045933457687935, 0.560985526796931),
            id='multi_axis_90_45_30',
        ),
        pytest.param((45.0, 90.0, 45.0), (0.5, 0.5, 0.5, 0.5), id='multi_axis_45_90_45'),
        pytest.param(
            (120.0, 60.0, 30.0),
            (0.7891491309924314, 0.04736717274537652, 0.5303300858899106, 0.3061862178478974),
            id='multi_axis_120_60_30',
        ),
        pytest.param(
            (15.0, 30.0, 45.0),
            (0.21467986690178759, 0.18882373494380095, 0.39769256879400694, 0.8718364368360203),
            id='multi_axis_15_30_45',
        ),
    ],
)
def test_setting_rotation_rpy_to_quaternion(
    euler_deg: tuple[float, float, float],
    expected_quat: tuple[float, float, float, float],
):
    """Test SettingRotation converts RPY (euler_deg) to quaternion correctly."""
    rotation = SettingRotation(euler_deg=euler_deg)
    actual = rotation.quaternion

    assert actual == pytest.approx(expected_quat, abs=1e-10)
