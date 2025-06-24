from typing import Any

from pydantic import BaseModel
from scipy.spatial.transform import Rotation

from kappe.utils.settings import SettingRotation, SettingTranslation
from kappe.writer import WrappedDecodedMessage

TF_SCHEMA_NAME = 'tf2_msgs/msg/TFMessage'
TF_SCHEMA_TEXT = """
geometry_msgs/TransformStamped[] transforms

================================================================================
MSG: geometry_msgs/TransformStamped
# This expresses a transform from coordinate frame header.frame_id
# to the coordinate frame child_frame_id at the time of header.stamp
#
# This message is mostly used by the
# <a href="https://index.ros.org/p/tf2/">tf2</a> package.
# See its documentation for more information.
#
# The child_frame_id is necessary in addition to the frame_id
# in the Header to communicate the full reference for the transform
# in a self contained message.

# The frame id in the header is used as the reference frame of this transform.
std_msgs/Header header

# The frame id of the child frame to which this transform points.
string child_frame_id

# Translation and rotation in 3-dimensions of child_frame_id from header.frame_id.
Transform transform

================================================================================
MSG: geometry_msgs/Transform
# This represents the transform between two coordinate frames in free space.

Vector3 translation
Quaternion rotation

================================================================================
MSG: geometry_msgs/Quaternion
# This represents an orientation in free space in quaternion form.

float64 x 0
float64 y 0
float64 z 0
float64 w 1

================================================================================
MSG: geometry_msgs/Vector3
# This represents a vector in free space.

# This is semantically different than a point.
# A vector is always anchored at the origin.
# When a transform is applied to a vector, only the rotational component is applied.

float64 x
float64 y
float64 z

================================================================================
MSG: std_msgs/Header
# Standard metadata for higher-level stamped data types.
# This is generally used to communicate timestamped data
# in a particular coordinate frame.

# Two-integer timestamp that is expressed as seconds and nanoseconds.
builtin_interfaces/Time stamp

# Transform frame with which this data is associated.
string frame_id

================================================================================
MSG: builtin_interfaces/Time
# This message communicates ROS Time defined here:
# https://design.ros2.org/articles/clock_and_time.html

# The seconds component, valid over all int32 values.
int32 sec

# The nanoseconds component, valid in the range [0, 10e9).
uint32 nanosec
"""


class SettingTFInsert(BaseModel):
    """
    TF insert settings.

    :ivar frame_id: Frame ID of the new transform.
    :ivar child_frame_id: Child frame ID of the new transform.
    :ivar translation: Translation of the new transform.
    :ivar rotation: Rotation of the new transform.
    """

    frame_id: str
    child_frame_id: str
    translation: SettingTranslation | None = None
    rotation: SettingRotation = SettingRotation()


class SettingTFOffset(BaseModel):
    """
    TF offset settings.

    :ivar child_frame_id: Child frame ID to apply offset to.
    :ivar translation: Translation offset to apply.
    :ivar rotation: Rotation offset to apply.
    """

    child_frame_id: str
    translation: SettingTranslation | None = None
    rotation: SettingRotation = SettingRotation()


class SettingTF(BaseModel):
    """
    TF settings.

    :ivar remove: List of child frame IDs to remove, or "all" to remove all transforms.
    :ivar insert: List of transforms to insert.
    :ivar offset: List of transforms to apply offsets to.
    """

    remove: list[str] | str | None = None
    remove_tf_static: bool = False
    insert: list[SettingTFInsert] | None = None
    offset: list[SettingTFOffset] | None = None


def tf_static_insert(cfg: SettingTF, stamp_ns: int) -> None | Any:
    if cfg.insert is None:
        return None

    sec = int(stamp_ns / 1e9)
    nanosec = int(stamp_ns % 1e9)

    transforms = []

    for insert in cfg.insert:
        trans = {}
        translation = insert.translation
        if translation is not None:
            trans['translation'] = {
                'x': translation.x,
                'y': translation.y,
                'z': translation.z,
            }

        rot_quat = insert.rotation.to_quaternion()
        if rot_quat is not None:
            trans['rotation'] = {
                'x': rot_quat[0],
                'y': rot_quat[1],
                'z': rot_quat[2],
                'w': rot_quat[3],
            }

        tf_msg = {
            'header': {
                'frame_id': insert.frame_id,
                'stamp': {
                    'sec': sec,
                    'nanosec': nanosec,
                },
            },
            'child_frame_id': insert.child_frame_id,
            'transform': trans,
        }
        transforms.append(tf_msg)

    return {'transforms': transforms}


def tf_apply_offset(cfg: SettingTF, msg: WrappedDecodedMessage) -> None:
    """Apply translation and rotation offsets to specified transforms."""
    if cfg.offset is None:
        return

    ros_msg = msg.decoded_message

    # Create a mapping of child_frame_id to offset settings
    offset_map = {offset.child_frame_id: offset for offset in cfg.offset}

    for transform in ros_msg.transforms:
        if transform.child_frame_id in offset_map:
            offset_cfg = offset_map[transform.child_frame_id]

            # Apply translation offset
            if offset_cfg.translation is not None:
                transform.transform.translation.x += offset_cfg.translation.x
                transform.transform.translation.y += offset_cfg.translation.y
                transform.transform.translation.z += offset_cfg.translation.z

            # Apply rotation offset
            offset_quat = offset_cfg.rotation.to_quaternion()
            if offset_quat is not None:
                # Get current rotation as quaternion (x, y, z, w)
                current_quat = [
                    transform.transform.rotation.x,
                    transform.transform.rotation.y,
                    transform.transform.rotation.z,
                    transform.transform.rotation.w,
                ]

                # Use scipy to multiply quaternions
                current_rot = Rotation.from_quat(current_quat)
                offset_rot = Rotation.from_quat(offset_quat)
                result_rot = current_rot * offset_rot

                # Get the result quaternion and update the transform
                result_quat = result_rot.as_quat()
                transform.transform.rotation.x = result_quat[0]
                transform.transform.rotation.y = result_quat[1]
                transform.transform.rotation.z = result_quat[2]
                transform.transform.rotation.w = result_quat[3]


def tf_remove(cfg: SettingTF, msg: WrappedDecodedMessage) -> bool:
    ros_msg = msg.decoded_message

    # Apply offsets before any removal operations
    tf_apply_offset(cfg, msg)

    if cfg.remove:
        if isinstance(cfg.remove, str) and cfg.remove.lower() == 'all':
            ros_msg.transforms = []
        elif isinstance(cfg.remove, list):
            ros_msg.transforms = [
                tf for tf in ros_msg.transforms if tf.child_frame_id not in cfg.remove
            ]
        else:
            raise ValueError(f'Invalid value for remove: {cfg.remove}')
        return len(ros_msg.transforms) > 0
    return True
