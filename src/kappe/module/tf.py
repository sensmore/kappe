import copy

from mcap.reader import DecodedMessageTuple
from pydantic import BaseModel, Extra

from kappe.utils.settings import SettingRotation, SettingTranslation
from kappe.utils.types import ClassDict


class SettingTFInsert(BaseModel, extra=Extra.forbid):
    """
    TF insert settings.

    :ivar frame_id: Frame ID of the new transform.
    :ivar child_frame_id: Child frame ID of the new transform.
    :ivar translation: Translation of the new transform.
    :ivar rotation: Rotation of the new transform.
    """

    frame_id: str
    child_frame_id: str
    translation: SettingTranslation | None
    rotation: SettingRotation = SettingRotation()


class SettingTF(BaseModel, extra=Extra.forbid):
    """
    TF settings.

    :ivar remove: List of child frame IDs to remove.
    :ivar insert: List of transforms to insert.
    """

    remove: list[str] | None = None
    insert: list[SettingTFInsert] | None = None


def tf_static_insert(cfg: SettingTF, msg: DecodedMessageTuple) -> bool:
    schema, channel, message, ros_msg = msg

    if cfg.insert is None:
        return True

    # One transform needed as reference
    if len(ros_msg.transforms) == 0:
        return False

    header = ros_msg.transforms[0].header

    for insert in cfg.insert:
        tf_msg = ClassDict(
            header=copy.deepcopy(header),
            child_frame_id=insert.child_frame_id,
        )

        tf_msg.header.frame_id = insert.frame_id

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

        tf_msg['transform'] = trans
        ros_msg.transforms.append(tf_msg)

    return True


def tf_remove(cfg: SettingTF, msg: DecodedMessageTuple):
    schema, channel, message, ros_msg = msg

    if cfg.remove:
        for transform in ros_msg.transforms:
            if transform.child_frame_id in cfg.remove:
                ros_msg.transforms.remove(transform)
