from typing import Annotated

from pydantic import BaseModel, Extra, conlist
from scipy.spatial.transform import Rotation


class SettingRotation(BaseModel, extra=Extra.forbid):
    """
    Rotation settings.

    If booth quaternion and euler_deg are set, quaternion is used.

    :ivar quaternion: Quaternion to apply.
    :ivar euler_deg: Euler angles to apply.
    """

    quaternion: Annotated[list[float] | None,
                          conlist(float, min_items=4, max_items=4)] = None
    euler_deg: Annotated[list[float] | None,
                         conlist(float, min_items=3, max_items=3)] = None

    def to_quaternion(self) -> None | list[float]:
        if self.quaternion:
            return self.quaternion

        if self.euler_deg:
            return Rotation.from_euler(
                'xyz', self.euler_deg, degrees=True).as_quat()

        return None


class SettingTranslation(BaseModel, extra=Extra.forbid):
    """
    Translation settings.

    :ivar x: Translation in x direction.
    :ivar y: Translation in y direction.
    :ivar z: Translation in z direction.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
