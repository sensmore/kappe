
from pydantic import BaseModel, Extra
from scipy.spatial.transform import Rotation


class SettingRotation(BaseModel, extra=Extra.forbid):
    """
    Rotation settings.

    If booth quaternion and euler_deg are set, quaternion is used.

    :ivar quaternion: Quaternion to apply.
    :ivar euler_deg: Euler angles to apply.
    """

    quaternion: tuple[float, float, float, float] | None = None
    euler_deg: tuple[float, float, float] | None = None

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
