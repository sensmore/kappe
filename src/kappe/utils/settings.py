from pydantic import BaseModel
from scipy.spatial.transform import Rotation


class AxisBound(BaseModel):
    """
    Axis bound settings.

    :ivar min: Minimum bound.
    :ivar max: Maximum bound.
    """

    min: float = 0.0
    max: float = 0.0


class SettingEgoBounds(BaseModel):
    """
    Ego bounds settings.

    :ivar x: X axis bound.
    :ivar y: Y axis bound.
    :ivar z: Z axis bound.
    """

    x: AxisBound = AxisBound()
    y: AxisBound = AxisBound()
    z: AxisBound = AxisBound()


class SettingRotation(BaseModel):
    """
    Rotation settings.

    If booth quaternion and euler_deg are set, quaternion is used.

    :ivar quaternion: Quaternion to apply.
    :ivar euler_deg: Euler angles to apply.
    """

    quaternion: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 1.0)

    @property
    def euler_deg(self) -> tuple[float, float, float]:
        return Rotation.from_quat(self.quaternion).as_euler('xyz', degrees=True)

    @euler_deg.setter
    def euler_deg(self, value: tuple[float, float, float]) -> None:
        self.quaternion = Rotation.from_euler('xyz', value, degrees=True).as_quat()


class SettingTranslation(BaseModel):
    """
    Translation settings.

    :ivar x: Translation in x direction.
    :ivar y: Translation in y direction.
    :ivar z: Translation in z direction.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
