import math

from pydantic import BaseModel


def euler_to_quaternion(rpy: tuple[float, float, float]) -> tuple[float, float, float, float]:
    roll = math.radians(rpy[0])
    pitch = math.radians(rpy[1])
    yaw = math.radians(rpy[2])

    cy = math.cos(yaw * 0.5)
    sy = math.sin(yaw * 0.5)
    cr = math.cos(roll * 0.5)
    sr = math.sin(roll * 0.5)
    cp = math.cos(pitch * 0.5)
    sp = math.sin(pitch * 0.5)

    w = cy * cr * cp + sy * sr * sp
    x = cy * sr * cp + sy * cr * sp
    y = cy * cr * sp - sy * sr * cp
    z = cr * cp * sy + cy * sr * sp

    return (x, y, z, w)


class SettingRotation(BaseModel):
    """
    Rotation settings.

    If booth quaternion and euler_deg are set, quaternion is used.

    :ivar quaternion: Quaternion to apply.
    :ivar euler_deg: Euler angles to apply.
    """

    quaternion: tuple[float, float, float, float] | None = None
    euler_deg: tuple[float, float, float] | None = None

    def to_quaternion(self) -> None | tuple[float, ...]:
        if self.quaternion:
            return self.quaternion

        if self.euler_deg:
            return euler_to_quaternion(self.euler_deg)

        return None


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
