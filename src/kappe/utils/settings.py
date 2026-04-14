from pydantic import BaseModel, model_validator

from kappe.utils.rotation import euler_to_quaternion


class AxisBound(BaseModel, frozen=True):
    """
    Axis bound settings.

    :ivar min: Minimum bound.
    :ivar max: Maximum bound.
    """

    min: float = 0.0
    max: float = 0.0


class SettingEgoBounds(BaseModel, frozen=True):
    """
    Ego bounds settings.

    :ivar x: X axis bound.
    :ivar y: Y axis bound.
    :ivar z: Z axis bound.
    """

    x: AxisBound = AxisBound()
    y: AxisBound = AxisBound()
    z: AxisBound = AxisBound()


class SettingRotation(BaseModel, frozen=True):
    """
    Rotation settings.

    If both quaternion and euler_deg are set, quaternion is used.

    :ivar quaternion: Quaternion to apply.
    :ivar euler_deg: Euler angles to apply.
    """

    quaternion: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 1.0)
    euler_deg: tuple[float, float, float] | None = None

    @model_validator(mode='before')
    @classmethod
    def _check_mutual_exclusive(cls, data: dict) -> dict:
        """
        Ensure that either 'quaternion' or 'euler_deg' is provided, but not both.
        """
        q = data.get('quaternion')
        e = data.get('euler_deg')

        q_supplied = q is not None and q != (0.0, 0.0, 0.0, 1.0)
        e_supplied = e is not None

        if q_supplied and e_supplied:
            raise ValueError("Provide either 'quaternion' or 'euler_deg', not both.")
        return data

    @model_validator(mode='after')
    def _derive_quaternion(self) -> 'SettingRotation':
        if self.euler_deg is not None:
            quat = euler_to_quaternion(self.euler_deg, degrees=True)
            # model is frozen - bypass immutability for derived field
            object.__setattr__(self, 'quaternion', quat)
        return self


class SettingTranslation(BaseModel, frozen=True):
    """
    Translation settings.

    :ivar x: Translation in x direction.
    :ivar y: Translation in y direction.
    :ivar z: Translation in z direction.
    """

    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
