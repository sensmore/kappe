import pytest

from kappe.utils.settings import SettingRotation


def test_setting_rotation_both_quaternion_and_euler():
    """Test SettingRotation raises error when both quaternion and euler_deg are provided."""
    with pytest.raises(ValueError, match="Provide either 'quaternion' or 'euler_deg', not both"):
        SettingRotation(quaternion=(0.0, 0.0, 0.7071068, 0.7071068), euler_deg=(0.0, 0.0, 90.0))
