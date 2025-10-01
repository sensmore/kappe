"""Tests comparing our rotation implementation with scipy."""

import numpy as np
import pytest
from scipy.spatial.transform import Rotation as ScipyRotation

from kappe.utils.rotation import euler_to_quaternion, quaternion_multiply, rotate_points


@pytest.mark.parametrize(
    'euler_deg',
    [
        (0.0, 0.0, 0.0),
        (90.0, 0.0, 0.0),
        (0.0, 90.0, 0.0),
        (0.0, 0.0, 90.0),
        (180.0, 0.0, 0.0),
        (0.0, 180.0, 0.0),
        (0.0, 0.0, 180.0),
        (45.0, 0.0, 0.0),
        (0.0, 45.0, 0.0),
        (0.0, 0.0, 45.0),
        (-180.0, 0.0, 93.8),
        (30.0, 45.0, 60.0),
        (90.0, 45.0, 30.0),
        (45.0, 90.0, 45.0),
        (120.0, 60.0, 30.0),
        (15.0, 30.0, 45.0),
        (-90.0, -45.0, -30.0),
        (123.45, 67.89, -12.34),
    ],
)
def test_euler_to_quaternion_matches_scipy(euler_deg: tuple[float, float, float]):
    """Test that our euler_to_quaternion matches scipy exactly."""
    # Our implementation
    our_quat = euler_to_quaternion(euler_deg, degrees=True)

    # Scipy implementation
    scipy_quat = ScipyRotation.from_euler('XYZ', euler_deg, degrees=True).as_quat()

    # Should match exactly
    assert our_quat == pytest.approx(tuple(scipy_quat), abs=1e-10)


@pytest.mark.parametrize(
    'euler_deg',
    [
        (30.0, 45.0, 60.0),
        (90.0, 45.0, 30.0),
        (45.0, 90.0, 45.0),
        (120.0, 60.0, 30.0),
        (15.0, 30.0, 45.0),
    ],
)
def test_euler_to_quaternion_uses_intrinsic_xyz_not_extrinsic(
    euler_deg: tuple[float, float, float],
):
    """Test that euler_to_quaternion uses intrinsic XYZ rotation order, not extrinsic xyz."""
    # Our implementation
    our_quat = euler_to_quaternion(euler_deg, degrees=True)

    # Scipy intrinsic XYZ (uppercase = intrinsic)
    scipy_intrinsic = ScipyRotation.from_euler('XYZ', euler_deg, degrees=True).as_quat()

    # Scipy extrinsic xyz (lowercase = extrinsic)
    scipy_extrinsic = ScipyRotation.from_euler('xyz', euler_deg, degrees=True).as_quat()

    # Our implementation should match intrinsic XYZ
    assert our_quat == pytest.approx(tuple(scipy_intrinsic), abs=1e-10)

    # Verify intrinsic and extrinsic are actually different for these test cases
    # (if they're the same, the test wouldn't prove anything)
    assert not np.allclose(scipy_intrinsic, scipy_extrinsic, atol=1e-10)


@pytest.mark.parametrize(
    ('q1', 'q2'),
    [
        # Identity * Identity
        ((0.0, 0.0, 0.0, 1.0), (0.0, 0.0, 0.0, 1.0)),
        # 90° Z rotation * 90° Z rotation = 180° Z rotation
        (
            (0.0, 0.0, 0.7071067811865476, 0.7071067811865475),
            (0.0, 0.0, 0.7071067811865476, 0.7071067811865475),
        ),
        # 90° X rotation * 90° Y rotation
        (
            (0.7071067811865476, 0.0, 0.0, 0.7071067811865475),
            (0.0, 0.7071067811865476, 0.0, 0.7071067811865475),
        ),
        # Complex case: 45° around X * 30° around Y
        (
            (0.38268343236508984, 0.0, 0.0, 0.9238795325112867),
            (0.0, 0.25881904510252074, 0.0, 0.9659258262890683),
        ),
    ],
)
def test_quaternion_multiply_matches_scipy(
    q1: tuple[float, float, float, float], q2: tuple[float, float, float, float]
):
    """Test that our quaternion_multiply matches scipy exactly."""
    # Our implementation
    our_result = quaternion_multiply(q1, q2)

    # Scipy implementation
    scipy_r1 = ScipyRotation.from_quat(q1)
    scipy_r2 = ScipyRotation.from_quat(q2)
    scipy_result = (scipy_r1 * scipy_r2).as_quat()

    # Should match exactly
    assert our_result == pytest.approx(tuple(scipy_result), abs=1e-10)


@pytest.mark.parametrize(
    ('points', 'quat'),
    [
        # Single point at origin
        (np.array([[0.0, 0.0, 0.0]]), (0.0, 0.0, 0.0, 1.0)),
        # Single point on X axis, identity rotation
        (np.array([[1.0, 0.0, 0.0]]), (0.0, 0.0, 0.0, 1.0)),
        # Single point on X axis, 90° Z rotation
        (np.array([[1.0, 0.0, 0.0]]), (0.0, 0.0, 0.7071067811865476, 0.7071067811865475)),
        # Multiple points, 90° X rotation
        (
            np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]),
            (0.7071067811865476, 0.0, 0.0, 0.7071067811865475),
        ),
        # Random points with normalized quaternion
        (
            np.array([[1.5, 2.3, -0.8], [-1.2, 0.7, 3.4], [0.0, 0.0, 0.0]]),
            (0.10259783520851541, 0.20519567041703082, 0.3077935056255462, 0.9233805168766387),
        ),
    ],
)
def test_rotate_points_matches_scipy(points: np.ndarray, quat: tuple[float, float, float, float]):
    """Test that our rotate_points matches scipy exactly."""
    # Our implementation
    our_result = rotate_points(points, quat)

    # Scipy implementation
    scipy_rot = ScipyRotation.from_quat(quat)
    scipy_result = scipy_rot.apply(points)

    # Should match exactly
    np.testing.assert_allclose(our_result, scipy_result, atol=1e-10)
