"""Rotation utilities using numpy for 3D transformations."""

import numpy as np


def euler_to_quaternion(
    euler_angles: tuple[float, float, float], *, degrees: bool = True
) -> tuple[float, float, float, float]:
    """
    Convert Euler angles (XYZ intrinsic) to quaternion (x, y, z, w).

    :param euler_angles: Tuple of (roll, pitch, yaw) angles.
    :param degrees: If True, input angles are in degrees; otherwise radians.
    :return: Quaternion as (x, y, z, w).
    """
    roll, pitch, yaw = euler_angles

    if degrees:
        roll = np.radians(roll)
        pitch = np.radians(pitch)
        yaw = np.radians(yaw)

    # Compute half angles
    cr = np.cos(roll * 0.5)
    sr = np.sin(roll * 0.5)
    cp = np.cos(pitch * 0.5)
    sp = np.sin(pitch * 0.5)
    cy = np.cos(yaw * 0.5)
    sy = np.sin(yaw * 0.5)

    # XYZ intrinsic rotation order
    # Formula derived from composition of rotation matrices
    w = cr * cp * cy - sr * sp * sy
    x = sr * cp * cy + cr * sp * sy
    y = cr * sp * cy - sr * cp * sy
    z = cr * cp * sy + sr * sp * cy

    return (float(x), float(y), float(z), float(w))


def quaternion_multiply(
    q1: tuple[float, float, float, float] | list[float],
    q2: tuple[float, float, float, float] | list[float],
) -> tuple[float, float, float, float]:
    """
    Multiply two quaternions (q1 * q2).

    Quaternions are in (x, y, z, w) format.
    Uses Hamilton product formula.

    :param q1: First quaternion (x, y, z, w).
    :param q2: Second quaternion (x, y, z, w).
    :return: Result quaternion (x, y, z, w).
    """
    x1, y1, z1, w1 = q1
    x2, y2, z2, w2 = q2

    # Hamilton product
    w = w1 * w2 - x1 * x2 - y1 * y2 - z1 * z2
    x = w1 * x2 + x1 * w2 + y1 * z2 - z1 * y2
    y = w1 * y2 + y1 * w2 + z1 * x2 - x1 * z2
    z = w1 * z2 + z1 * w2 + x1 * y2 - y1 * x2

    return (float(x), float(y), float(z), float(w))


def rotate_points(
    points: np.ndarray, quaternion: tuple[float, float, float, float] | list[float]
) -> np.ndarray:
    """
    Apply quaternion rotation to an array of 3D points.

    :param points: Nx3 numpy array of points.
    :param quaternion: Rotation quaternion (x, y, z, w).
    :return: Rotated points as Nx3 numpy array.
    """
    # Ensure points are float64
    points = np.asarray(points, dtype=np.float64)

    x, y, z, w = quaternion

    # Precompute repeated terms for efficiency
    xx = x * x
    yy = y * y
    zz = z * z
    xy = x * y
    xz = x * z
    yz = y * z
    wx = w * x
    wy = w * y
    wz = w * z

    # Rotation matrix from quaternion (Hamilton convention)
    # Matrix for rotating vectors (not coordinate frames)
    # fmt: off
    rot_matrix = np.array([
        [1 - 2*(yy + zz),     2*(xy - wz),     2*(xz + wy)],
        [    2*(xy + wz), 1 - 2*(xx + zz),     2*(yz - wx)],
        [    2*(xz - wy),     2*(yz + wx), 1 - 2*(xx + yy)],
    ], dtype=np.float64)
    # fmt: on

    # Apply rotation: (3x3) @ (3xN) = (3xN), then transpose to (Nx3)
    return (rot_matrix @ points.T).T
