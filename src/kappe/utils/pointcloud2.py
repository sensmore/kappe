import array
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.lib.recfunctions import unstructured_to_structured

_DATATYPES: dict[int, np.dtype] = {}
_DATATYPES[1] = np.dtype(np.int8)
_DATATYPES[2] = np.dtype(np.uint8)
_DATATYPES[3] = np.dtype(np.int16)
_DATATYPES[4] = np.dtype(np.uint16)
_DATATYPES[5] = np.dtype(np.int32)
_DATATYPES[6] = np.dtype(np.uint32)
_DATATYPES[7] = np.dtype(np.float32)
_DATATYPES[8] = np.dtype(np.float64)


NP_TO_FIELD_TYPE: dict[Any, int] = {}
NP_TO_FIELD_TYPE[np.int8] = 1
NP_TO_FIELD_TYPE[np.uint8] = 2
NP_TO_FIELD_TYPE[np.int16] = 3
NP_TO_FIELD_TYPE[np.uint16] = 4
NP_TO_FIELD_TYPE[np.int32] = 5
NP_TO_FIELD_TYPE[np.uint32] = 6
NP_TO_FIELD_TYPE[np.float32] = 7
NP_TO_FIELD_TYPE[np.float64] = 8


@dataclass
class PointField:
    name: str
    offset: int
    datatype: int
    count: int = 1


DUMMY_FIELD_PREFIX = 'unnamed_field'


def dtype_from_fields(fields: Iterable[PointField], point_step: int | None = None) -> np.dtype:
    """
    Convert a Iterable of sensor_msgs.msg.PointField messages to a np.dtype.

    :param fields: The point cloud fields.
                   (Type: iterable of sensor_msgs.msg.PointField)
    :returns: NumPy datatype
    """
    # Create a lists containing the names, offsets and datatypes of all fields
    field_names: list[str] = []
    field_offsets: list[int] = []
    field_datatypes: list[str] = []
    for i, field in enumerate(fields):
        # Datatype as numpy datatype
        datatype = _DATATYPES[field.datatype]
        # Name field
        name = f'{DUMMY_FIELD_PREFIX}_{i}' if not field.name else field.name
        # Handle fields with count > 1 by creating subfields with a suffix consisting
        # of '_' followed by the subfield counter [0 -> (count - 1)]
        assert field.count > 0, "Can't process fields with count = 0."
        for a in range(field.count):
            # Add suffix if we have multiple subfields
            subfield_name = f'{name}_{a}' if field.count > 1 else name
            assert subfield_name not in field_names, 'Duplicate field names are not allowed!'
            field_names.append(subfield_name)
            # Create new offset that includes subfields
            field_offsets.append(field.offset + a * datatype.itemsize)
            field_datatypes.append(datatype.str)

    # Create a tuple for each field containing name and data type
    dtype_dict = {
        'names': field_names,
        'formats': field_datatypes,
        'offsets': field_offsets,
    }
    if point_step is not None:
        dtype_dict['itemsize'] = point_step
    return np.dtype(dtype_dict)


def fields_from_dtype(dtype: np.dtype) -> list[PointField]:

    fields = []
    for name, (dt, offset) in dtype.fields.items():
        fields.append(PointField(name, offset, NP_TO_FIELD_TYPE[dt.type]))

    return fields


def read_points(
        cloud: Any,
        field_names: list[str] | None = None,
        *,
        skip_nans: bool = False,
        uvs: Iterable | None = None,
        reshape_organized_cloud: bool = False) -> np.ndarray:
    """
    Read points from a sensor_msgs.PointCloud2 message.

    :param cloud: The point cloud to read from sensor_msgs.PointCloud2.
    :param field_names: The names of fields to read. If None, read all fields.
                        (Type: Iterable, Default: None)
    :param skip_nans: If True, then don't return any point with a NaN value.
                      (Type: Bool, Default: False)
    :param uvs: If specified, then only return the points at the given
        coordinates. (Type: Iterable, Default: None)
    :param reshape_organized_cloud: Returns the array as an 2D organized point cloud if set.
    :return: Structured NumPy array containing all points.
    """
    points = np.ndarray(
        shape=(cloud.width * cloud.height,),
        dtype=dtype_from_fields(cloud.fields, point_step=cloud.point_step),
        buffer=cloud.data)

    # Keep only the requested fields
    if field_names is not None:
        assert all(field_name in points.dtype.names for field_name in field_names), \
            'Requests field is not in the fields of the PointCloud!'
        # Mask fields
        points = points[list(field_names)]

    # Swap array if byte order does not match
    if bool(sys.byteorder != 'little') != bool(cloud.is_bigendian):
        points = points.byteswap()

    # Check if we want to drop points with nan values
    if skip_nans and not cloud.is_dense:
        # Init mask which selects all points
        not_nan_mask = np.ones(len(points), dtype=bool)
        for field_name in points.dtype.names:
            # Only keep points without any non values in the mask
            not_nan_mask = np.logical_and(
                not_nan_mask, ~np.isnan(points[field_name]))
        # Select these points
        points = points[not_nan_mask]

    # Select points indexed by the uvs field
    if uvs is not None:
        # Don't convert to numpy array if it is already one
        if not isinstance(uvs, np.ndarray):
            uvs = np.fromiter(uvs, int)
        # Index requested points
        points = points[uvs]

    # Cast into 2d array if cloud is 'organized'
    if reshape_organized_cloud and cloud.height > 1:
        points = points.reshape(cloud.width, cloud.height)

    return points


def create_cloud(
        header: Any,
        fields: Iterable,
        points: np.ndarray,
        step: int | None = None) -> Any:
    """
    Create a sensor_msgs.msg.PointCloud2 message.

    :param header: The point cloud header. (Type: std_msgs.msg.Header)
    :param fields: The point cloud fields.
                   (Type: iterable of sensor_msgs.msg.PointField)
    :param points: The point cloud points. List of iterables, i.e. one iterable
                   for each point, with the elements of each iterable being the
                   values of the fields for that point (in the same order as
                   the fields parameter)
    :return: The point cloud as sensor_msgs.msg.PointCloud2.
    """
    # Check if input is numpy array
    if isinstance(points, np.ndarray):
        # Check if this is an unstructured array
        if points.dtype.names is None:
            assert all(fields[0].datatype == field.datatype for field in fields[1:]), \
                'All fields need to have the same datatype. Pass a structured NumPy array \
                    with multiple dtypes otherwise.'
            # Convert unstructured to structured array
            points = unstructured_to_structured(
                points,
                dtype=dtype_from_fields(fields, point_step=step))
        else:
            assert points.dtype == dtype_from_fields(fields, point_step=step), \
                'PointFields and structured NumPy array dtype do not match for all fields! \
                    Check their field order, names and types.'
    else:
        # Cast python objects to structured NumPy array (slow)
        points = np.array(
            # Points need to be tuples in the structured array
            list(map(tuple, points)),
            dtype=dtype_from_fields(fields, point_step=step))

    # Handle organized clouds
    assert len(points.shape) <= 2, \
        'Too many dimensions for organized cloud! \
            Points can only be organized in max. two dimensional space'
    height = 1
    width = points.shape[0]
    # Check if input points are an organized cloud (2D array of points)
    if len(points.shape) == 2:
        height = points.shape[1]

    # Convert numpy points to array.array
    memory_view = memoryview(points)
    casted = memory_view.cast('B')
    array_array = array.array('B')
    array_array.frombytes(casted)

    # Put everything together
    cloud = {
        'header': header,
        'height': height,
        'width': width,
        'is_dense': False,
        'is_bigendian': sys.byteorder != 'little',
        'fields': fields,
        'point_step': points.dtype.itemsize,
        'row_step': (points.dtype.itemsize * width),
    }
    # Set cloud via property instead of the constructor because of the bug described in
    # https://github.com/ros2/common_interfaces/issues/176
    cloud['data'] = np.array(array_array, dtype=np.uint8).tobytes()
    return cloud
