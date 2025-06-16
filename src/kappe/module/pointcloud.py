import numpy as np
from pointcloud2 import create_cloud, read_points
from pydantic import BaseModel
from scipy.spatial.transform import Rotation

from kappe.utils.settings import SettingEgoBounds, SettingRotation
from kappe.writer import WrappedDecodedMessage


class SettingPointCloud(BaseModel):
    """
    Point cloud settings.

    :ivar remove_zero: Remove points with all zero coordinates (x, y, z).
    :ivar ego_bounds: Ego bounds to remove points from.
    :ivar rotation: Rotation to apply to point cloud.
    :ivar field_mapping: Mapping of point cloud field names to a new name.
    """

    remove_zero: bool = False
    ego_bounds: SettingEgoBounds | None = None
    rotation: SettingRotation = SettingRotation()
    field_mapping: dict[str, str] | None = None


def point_cloud(cfg: SettingPointCloud, msg: WrappedDecodedMessage) -> None:
    ros_msg = msg.decoded_message
    if cfg.field_mapping is not None:
        for pc_field in ros_msg.fields:
            pc_field.name = cfg.field_mapping.get(pc_field.name, pc_field.name)

    fields = [x.name for x in ros_msg.fields]
    if 'x' in fields and 'y' in fields and 'z' in fields:
        cloud = np.array(read_points(ros_msg))
        org_len = len(cloud)

        if cfg.remove_zero:
            cloud = cloud[np.logical_and(cloud['x'] != 0.0, cloud['y'] != 0.0, cloud['z'] != 0.0)]

        if cfg.ego_bounds is not None:
            # Create individual boolean masks for each condition
            x_mask = np.logical_and(
                cloud['x'] < cfg.ego_bounds.x.max, cloud['x'] > cfg.ego_bounds.x.min
            )
            y_mask = np.logical_and(
                cloud['y'] < cfg.ego_bounds.y.max, cloud['y'] > cfg.ego_bounds.y.min
            )
            z_mask = np.logical_and(
                cloud['z'] < cfg.ego_bounds.z.max, cloud['z'] > cfg.ego_bounds.z.min
            )

            # Combine all masks - keep points OUTSIDE the ego bounds
            ego_mask = np.logical_not(np.logical_and(np.logical_and(x_mask, y_mask), z_mask))
            cloud = cloud[ego_mask]

        quat = cfg.rotation.to_quaternion()
        if quat is not None:
            rot = Rotation.from_quat(np.array(quat))
            stack = np.column_stack([cloud['x'], cloud['y'], cloud['z']])

            r_cloud = rot.apply(stack)

            cloud['x'] = r_cloud[:, 0]
            cloud['y'] = r_cloud[:, 1]
            cloud['z'] = r_cloud[:, 2]

        if quat is not None or len(cloud) != org_len:
            msg_cloud = create_cloud(
                ros_msg.header,
                ros_msg.fields,
                cloud,
                ros_msg.point_step,
            )
            ros_msg.data = msg_cloud.data

            ros_msg.height = msg_cloud.height
            ros_msg.width = msg_cloud.width
            ros_msg.is_dense = msg_cloud.is_dense
            ros_msg.is_bigendian = msg_cloud.is_bigendian
            ros_msg.point_step = msg_cloud.point_step
            ros_msg.row_step = msg_cloud.row_step
