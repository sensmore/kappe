import logging
from typing import Any

from kappe.plugin import ConverterPlugin

logger = logging.getLogger(__name__)


class InsertCameraInfo(ConverterPlugin):
    def __init__(self, *, camera_info: dict[str, Any]) -> None:
        """Initialize the camera info plugin with the camera parameters.
        
        Args:
            camera_info: Dictionary containing the camera calibration parameters
                in ROS camera_info format. Only specified parameters will be updated.
        """
        super().__init__()
        self.camera_info = camera_info
        self.logger.debug('Loaded camera info for: %s', camera_info.get('camera_name', 'unnamed'))

    def convert(self, ros_msg: Any) -> Any:
        """Update calibration data in the camera_info message.
        
        Preserves all existing message fields and only updates calibration 
        parameters that are specified in the config.
        
        Args:
            ros_msg: Input ROS message (sensor_msgs/CameraInfo)
            
        Returns:
            Updated camera info message with new calibration parameters
        """
        # Start with a copy of the original message
        new_msg = {
            'header': ros_msg.header,
            'height': ros_msg.height,
            'width': ros_msg.width,
            'distortion_model': ros_msg.distortion_model,
            'd': ros_msg.d,
            'k': ros_msg.k,
            'r': ros_msg.r,
            'p': ros_msg.p,
            'binning_x': ros_msg.binning_x,
            'binning_y': ros_msg.binning_y,
            'roi': ros_msg.roi,
        }

        # Update only the fields that are specified in the config
        if 'image_height' in self.camera_info:
            new_msg['height'] = self.camera_info['image_height']
        if 'image_width' in self.camera_info:
            new_msg['width'] = self.camera_info['image_width']
        if 'distortion_model' in self.camera_info:
            new_msg['distortion_model'] = self.camera_info['distortion_model']

        # Update calibration matrices if specified
        if 'distortion_coefficients' in self.camera_info:
            new_msg['d'] = self.camera_info['distortion_coefficients']['data']
        if 'camera_matrix' in self.camera_info:
            new_msg['k'] = self.camera_info['camera_matrix']['data']
        if 'rectification_matrix' in self.camera_info:
            new_msg['r'] = self.camera_info['rectification_matrix']['data']
        if 'projection_matrix' in self.camera_info:
            new_msg['p'] = self.camera_info['projection_matrix']['data']

        return new_msg

    @property
    def output_schema(self) -> str:
        """Return the output message type."""
        return 'sensor_msgs/msg/CameraInfo' 
