import logging
from typing import Any

from kappe.plugin import ConverterPlugin

logger = logging.getLogger(__name__)


class UpdateCameraInfo(ConverterPlugin):
    """Plugin to update existing camera calibration parameters in CameraInfo messages.

    This plugin modifies existing sensor_msgs/CameraInfo messages by updating
    only the calibration parameters specified in the configuration, preserving
    all other message fields.
    """

    def __init__(self, *, camera_info: dict[str, Any]) -> None:
        """Initialize the camera info plugin with the camera parameters.

        Args:
            camera_info: Dictionary containing the camera calibration parameters
                in ROS camera_info format. Only specified parameters will be updated.
                Expected keys:
                - image_height: Image height in pixels
                - image_width: Image width in pixels
                - distortion_model: Distortion model name (e.g., 'plumb_bob', 'equidistant')
                - distortion_coefficients: Dict with 'data' key containing distortion coefficients
                - camera_matrix: Dict with 'data' key containing 3x3 camera matrix (flattened)
                - rectification_matrix: Dict with 'data' key containing 3x3 rectification matrix
                - projection_matrix: Dict with 'data' key containing 3x4 projection matrix
        """
        super().__init__()
        self.camera_info = camera_info
        self.logger.debug('Loaded camera info for: %s', camera_info.get('camera_name', 'unnamed'))

    def convert(self, ros_msg: Any) -> dict[str, Any]:
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


class InsertCameraInfo(ConverterPlugin):
    """Plugin to create CameraInfo messages from Image messages.

    This plugin is designed for MCAP files that contain camera images but lack
    corresponding CameraInfo messages. It creates new CameraInfo messages using
    the image header information and configured calibration parameters.
    """

    def __init__(self, *, camera_info: dict[str, Any]) -> None:
        """Initialize the camera info plugin with the camera parameters.

        Args:
            camera_info: Dictionary containing the camera calibration parameters
                in ROS camera_info format. All calibration parameters should be specified.
                Expected keys:
                - image_height: Image height in pixels
                - image_width: Image width in pixels
                - distortion_model: Distortion model name (e.g., 'plumb_bob', 'equidistant')
                - distortion_coefficients: Dict with 'data' key containing distortion coefficients
                - camera_matrix: Dict with 'data' key containing 3x3 camera matrix (flattened)
                - rectification_matrix: Dict with 'data' key containing 3x3 rectification matrix
                - projection_matrix: Dict with 'data' key containing 3x4 projection matrix
                - binning_x: Horizontal binning factor (optional)
                - binning_y: Vertical binning factor (optional)
                - roi: Region of interest dictionary (optional)
        """
        super().__init__()
        self.camera_info = camera_info
        self.logger.debug('Loaded camera info for: %s', camera_info.get('camera_name', 'unnamed'))

    def convert(self, ros_msg: Any) -> dict[str, Any]:
        """Create a new camera_info message based on the configured parameters.

        This plugin is triggered by raw camera image messages and creates
        a corresponding CameraInfo message with the calibration parameters.

        Args:
            ros_msg: Input ROS message (sensor_msgs/Image) - used for header and dimension info

        Returns:
            New camera info message with calibration parameters

        Raises:
            AttributeError: If the input message lacks required attributes (header)
        """
        if not hasattr(ros_msg, 'header'):
            raise AttributeError("Input message must have a 'header' attribute")

        # Create base camera info message structure using image message attributes
        new_msg = {
            'header': ros_msg.header,  # Preserve timestamp and frame_id from image
            'height': getattr(ros_msg, 'height', 0),
            'width': getattr(ros_msg, 'width', 0),
            'distortion_model': '',
            'd': [],
            'k': [0.0] * 9,  # 3x3 camera matrix (flattened)
            'r': [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],  # 3x3 identity rectification matrix
            'p': [0.0] * 12,  # 3x4 projection matrix (flattened)
            'binning_x': 0,
            'binning_y': 0,
            'roi': getattr(
                ros_msg,
                'roi',
                {'x_offset': 0, 'y_offset': 0, 'height': 0, 'width': 0, 'do_rectify': False},
            ),
        }

        # Override with configured values
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

        # Update binning if specified
        if 'binning_x' in self.camera_info:
            new_msg['binning_x'] = self.camera_info['binning_x']
        if 'binning_y' in self.camera_info:
            new_msg['binning_y'] = self.camera_info['binning_y']

        # Update ROI if specified
        if 'roi' in self.camera_info:
            new_msg['roi'] = self.camera_info['roi']

        self.logger.debug(
            'Created CameraInfo message for frame: %s',
            getattr(new_msg['header'], 'frame_id', 'unknown'),
        )

        return new_msg

    @property
    def output_schema(self) -> str:
        """Return the output message type."""
        return 'sensor_msgs/msg/CameraInfo'
