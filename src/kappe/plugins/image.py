import logging
from io import BytesIO
from pathlib import Path
from typing import Any

from kappe.plugin import ConverterPlugin

logger = logging.getLogger(__name__)

try:
    from PIL import Image
except ImportError as err:
    logger.warning('PIL not installed. Install with "pip install pillow"')
    raise ImportError from err


class CompressImage(ConverterPlugin):
    def __init__(self, *, quality: int = 95) -> None:
        super().__init__()
        self.quality = quality
        self.logger.debug('quality=%d', quality)

    def convert(self, ros_msg: Any) -> Any:
        output = BytesIO()
        img = Image.frombytes('RGB', (ros_msg.width, ros_msg.height), ros_msg.data)
        img.save(output, format='jpeg', optimize=True, quality=self.quality)
        return {
            'header': ros_msg.header,
            'format': 'jpeg',
            'data': output.getvalue(),
        }

    @property
    def output_schema(self) -> str:
        return 'sensor_msgs/msg/CompressedImage'


class CropImage(ConverterPlugin):
    def __init__(self, *, x_min: int, x_max: int, y_min: int, y_max: int) -> None:
        super().__init__()
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

        self.logger.debug('x_min=%d, x_max=%d, y_min=%d, y_max=%d', x_min, x_max, y_min, y_max)

    def convert(self, ros_msg: Any) -> Any:
        stream = BytesIO(ros_msg.data)
        output = BytesIO()
        img = Image.open(stream)
        img = img.crop((self.x_min, self.y_min, self.x_max, self.y_max))
        img.save(output, format='jpeg', optimize=True)
        return {
            'header': ros_msg.header,
            'format': 'jpeg',
            'data': output.getvalue(),
        }

    @property
    def output_schema(self) -> str:
        return 'sensor_msgs/msg/CompressedImage'


class ReCompress(ConverterPlugin):
    def __init__(self, *, quality: int = 10) -> None:
        self.quality = quality

    def convert(self, ros_msg: Any) -> Any:
        stream = BytesIO(ros_msg.data)
        output = BytesIO()
        img = Image.open(stream)
        img.save(output, format='jpeg', quality=self.quality)
        return {
            'header': ros_msg.header,
            'format': 'jpeg',
            'data': output.getvalue(),
        }

    @property
    def output_schema(self) -> str:
        return 'sensor_msgs/msg/CompressedImage'


class SaveCompress(ConverterPlugin):
    def __init__(self, *, quality: int = 10) -> None:
        self.quality = quality
        self.counter = 0

    def convert(self, ros_msg: Any) -> Any:
        stream = BytesIO(ros_msg.data)
        img = Image.open(stream)
        with Path(f'{self.counter:08}.jpeg').open('wb') as f:
            img.save(f, format='jpeg', quality=self.quality)


class DecompressImage(ConverterPlugin):
    """
    Decompresses a sensor_msgs/msg/CompressedImage into raw image data
    suitable for a sensor_msgs/msg/Image message.
    """

    def __init__(self) -> None:
        super().__init__()
        logger.debug('DecompressImage initialized')

    def convert(self, ros_msg: Any) -> Any:
        stream = BytesIO(ros_msg.data)
        img = Image.open(stream)

        img = img.convert('L')  # 'L' = 8-bit pixels, black and white

        width, height = img.size
        raw_data = img.tobytes()

        return {
            'header': ros_msg.header,
            'height': height,
            'width': width,
            'encoding': 'mono8',
            'is_bigendian': 0,
            'step': width,
            'data': raw_data,
        }

    @property
    def output_schema(self) -> str:
        return 'sensor_msgs/msg/Image'
