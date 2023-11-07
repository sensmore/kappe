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
