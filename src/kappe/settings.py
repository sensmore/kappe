from multiprocessing import cpu_count
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from kappe.module.pointcloud import SettingPointCloud
from kappe.module.tf import SettingTF
from kappe.module.timing import SettingTimeOffset


class SettingGeneral(BaseModel):
    """
    General settings.

    :ivar threads: Number of threads to use for processing. If None, use all available cores.
    """

    threads: int = cpu_count()


class SettingTopic(BaseModel):
    """
    Topic settings.

    :ivar mapping: Mapping of topic names to a new name.
    :ivar remove: List of topic names to remove.
    :ivar drop: Drop every nth message.
    """

    mapping: dict[str, str] = {}
    remove: list[str] = []
    drop: dict[str, int] = {}


class SettingSchema(BaseModel):
    """
    Schema settings.

    :ivar definition: Mapping of message names to a schema definition.
    :ivar mapping: Mapping of message names to a new name.
    """

    definition: dict[str, str] = {}
    mapping: dict[str, str] = {}


class SettingPlugin(BaseModel):
    """
    Plugin settings.

    :ivar name: Name of the plugin, e.g. arbe.PointCloud
    :ivar input_topic: Input topic name.
    :ivar output_topic: Output topic name.
    :ivar settings: Plugin specific settings.
    """

    name: str
    input_topic: str
    output_topic: str
    settings: dict[str, Any] = {}


class Settings(BaseModel):
    """
    Settings.

    Note: topic names for topic mapping, point_cloud and time_offset are always
    the original topic names.

    :ivar general: General settings.
    :ivar point_cloud: Mapping of topic names to point cloud settings.
    :ivar topic: Topic settings.
    :ivar tf_static: TF settings.
    :ivar msg_schema: Schema settings.
    :ivar time_offset: Mapping of topic names to time offset settings.
    :ivar plugins: List of plugins.
    :ivar time_start: Start time of the recording.
    :ivar time_end: End time of the recording.
    :ivar keep_all_static_tf: Keep all static TF frames.
    :ivar msg_folder: Folder containing message definitions, defaults to ./msgs/.
    :ivar raw_text: Raw yaml as read from disk.
    """

    general: SettingGeneral = SettingGeneral()
    topic: SettingTopic = SettingTopic()
    tf_static: SettingTF = SettingTF()
    msg_schema: SettingSchema = SettingSchema()

    point_cloud: dict[str, SettingPointCloud] = {}
    time_offset: dict[str, SettingTimeOffset] = {}
    plugins: list[SettingPlugin] = []

    time_start: float | None = None
    time_end: float | None = None

    keep_all_static_tf: bool = False

    msg_folder: Path | None = Path('./msgs')
    plugin_folder: Path | None = Path('./plugins')

    raw_text: str = ''
