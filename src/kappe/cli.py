import logging
import sys
from pathlib import Path
from typing import Any

from jsonargparse import CLI
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from kappe import __version__
from kappe.convert import Converter
from kappe.cut import CutSettings, CutSplitOn, CutSplits, cutter
from kappe.module.pointcloud import SettingPointCloud
from kappe.module.tf import SettingTF
from kappe.module.timing import SettingTimeOffset
from kappe.plugin import load_plugin
from kappe.settings import (
    ROS2Distro,
    SettingGeneral,
    SettingPlugin,
    Settings,
    SettingSchema,
    SettingTopic,
)


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: Any) -> None:
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:  # noqa: BLE001
            self.handleError(record)


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-7s | %(name)s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[TqdmLoggingHandler()],
)

logger = logging.getLogger(__name__)


def convert_worker(arg: tuple[Path, Path, Settings, int]) -> None:
    # TODO: dataclass
    input_path, output_path, config, tqdm_idx = arg

    logger.info('Writing %s', output_path)
    try:
        conv = Converter(config, input_path, output_path)
        conv.process_file(tqdm_idx)
        conv.finish()

    except KeyboardInterrupt:
        logger.info('WORKER: Keyboard interrupt')
        return
    except Exception:
        logger.exception('Failed to convert %s', input_path)
        raise

    logger.info('Done    %s', output_path)


def convert_process(  # noqa: PLR0912
    config: Settings,
    input_path: Path | list[Path],
    output_path: Path,
    *,
    overwrite: bool,
) -> None:
    tasks: list[tuple[Path, Path, Settings, int]] = []

    if isinstance(input_path, list):
        for idx, inp in enumerate(input_path):
            tasks.append((inp, output_path / inp.name, config, idx))
    elif input_path.is_file():
        tasks.append((input_path, output_path / input_path.name, config, 0))
    else:
        for idx, mcap_in in enumerate(input_path.rglob('**/*.mcap')):
            mcap_out = output_path / mcap_in.relative_to(input_path.parent)
            tasks.append((mcap_in, mcap_out, config, idx))

    def filter_tasks(task: tuple[Path, Path, Settings, int]) -> bool:
        mcap_out = task[1]
        if mcap_out.exists():
            logger.info('File exists: %s -> skipping', mcap_out)
            return False

        return True

    if not overwrite:
        tasks = list(filter(filter_tasks, tasks))

    if len(tasks) == 0:
        logger.info('No files to convert')
        return

    logger.info('Using %d threads', config.general.threads)
    if config.general.threads == 0 or len(tasks) == 1:
        for t in tasks:
            convert_worker(t)
    else:
        pool = None
        try:
            process_map(convert_worker, tasks, max_workers=min(config.general.threads, len(tasks)))
        except KeyboardInterrupt:
            logger.info('Keyboard interrupt')
        finally:
            if pool is not None:
                pool.terminate()
                pool.join()


class KappeCLI:
    def __init__(
        self,
        *,
        progress: bool = True,
    ) -> None:
        """general options
        :ivar progress: Display progress bar
        """
        self.progress = progress

    def convert(  # noqa: PLR0913, PLR0912, PLR0915
        self,
        input: list[Path] | Path,  # noqa: A002
        output: Path,
        *,
        general: SettingGeneral | None = None,
        topic: SettingTopic | None = None,
        tf_static: SettingTF | None = None,
        tf: SettingTF | None = None,
        msg_schema: SettingSchema | None = None,
        msg_folder: Path | None = None,
        point_cloud: dict[str, SettingPointCloud] | None = None,
        time_offset: dict[str, SettingTimeOffset] | None = None,
        plugins: list[SettingPlugin] | None = None,
        plugin_folder: Path | None = Path('./plugins'),
        time_start: float | None = None,
        time_end: float | None = None,
        keep_all_static_tf: bool = False,
        save_metadata: bool = True,
        overwrite: bool = False,
        frame_id_mapping: dict[str, str] | None = None,
        ros_distro: ROS2Distro = ROS2Distro.HUMBLE,
    ) -> None:
        """Convert mcap(s) with changing, filtering, converting, ... data.

        Args:
            input: Input mcap or folder of mcaps.
            general: General settings (threads, etc.).
            topic: Migrations for topics (remove, rename, etc.).
            frame_id_mapping: Mapping of topic names to new frame_id values. Applied globally.
            tf_static: Migrations for TF static (insert, remove, offset).
            tf: Migrations for TF (insert, remove, offset).
            msg_schema: Updating or changing a schema.
            msg_folder: Path to the folder containing .msg files used to change the schema and
                upgrading from ROS1.
            point_cloud: Migrations for point clouds (Update filed, rotate, etc.).
            time_offset: Migrations for time (Add offset, sync with mcap time, etc.).
            plugins: Settings to loading custom plugins.
            plugin_folder: Path to plugin folder.
            time_start: Start time of the new MCAP.
            time_end: End time of the new MCAP.
            keep_all_static_tf: If true ensue all /tf_static messages are in the outputted file.
            overwrite: If true already existing files will be overwritten.
            ros_distro: ROS2 distribution to use for message definitions.
        """

        # TODO: cleanup
        if general is None:
            general = SettingGeneral()
        if topic is None:
            topic = SettingTopic()
        if frame_id_mapping is None:
            frame_id_mapping = {}
        if tf_static is None:
            tf_static = SettingTF()
        if tf is None:
            tf = SettingTF()
        if msg_schema is None:
            msg_schema = SettingSchema()
        if point_cloud is None:
            point_cloud = {}
        if time_offset is None:
            time_offset = {}
        if plugins is None:
            plugins = []

        config = Settings()
        config.general = general
        config.topic = topic
        config.tf_static = tf_static
        config.tf = tf
        config.msg_schema = msg_schema
        config.point_cloud = point_cloud
        config.time_offset = time_offset
        config.plugins = plugins
        config.time_start = time_start
        config.time_end = time_end
        config.keep_all_static_tf = keep_all_static_tf
        config.msg_folder = msg_folder
        config.plugin_folder = plugin_folder
        config.progress = self.progress
        config.save_metadata = save_metadata
        config.frame_id_mapping = frame_id_mapping
        config.ros_distro = ros_distro

        # check for msgs folder
        if msg_folder is not None and not msg_folder.exists():
            logger.warning('msg_folder does not exist: %s', msg_folder)
            msg_folder = None

        errors = False

        for conv in plugins:
            try:
                load_plugin(plugin_folder, conv.name)
                continue
            except ValueError:
                pass

            errors = True
            logger.error('Failed to load plugin: %s', conv.name)

        if isinstance(input, list):
            for inp in input:
                if not inp.exists():
                    errors = True
                    logger.error('Input path does not exist: %s', inp)
        elif not input.exists():
            errors = True
            logger.error('Input path does not exist: %s', input)

        output_path: Path = output

        if errors:
            logger.error('Errors found, aborting')
        else:
            convert_process(
                config,
                input,
                output_path,
                overwrite=overwrite,
            )

    def cut(  # noqa: PLR0913
        self,
        mcap: Path,
        output: Path = Path('./output'),
        *,
        overwrite: bool = False,
        keep_tf_tree: bool = False,
        splits: list[CutSplits] | None = None,
        topic: str | None = None,
        debounce: float = 0.0,
    ) -> None:
        """
        Cut a mcap based on time or maker topic.

        Args:
            mcap: Input mcap file.
            output: Output folder.
            overwrite: Overwrite existing files.
            keep_tf_tree: Keep all /tf_static message in file.
            splits: List of splits.
            topic: Topic to use for splitting.
            debounce: Number of seconds to wait before splitting on the same topic.
        """
        split_on_topic = None

        if output.exists() and not overwrite:
            logger.error('Output folder already exists. Delete or use --overwrite=true.')
            return

        if topic is not None:
            split_on_topic = CutSplitOn(
                topic=topic,
                debounce=debounce,
            )

        config = CutSettings(
            keep_tf_tree=keep_tf_tree,
            splits=splits,
            split_on_topic=split_on_topic,
            progress=self.progress,
        )

        cutter(mcap, output, config)

    def version(self) -> None:
        print('Kappe:  ', __version__)  # noqa: T201
        print('Python: ', sys.version)  # noqa: T201


def main() -> None:
    CLI(KappeCLI, version=__version__)


if __name__ == '__main__':
    main()
