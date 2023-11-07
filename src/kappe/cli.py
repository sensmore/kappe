import logging
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
from kappe.settings import SettingGeneral, SettingPlugin, Settings, SettingSchema, SettingTopic


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

    logger.info('Done    %s', output_path)


def convert_process(
    config: Settings,
    input_path: Path,
    output_path: Path,
    *,
    overwrite: bool,
) -> None:
    tasks: list[tuple[Path, Path, Settings, int]] = []

    # TODO: make more generic
    if input_path.is_file():
        mcap_out = output_path / input_path.name
        if mcap_out.exists() and not overwrite:
            logger.info('File exists: %s -> skipping', mcap_out)
        else:
            tasks.append((input_path, mcap_out, config, 0))
    else:
        for idx, mcap_in in enumerate(input_path.rglob('**/*.mcap')):
            mcap_out = output_path / mcap_in.relative_to(input_path.parent)

            if mcap_out.exists() and not overwrite:
                logger.info('File exists: %s -> skipping', mcap_out)
                continue
            tasks.append((mcap_in, mcap_out, config, idx))

    if len(tasks) == 0:
        logger.info('No files to convert')
        return

    logger.info('Using %d threads', config.general.threads)

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
    def convert(  # noqa: PLR0913, PLR0912
        self,
        input: Path,  # noqa: A002
        output: Path,
        *,
        general: SettingGeneral | None = None,
        topic: SettingTopic | None = None,
        tf_static: SettingTF | None = None,
        msg_schema: SettingSchema | None = None,
        msg_folder: Path | None = Path('./msgs'),
        point_cloud: dict[str, SettingPointCloud] | None = None,
        time_offset: dict[str, SettingTimeOffset] | None = None,
        plugins: list[SettingPlugin] | None = None,
        plugin_folder: Path | None = Path('./plugins'),
        time_start: float | None = None,
        time_end: float | None = None,
        keep_all_static_tf: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Convert mcap(s) with changing, filtering, converting, ... data.

        Args:
            input: Input mcap or folder of mcaps.
            general: General settings (threads, etc.).
            topic: Migrations for topics (remove, rename, etc.).
            tf_static: Migrations for TF (insert, remove).
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
        """

        # TODO: cleanup
        if general is None:
            general = SettingGeneral()
        if topic is None:
            topic = SettingTopic()
        if tf_static is None:
            tf_static = SettingTF()
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
        config.msg_schema = msg_schema
        config.point_cloud = point_cloud
        config.time_offset = time_offset
        config.plugins = plugins
        config.time_start = time_start
        config.time_end = time_end
        config.keep_all_static_tf = keep_all_static_tf
        config.msg_folder = msg_folder
        config.plugin_folder = plugin_folder

        # check for msgs folder
        if msg_folder is not None and not msg_folder.exists():
            logger.error('msg_folder does not exist: %s', msg_folder)
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

        input_path: Path = input
        if not input_path.exists():
            raise FileNotFoundError(f'Input path does not exist: {input_path}')

        output_path: Path = output

        if errors:
            logger.error('Errors found, aborting')
        else:
            convert_process(
                config,
                input_path,
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
        )

        cutter(mcap, output, config)


def main() -> None:
    CLI(KappeCLI, version=__version__)


if __name__ == '__main__':
    main()
