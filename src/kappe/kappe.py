"""
Convert mcap (ROS1 & ROS2) files.

Message definitions:
    Message definitions are read from ROS and disk ./msgs/
    git clone --depth=1 --branch=humble https://github.com/ros2/common_interfaces.git msgs
"""

import argparse
import logging
from multiprocessing import Pool, RLock
from pathlib import Path
from typing import Any

import pydantic
import strictyaml
from tqdm import tqdm

from kappe import __version__
from kappe.convert import Converter
from kappe.cut import CutSettings, cutter
from kappe.plugin import load_plugin
from kappe.settings import Settings


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record: Any):
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


def print_error(e: pydantic.ValidationError, config_yaml: strictyaml.YAML):
    logger.info('Failed to parse config file')
    for err in e.errors():
        yaml_obj = config_yaml
        for x in err['loc']:
            k = None

            match x:
                case int(idx) if len(yaml_obj) > idx:
                    k = yaml_obj[idx]
                case str(key):
                    k = yaml_obj.get(key)

            if k is None:
                break

            yaml_obj = yaml_obj[x]

        loc = ' -> '.join(str(x) for x in err['loc'])
        msg = err['msg']
        err_type = err['type']
        logger.info('%s: %s @ Line: %i "%s"', err_type, msg, yaml_obj.start_line, loc)


def worker(arg: tuple[Path, Path, Settings, int]):
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


def process(config: Settings, input_path: Path, output_path: Path, *, overwrite: bool) -> None:
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
    tqdm.set_lock(RLock())  # for managing output contention

    pool = None
    try:
        pool = Pool(min(config.general.threads, len(tasks)),
                    initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
        pool.map(worker, tasks)
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt')
    finally:
        if pool is not None:
            pool.terminate()
            pool.join()


def cmd_convert(args: argparse.Namespace):
    if args.config is None:
        config = Settings()
    else:

        config_text = args.config.read()
        config_yaml: strictyaml.YAML = strictyaml.load(config_text)
        try:
            config = Settings(**config_yaml.data)
        except pydantic.ValidationError as e:
            print_error(e, config_yaml)
            return

        config.raw_text = config_text

    # check for msgs folder
    if config.msg_folder is not None and not config.msg_folder.exists():
        logger.error('msg_folder does not exist: %s', config.msg_folder)
        config.msg_folder = None

    errors = False

    for conv in config.plugins:
        try:
            load_plugin(config.plugin_folder, conv.name)
            continue
        except ValueError:
            pass

        errors = True
        logger.error('Failed to load plugin: %s', conv.name)

    input_path: Path = args.input
    if not input_path.exists():
        raise FileNotFoundError(f'Input path does not exist: {input_path}')

    output_path: Path = args.output

    if errors:
        logger.error('Errors found, aborting')
    else:
        process(config, input_path, output_path, overwrite=args.overwrite)


def cmd_cut(args: argparse.Namespace):
    logger.info('cut')

    config_text = args.config.read()
    config_yaml: strictyaml.YAML = strictyaml.load(config_text)

    try:
        config = CutSettings(**config_yaml.data)
    except pydantic.ValidationError as e:
        print_error(e, config_yaml)
        return

    cutter(args.input, args.output_folder, config)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        fromfile_prefix_chars='@',
    )

    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--version', action='version', version=__version__)

    sub = parser.add_subparsers(
        title='subcommands',
        required=True,
    )

    cutter = sub.add_parser('cut')
    cutter.set_defaults(func=cmd_cut)

    cutter.add_argument('input', type=Path, help='input file')
    cutter.add_argument(
        'output_folder',
        type=Path,
        help='output folder, default: ./cut_out',
        default=Path('./cut_out'),
        nargs='?')
    cutter.add_argument('--config', type=argparse.FileType(), help='config file', required=True)
    cutter.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files')

    convert = sub.add_parser('convert')
    convert.set_defaults(func=cmd_convert)

    convert.add_argument('input', type=Path, help='input folder or file')
    convert.add_argument('output', type=Path, help='output folder')
    convert.add_argument('--config', type=argparse.FileType(), help='config file')
    convert.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    args.func(args)


if __name__ == '__main__':
    main()
