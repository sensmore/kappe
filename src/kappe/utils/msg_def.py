import logging
from io import BytesIO
from pathlib import Path
from typing import IO
from zipfile import ZipFile

import platformdirs

# TODO: vendor this
from mcap_ros2._vendor.rosidl_adapter import parser as ros2_parser

from kappe.settings import ROS2Distro

logger = logging.getLogger(__name__)


def _get_msg_def_repos(distro: ROS2Distro) -> list[tuple[str, str]]:
    """Get message definition repositories for a specific ROS2 distribution."""
    distro_str = distro.value

    return [
        (
            'rcl_interfaces',
            f'https://github.com/ros2/rcl_interfaces/archive/refs/heads/{distro_str}.zip',
        ),
        (
            'common_interfaces',
            f'https://github.com/ros2/common_interfaces/archive/refs/heads/{distro_str}.zip',
        ),
        (
            'geometry2',
            f'https://github.com/ros2/geometry2/archive/refs/heads/{distro_str}.zip',
        ),
    ]


def _download(url: str, buffer: IO) -> None:
    from urllib.error import URLError
    from urllib.request import urlopen

    if not url.startswith(('http:', 'https:')):
        raise ValueError("URL must start with 'http:' or 'https:'")

    try:
        with urlopen(url) as response:  # noqa: S310
            if response.status != 200:
                msg = f'Failed to download {url}: {response.status} {response.reason}'
                raise ValueError(msg)
            buffer.write(response.read())
    except URLError as e:
        raise ValueError(f'Network error downloading {url}: {e}') from e


def _download_and_extract(url: str, target_dir: Path) -> None:
    if target_dir.exists():
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    buffer = BytesIO()
    _download(url, buffer)
    with ZipFile(buffer, 'r') as zip_ref:
        zip_ref.extractall(target_dir)


def _update_cache(cache_dir: Path, distro: ROS2Distro) -> None:
    msg_def_repos = _get_msg_def_repos(distro)
    for name, url in msg_def_repos:
        _download_and_extract(url, cache_dir / name)


def _get_cache_dir(distro: ROS2Distro) -> Path:
    """Get cache directory for a specific ROS2 distribution."""
    return platformdirs.user_cache_path(
        appname='kappe_msg_def',
        ensure_exists=True,
        version=f'1_{distro.value}',
    )


def _rglob_first(folder: list[Path], pattern: str) -> Path | None:
    for f in folder:
        if f is None or not f.exists():
            continue
        matches = list(f.rglob(pattern))
        if len(matches) > 0:
            return matches[0]
    return None


def _get_msg_def_disk(msg_type: str, folder: list[Path]) -> tuple[str, list[str]] | None:
    pkg_name = msg_type.split('/')[0]
    msg_name = msg_type.split('/')[-1]

    # TODO: make 'msg' optional?
    # TODO: how to handle multiple matches?
    msg_path = _rglob_first(folder, f'**/{pkg_name}/msg/{msg_name}.msg')
    if msg_path is None:
        return None

    with msg_path.open(encoding='utf-8') as msg_file:
        msg_text = msg_file.read()

    dependencies = []
    msg_def = ros2_parser.parse_message_string(pkg_name, msg_name, msg_text)
    for field in msg_def.fields:
        f_type = field.type
        if field.type.is_primitive_type():
            continue

        # builtin_interfaces are expected to be known by the parser
        if f_type.pkg_name == 'builtin_interfaces':
            continue

        dependencies.append(f'{f_type.pkg_name}/{f_type.type}')

    return (msg_text, dependencies)


def _get_msg_def(
    msg_type: str,
    distro: ROS2Distro,
    folder: Path | None = None,
) -> tuple[str, list[str]] | None:
    cache_dir = _get_cache_dir(distro)
    _update_cache(cache_dir, distro)
    return _get_msg_def_disk(msg_type, [folder, cache_dir] if folder else [cache_dir])


def get_message_definition(
    msg_type: str,
    distro: ROS2Distro,
    folder: Path | None = None,
) -> str | None:
    msg_def = _get_msg_def(msg_type, distro, folder)
    if msg_def is None:
        return None

    msg_text, dependencies = msg_def
    added_types = set()
    while len(dependencies) > 0:
        for dep in dependencies:
            if dep in added_types:
                continue

            msg_def = _get_msg_def(dep, distro, folder)
            if msg_def is None:
                return None

            dep_text, dep_dep = msg_def

            msg_text += '=' * 40 + '\n'
            msg_text += f'MSG: {dep}\n'
            msg_text += dep_text
            added_types.add(dep)
            dependencies.extend(dep_dep)

        # remove added types
        for added in added_types:
            if added in dependencies:
                dependencies.remove(added)

    return msg_text
