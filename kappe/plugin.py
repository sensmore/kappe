import imp
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any


class ConverterPlugin(ABC):
    @property
    @abstractmethod
    def output_schema(self) -> str:
        pass

    @abstractmethod
    def convert(self, ros_msg: Any) -> Any:
        pass


def load_plugin(base_folder: Path | None, plugin_name: str) -> Callable[..., ConverterPlugin]:
    """Load a plugin by name."""
    pkg_name = plugin_name
    class_name = 'Converter'

    if '.' in plugin_name:
        pkg_name, class_name = plugin_name.split('.')

    plugin_folders: list[Path | None] = [Path(__file__).parent / 'plugins', base_folder]

    for path in plugin_folders:
        if path is None:
            continue

        plugin_file = path / f'{pkg_name}.py'
        if not plugin_file.exists():
            continue

        module = imp.load_source(pkg_name, str(plugin_file))
        return getattr(module, class_name)

    raise ValueError(f'Plugin file {plugin_name} does not exist')
