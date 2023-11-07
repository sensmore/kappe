import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


class ConverterPlugin(ABC):
    def __init__(self, **_kwargs: Any) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abstractmethod
    def output_schema(self) -> str:
        pass

    @abstractmethod
    def convert(self, ros_msg: Any) -> Any:
        pass


def module_get_plugins(module: ModuleType) -> list[str]:
    """Get a list of available plugins in a module."""
    return [
        cls
        for cls in dir(module)
        if isinstance(getattr(module, cls), type)
        and issubclass(getattr(module, cls), ConverterPlugin)
        and cls != 'ConverterPlugin'
    ]


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
            logger.debug('Plugin %s does not exist in %s', plugin_name, path)
            continue

        try:
            module = SourceFileLoader(pkg_name, str(plugin_file)).load_module()
        except ImportError:
            logger.debug('Plugin %s could not be loaded from %s', plugin_name, path)
            continue

        if hasattr(module, class_name):
            logger.debug('Plugin %s loaded from %s', plugin_name, path)
            return getattr(module, class_name)

        logger.error('Plugin %s does not have class %s', plugin_name, class_name)
        logger.info('Available plugins: %s', ', '.join(module_get_plugins(module)))

    raise ValueError(f'Plugin file {plugin_name} does not exist')
