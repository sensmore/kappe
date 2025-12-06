import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


class ConverterPlugin(ABC):
    def __init__(self, **_kwargs: Any) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    @abstractmethod
    def output_schema(self) -> str:  # pragma: no cover
        pass

    @abstractmethod
    def convert(self, ros_msg: Any) -> Any:  # pragma: no cover
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

    plugin_folders: list[Path] = [Path(__file__).parent / 'plugins']
    if base_folder:
        plugin_folders.append(base_folder)

    for path in plugin_folders:
        plugin_file = path / f'{pkg_name}.py'
        if not plugin_file.exists():
            logger.debug('Plugin %s does not exist in %s', plugin_name, path)
            continue

        try:
            loader = SourceFileLoader(pkg_name, str(plugin_file))
            spec = spec_from_loader(pkg_name, loader)
            if spec is None or spec.loader is None:
                raise ImportError(f'Could not create spec for {pkg_name}')  # noqa: TRY301
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
        except ImportError:
            logger.debug('Plugin %s could not be loaded from %s', plugin_name, path)
            continue

        if hasattr(module, class_name):
            logger.debug('Plugin %s loaded from %s', plugin_name, path)
            return getattr(module, class_name)

        logger.error('Plugin %s does not have class %s', plugin_name, class_name)
        logger.info('Available plugins: %s', ', '.join(module_get_plugins(module)))

    raise ValueError(f'Plugin file {plugin_name} does not exist')
