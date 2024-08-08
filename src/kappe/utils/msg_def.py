import logging
from pathlib import Path

# TODO: vendor this
from mcap_ros2._vendor.rosidl_adapter import parser as ros2_parser

logger = logging.getLogger(__name__)

try:
    from rosidl_runtime_py import get_interface_path, get_message_slot_types
    from rosidl_runtime_py.utilities import get_message
    from rosidl_generator_type_description import field_type_nested_type_name
except ImportError:
    logger.debug('rosidl_runtime_py not found')
    get_interface_path = None
    get_message = None




def get_msg_def_ros(msg: str) -> tuple[str, list[str]] | None:
    if get_message is None or get_interface_path is None:
        return None

    parser_def_class = get_message(msg)

    dependencies = []
    for field_type in get_message_slot_types(parser_def_class).values():
        # builtin_interfaces are expected to be known by the parser
        type_name = field_type_nested_type_name(field_type)
        if type_name == '':
            continue

        if type_name.startswith('builtin_interfaces/'):
            continue

        dependencies.append(type_name)

    with Path(get_interface_path(msg)).open(encoding='utf-8') as msg_file:
        text = msg_file.read()

    return text, dependencies


"""
    Returns message definition and its dependencies
"""


def get_msg_def_disk(msg_type: str, folder: Path) -> tuple[str, list[str]] | None:
    pkg_name = msg_type.split('/')[0]
    msg_name = msg_type.split('/')[-1]

    # TODO: make 'msg' optional?
    # TODO: how to handle multiple matches?
    msg_path = list(folder.glob(f'**/{pkg_name}/msg/{msg_name}.msg'))
    if len(msg_path) == 0:
        return None

    msg_path = msg_path[0]

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


def get_msg_def(msg_type: str, folders: list[Path] | None = None) -> tuple[str, list[str]] | None:
    ret = get_msg_def_ros(msg_type)
    if ret is None and folders is not None:
        for folder in folders:
            ret = get_msg_def_disk(msg_type, folder)
            if ret is not None:
                break

    return ret


def get_message_definition(msg_type: str, folders: list[Path] | None = None) -> str | None:
    msg_def = get_msg_def(msg_type, folders)
    if msg_def is None:
        return None

    msg_text, dependencies = msg_def
    added_types = set()
    while len(dependencies) > 0:
        for dep in dependencies:
            if dep in added_types:
                continue

            msg_def = get_msg_def(dep, folders)
            if msg_def is None:
                return None

            dep_text, dep_dep = msg_def

            msg_text += '=' * 40 + '\n'
            msg_text += f'MSG: {dep}\n'
            msg_text += dep_text
            msg_text += "\n"
            added_types.add(dep)
            dependencies.extend(dep_dep)

        # remove added types
        for added in added_types:
            if added in dependencies:
                dependencies.remove(added)

    return msg_text
