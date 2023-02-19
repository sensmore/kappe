import logging
from pathlib import Path

# TODO: vendor this
from mcap_ros2._vendor.rosidl_adapter import parser as ros2_parser

ROS_AVAILABLE = False
try:
    from rosidl_runtime_py import get_interface_path
    from rosidl_runtime_py.utilities import get_message

    ROS_AVAILABLE = True
except ImportError:
    pass


def get_msg_def_ros(msg: str) -> str:
    text = ''
    fields = get_message(msg).get_fields_and_field_types()

    for type_name in fields.values():
        # primitive
        if '/' not in type_name:
            continue

        if type_name.startswith('builtin_interfaces/'):
            continue

        text += get_msg_def_ros(type_name)

    text += '========================================\n'
    text += f'MSG: {msg}\n'

    if '/msg/' not in msg:
        split = msg.split('/')
        msg = '/'.join([split[0], 'msg', *split[1:]])

    with Path(get_interface_path(msg)).open(encoding='utf-8') as msg_file:
        text += msg_file.read()

    return text


def get_msg_def_disk(msg: str, folder: Path) -> str | None:
    pkg_name = msg.split('/')[0]
    msg_name = msg.split('/')[-1]

    # TODO: make 'msg' optional?
    # TODO: how to handle multiple matches?
    msg_path = list(folder.glob(f'**/{pkg_name}/msg/{msg_name}.msg'))
    if len(msg_path) == 0:
        return None

    msg_path = msg_path[0]

    if not msg_path.exists():
        return None

    text = ''
    with msg_path.open(encoding='utf-8') as msg_file:
        msg_text = msg_file.read()

    text += f'MSG: {msg}\n'
    text += msg_text
    text += '\n'
    text += '=' * 40 + '\n'

    msg_def = ros2_parser.parse_message_string(pkg_name, msg_name, msg_text)
    for field in msg_def.fields:
        f_type = field.type
        if field.type.is_primitive_type():
            continue

        if f_type.pkg_name == 'builtin_interfaces':
            continue

        field_text = get_msg_def_disk(f'{f_type.pkg_name}/{f_type.type}', folder)
        if field_text is None:
            logging.error(
                'Failed to find definition for %s/%s',
                f_type.pkg_name,
                f_type.type)
            return None
        text += field_text

    return text


def get_msg_def(msg: str, folder: Path | None = None) -> str | None:
    new_data = None
    if ROS_AVAILABLE:
        # use ROS to get the message definition
        new_data = get_msg_def_ros(msg)

    if new_data is None and folder is not None:
        # use ./msgs/ to get the message definition
        return get_msg_def_disk(msg, folder)

    return new_data
