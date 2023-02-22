from mcap_ros1.reader import McapROS1Message
from mcap_ros2.reader import McapROS2Message

McapROSMessage = McapROS1Message | McapROS2Message


class ClassDict(dict):
    """Class to allow attribute access to dict items."""

    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
