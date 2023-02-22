from types import SimpleNamespace
from typing import Any

from mcap_ros1._vendor.genpy.rostime import Duration as ROS1Duration
from mcap_ros1._vendor.genpy.rostime import Time as ROS1Time
from pydantic import BaseModel, Extra

from kappe.utils.types import McapROSMessage

# TODO: move to utils, make more generic
TimeMsg = type(
    'builtin_interfaces/Time',
    (SimpleNamespace,),
    {
        '__name__': 'builtin_interfaces/Time',
        '__slots__': ['secs', 'nsecs'],
    },
)


class SettingTimeOffset(BaseModel, extra=Extra.forbid):
    """
    Time offset settings.

    :ivar sec:      Seconds to add to the timestamp.
    :ivar nanosec:  Nanoseconds to add to the timestamp.
    :ivar pub_time: Use the publish time instead of the message time. If set sec and nanosec
                    will be added to the publish time.
    """

    sec: int = 0
    nanosec: int = 0
    pub_time: bool | None


def time_offset(cfg: SettingTimeOffset, msg: McapROSMessage):
    """Apply time offset to the message."""
    pub_time = cfg.pub_time
    off_sec = cfg.sec
    off_nanosec = cfg.nanosec

    if pub_time is True:
        header_sec = msg.publish_time_ns // 1e9
        header_nanosec = msg.publish_time_ns % int(1e9)
    else:
        header_sec = int(msg.ros_msg.header.stamp.sec)
        header_nanosec = int(msg.ros_msg.header.stamp.nanosec)

    header_nanosec += off_nanosec

    # handle nanosec under & overflow
    if header_nanosec < 0:
        header_nanosec += int(1e9)
        header_sec -= 1
    elif header_nanosec >= int(1e9):
        header_nanosec -= int(1e9)
        header_sec += 1

    header = msg.ros_msg.header
    header.stamp.nanosec = header_nanosec
    header.stamp.sec = header_sec + off_sec


def fix_ros1_time(msg: Any):
    """
    Fix ROS1 time and duration types, recursively inplace.

    secs -> sec
    nsecs -> nanosec
    """
    if not hasattr(msg, '__slots__'):
        return

    for slot in msg.__slots__:
        attr = getattr(msg, slot)

        if isinstance(attr, list):
            for i in attr:
                fix_ros1_time(i)

        elif isinstance(attr, (ROS1Time, ROS1Duration)):
            time = TimeMsg()
            time.secs = attr.secs
            time.nsecs = attr.nsecs

            setattr(msg, slot, time)
