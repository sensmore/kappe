import logging
from types import SimpleNamespace
from typing import Any

from mcap.reader import DecodedMessageTuple
from mcap_ros1._vendor.genpy.rostime import Duration as ROS1Duration
from mcap_ros1._vendor.genpy.rostime import Time as ROS1Time
from pydantic import BaseModel, Extra

logger = logging.getLogger(__name__)

# TODO: move to utils, make more generic
TimeMsg = type(
    'builtin_interfaces/Time',
    (SimpleNamespace,),
    {
        '__name__': 'builtin_interfaces/Time',
        '__slots__': ['sec', 'nanosec'],
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


def time_offset_stamp(cfg: SettingTimeOffset, publish_time_ns: int, stamp: TimeMsg):
    pub_time = cfg.pub_time
    off_sec = cfg.sec
    off_nanosec = cfg.nanosec

    if pub_time is True:
        header_sec = publish_time_ns // 1e9
        header_nanosec = publish_time_ns % int(1e9)
    else:
        header_sec = int(stamp.sec)
        header_nanosec = int(stamp.nanosec)

    header_nanosec += off_nanosec

    # handle nanosec under & overflow
    if header_nanosec < 0:
        header_nanosec += int(1e9)
        header_sec -= 1
    elif header_nanosec >= int(1e9):
        header_nanosec -= int(1e9)
        header_sec += 1

    stamp.nanosec = header_nanosec
    stamp.sec = header_sec + off_sec


def time_offset_rec(cfg: SettingTimeOffset, publish_time_ns: int, msg: Any):
    if not hasattr(msg, '__slots__'):
        return

    for slot in msg.__slots__:
        attr = getattr(msg, slot)

        if isinstance(attr, list):
            for i in attr:
                time_offset_rec(cfg, publish_time_ns, i)

        if 'mcap_ros2._dynamic.Time' in str(type(attr)):
            time_offset_stamp(cfg, publish_time_ns, attr)
        else:
            time_offset_rec(cfg, publish_time_ns, attr)


def time_offset(cfg: SettingTimeOffset, msg: DecodedMessageTuple):
    """Apply time offset to the message."""
    schema, channel, message, ros_msg = msg
    if not hasattr(msg, '__slots__'):
        return
    time_offset_rec(cfg, message.publish_time_ns, ros_msg)


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

        elif isinstance(attr, ROS1Time | ROS1Duration):
            time = TimeMsg()
            time.sec = attr.secs
            time.nanosec = attr.nsecs

            setattr(msg, slot, time)
