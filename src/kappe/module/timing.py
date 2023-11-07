import logging
from types import SimpleNamespace
from typing import Any

from mcap.reader import DecodedMessageTuple
from mcap.records import Message
from mcap_ros1._vendor.genpy.rostime import Duration as ROS1Duration
from mcap_ros1._vendor.genpy.rostime import Time as ROS1Time
from pydantic import BaseModel

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


class SettingTimeOffset(BaseModel):
    """
    Time offset settings.

    :ivar sec:      Seconds to add to the timestamp.
    :ivar nanosec:  Nanoseconds to add to the timestamp.
    :ivar pub_time: Use the publish time instead of the message time. If set sec and nanosec
                    will be added to the publish time.
    :ivar update_log_time: Updates the log_time of the MCAP message.
    :ivar update_publish_time: Updates the publish_time of the MCAP message.
    """

    sec: int = 0
    nanosec: int = 0
    pub_time: bool = False
    update_log_time: bool = False
    update_publish_time: bool = False


def time_offset_stamp(cfg: SettingTimeOffset, message: Message, stamp: TimeMsg) -> None:
    if cfg.pub_time:
        stamp_nano = message.publish_time
    else:
        stamp_nano = int(stamp.sec * 1e9) + int(stamp.nanosec)

    stamp_nano += cfg.sec
    stamp_nano += int(cfg.nanosec * 1e9)

    sec = int(stamp_nano // 1e9)
    nanosec = stamp_nano - (sec * 1e9)

    stamp.sec = sec
    stamp.nanosec = nanosec

    if cfg.update_publish_time:
        message.publish_time = stamp_nano

    if cfg.update_log_time:
        message.log_time = stamp_nano


def time_offset_rec(cfg: SettingTimeOffset, message: Message, msg: Any) -> None:
    if not hasattr(msg, '__slots__'):
        return

    for slot in msg.__slots__:
        attr = getattr(msg, slot)

        if isinstance(attr, list):
            for i in attr:
                time_offset_rec(cfg, message, i)

        if 'mcap_ros2._dynamic.Time' in str(type(attr)):
            time_offset_stamp(cfg, message, attr)
        else:
            time_offset_rec(cfg, message, attr)


def time_offset(cfg: SettingTimeOffset, msg: DecodedMessageTuple) -> None:
    """Apply time offset to the message."""
    ros_msg = msg.decoded_message
    if not hasattr(msg, '__slots__'):
        return
    time_offset_rec(cfg, msg.message, ros_msg)


def fix_ros1_time(msg: Any) -> None:
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
