from types import SimpleNamespace

import pytest
from mcap_ros1._vendor.genpy.rostime import Duration as ROS1Duration
from mcap_ros1._vendor.genpy.rostime import Time as ROS1Time

from kappe.module.timing import fix_ros1_time


def test_fix_ros1_time_basic():
    """Test fixing a simple ROS1 Time object."""
    msg = SimpleNamespace()
    msg.__slots__ = ['timestamp']
    msg.timestamp = ROS1Time(secs=10, nsecs=500000000)

    fix_ros1_time(msg)

    assert hasattr(msg.timestamp, 'sec')
    assert hasattr(msg.timestamp, 'nanosec')
    assert msg.timestamp.sec == 10
    assert msg.timestamp.nanosec == 500000000


def test_fix_ros1_duration_basic():
    """Test fixing a simple ROS1 Duration object."""
    msg = SimpleNamespace()
    msg.__slots__ = ['duration']
    msg.duration = ROS1Duration(secs=5, nsecs=250000000)

    fix_ros1_time(msg)

    assert hasattr(msg.duration, 'sec')
    assert hasattr(msg.duration, 'nanosec')
    assert msg.duration.sec == 5
    assert msg.duration.nanosec == 250000000


def test_fix_ros1_time_nested():
    """Test fixing nested messages with ROS1 Time (recursively converts all nested objects)."""
    inner = SimpleNamespace()
    inner.__slots__ = ['timestamp']
    inner.timestamp = ROS1Time(secs=20, nsecs=100000000)

    outer = SimpleNamespace()
    outer.__slots__ = ['header', 'time']
    outer.header = inner
    outer.time = ROS1Time(secs=10, nsecs=50000000)

    fix_ros1_time(outer)

    # Top-level ROS1Time is converted
    assert hasattr(outer.time, 'sec')
    assert outer.time.sec == 10
    assert outer.time.nanosec == 50000000

    # Nested object's ROS1Time is also converted (function recurses into nested objects)
    assert hasattr(outer.header.timestamp, 'sec')
    assert outer.header.timestamp.sec == 20
    assert outer.header.timestamp.nanosec == 100000000


def test_fix_ros1_time_list():
    """Test fixing ROS1 Time objects in nested messages within a list."""
    inner1 = SimpleNamespace()
    inner1.__slots__ = ['timestamp']
    inner1.timestamp = ROS1Time(secs=1, nsecs=0)

    inner2 = SimpleNamespace()
    inner2.__slots__ = ['timestamp']
    inner2.timestamp = ROS1Time(secs=2, nsecs=0)

    msg = SimpleNamespace()
    msg.__slots__ = ['messages']
    msg.messages = [inner1, inner2]

    fix_ros1_time(msg)

    # The function recurses into list items
    assert hasattr(msg.messages[0].timestamp, 'sec')
    assert msg.messages[0].timestamp.sec == 1
    assert hasattr(msg.messages[1].timestamp, 'sec')
    assert msg.messages[1].timestamp.sec == 2


def test_fix_ros1_time_complex_nested():
    """Test fixing a complex structure with lists."""
    # Create items in a list that contain ROS1 times
    inner1 = SimpleNamespace()
    inner1.__slots__ = ['time']
    inner1.time = ROS1Time(secs=1, nsecs=111)

    inner2 = SimpleNamespace()
    inner2.__slots__ = ['duration']
    inner2.duration = ROS1Duration(secs=2, nsecs=222)

    outer = SimpleNamespace()
    outer.__slots__ = ['items']
    outer.items = [inner1, inner2]

    fix_ros1_time(outer)

    # List items are recursively processed
    assert hasattr(outer.items[0].time, 'sec')
    assert outer.items[0].time.sec == 1
    assert outer.items[0].time.nanosec == 111
    assert hasattr(outer.items[1].duration, 'sec')
    assert outer.items[1].duration.sec == 2
    assert outer.items[1].duration.nanosec == 222


def test_fix_ros1_time_no_slots():
    """Test that objects without __slots__ are handled gracefully."""
    msg = {'timestamp': ROS1Time(secs=10, nsecs=0)}
    # Should not raise an error
    fix_ros1_time(msg)
    # Dict should remain unchanged
    assert isinstance(msg['timestamp'], ROS1Time)


@pytest.mark.parametrize(
    ('secs', 'nsecs'),
    [
        pytest.param(0, 0, id='zero'),
        pytest.param(1, 0, id='one_sec'),
        pytest.param(0, 999999999, id='max_nsec'),
        pytest.param(999999999, 999999999, id='large_values'),
        pytest.param(123, 456789, id='arbitrary'),
    ],
)
def test_fix_ros1_time_values(secs: int, nsecs: int):
    """Test fixing ROS1 Time with various values."""
    msg = SimpleNamespace()
    msg.__slots__ = ['timestamp']
    msg.timestamp = ROS1Time(secs=secs, nsecs=nsecs)

    fix_ros1_time(msg)

    assert msg.timestamp.sec == secs
    assert msg.timestamp.nanosec == nsecs


def test_fix_ros1_time_mixed_types():
    """Test message with both ROS1 time and regular attributes."""
    msg = SimpleNamespace()
    msg.__slots__ = ['timestamp', 'name', 'value']
    msg.timestamp = ROS1Time(secs=42, nsecs=123456)
    msg.name = 'test'
    msg.value = 100

    fix_ros1_time(msg)

    # ROS1 time should be converted
    assert hasattr(msg.timestamp, 'sec')
    assert msg.timestamp.sec == 42
    # Other attributes should remain unchanged
    assert msg.name == 'test'
    assert msg.value == 100
