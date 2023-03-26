from dataclasses import dataclass
from enum import IntEnum

import strictyaml


class HistoryPolicy(IntEnum):
    """
    Enum for QoS History settings.

    This enum matches the one defined in rmw/types.h
    """

    SYSTEM_DEFAULT = 0
    KEEP_LAST = 1
    KEEP_ALL = 2
    UNKNOWN = 3


class ReliabilityPolicy(IntEnum):
    """
    Enum for QoS Reliability settings.

    This enum matches the one defined in rmw/types.h
    """

    SYSTEM_DEFAULT = 0
    RELIABLE = 1
    BEST_EFFORT = 2
    UNKNOWN = 3
    BEST_AVAILABLE = 4


class DurabilityPolicy(IntEnum):
    """
    Enum for QoS Durability settings.

    This enum matches the one defined in rmw/types.h
    """

    SYSTEM_DEFAULT = 0
    TRANSIENT_LOCAL = 1
    VOLATILE = 2
    UNKNOWN = 3
    BEST_AVAILABLE = 4


class LivelinessPolicy(IntEnum):
    """
    Enum for QoS Liveliness settings.

    This enum matches the one defined in rmw/types.h
    """

    SYSTEM_DEFAULT = 0
    AUTOMATIC = 1
    MANUAL_BY_TOPIC = 3
    UNKNOWN = 4
    BEST_AVAILABLE = 5


@dataclass
class QosDuration:
    sec: int
    nsec: int


QOS_DURATION_DEFAULT = QosDuration(sec=0, nsec=0)
QOS_DURATION_INFINITE = QosDuration(sec=9223372036, nsec=854775807)


@dataclass
class Qos:
    history: HistoryPolicy = HistoryPolicy.KEEP_LAST
    depth: int = 10
    reliability: ReliabilityPolicy = ReliabilityPolicy.BEST_EFFORT
    durability: DurabilityPolicy = DurabilityPolicy.VOLATILE
    deadline: QosDuration = QOS_DURATION_DEFAULT
    lifespan: QosDuration = QOS_DURATION_DEFAULT
    liveliness: LivelinessPolicy = LivelinessPolicy.SYSTEM_DEFAULT
    liveliness_lease_duration: QosDuration = QOS_DURATION_DEFAULT
    avoid_ros_namespace_conventions: bool = False

    def to_yaml(self) -> str:
        d = {
            'history': self.history.name,
            'depth': self.depth,
            'reliability': self.reliability.name,
            'durability': self.durability.name,
            'deadline': self.deadline,
            'lifespan': self.lifespan,
            'liveliness': self.liveliness.name,
            'liveliness_lease_duration': self.liveliness_lease_duration,
            'avoid_ros_namespace_conventions': self.avoid_ros_namespace_conventions,
        }

        return strictyaml.as_document(d).as_yaml()
