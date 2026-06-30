from kappe.module.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    ReliabilityPolicy,
    dump_qos_list,
    parse_qos_list,
)

# ROS 2 Iron+/Jazzy use string policy names; Humble and older use integers.
JAZZY = '- {history: keep_last, reliability: reliable, durability: transient_local}'
LEGACY = '- {history: 1, reliability: 1, durability: 1}'


def test_parses_both_forms():
    """String names (Jazzy) and integers (Humble) parse to the same Qos."""
    qos = parse_qos_list(JAZZY)[0]
    assert (qos.history, qos.reliability, qos.durability) == (
        HistoryPolicy.KEEP_LAST,
        ReliabilityPolicy.RELIABLE,
        DurabilityPolicy.TRANSIENT_LOCAL,
    )
    assert parse_qos_list(JAZZY) == parse_qos_list(LEGACY)


def test_roundtrip_writes_integers():
    """kappe writes integers (readable on every distro); re-parsing is stable."""
    qos = parse_qos_list(JAZZY)
    dumped = dump_qos_list(qos)
    assert 'keep_last' not in dumped and 'history: 1' in dumped
    assert parse_qos_list(dumped) == qos
