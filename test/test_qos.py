import pytest

from kappe.module.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    LivelinessPolicy,
    Qos,
    QosDuration,
    ReliabilityPolicy,
    dump_qos_list,
    parse_qos_list,
)


def test_parse_qos_list_single():
    """Test parsing a single QoS profile from YAML."""
    yaml_str = """
- history: 1
  depth: 10
  reliability: 2
  durability: 2
"""
    result = parse_qos_list(yaml_str)
    assert len(result) == 1
    assert result[0].history == HistoryPolicy.KEEP_LAST
    assert result[0].depth == 10
    assert result[0].reliability == ReliabilityPolicy.BEST_EFFORT
    assert result[0].durability == DurabilityPolicy.VOLATILE


def test_parse_qos_list_multiple():
    """Test parsing multiple QoS profiles from YAML."""
    yaml_str = """
- history: 1
  depth: 10
  reliability: 1
  durability: 1
- history: 2
  depth: 20
  reliability: 2
  durability: 2
"""
    result = parse_qos_list(yaml_str)
    assert len(result) == 2
    assert result[0].history == HistoryPolicy.KEEP_LAST
    assert result[0].reliability == ReliabilityPolicy.RELIABLE
    assert result[1].history == HistoryPolicy.KEEP_ALL
    assert result[1].reliability == ReliabilityPolicy.BEST_EFFORT


def test_dump_qos_list_single():
    """Test dumping a single QoS profile to YAML."""
    qos = Qos(
        history=HistoryPolicy.KEEP_LAST,
        depth=5,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )
    result = dump_qos_list(qos)
    assert 'history: 1' in result
    assert 'depth: 5' in result
    assert 'reliability: 1' in result
    assert 'durability: 1' in result


def test_dump_qos_list_multiple():
    """Test dumping multiple QoS profiles to YAML."""
    qos_list = [
        Qos(history=HistoryPolicy.KEEP_LAST, depth=10),
        Qos(history=HistoryPolicy.KEEP_ALL, depth=20),
    ]
    result = dump_qos_list(qos_list)
    # Should be a YAML list with two items
    assert result.count('- ') == 2
    assert 'depth: 10' in result
    assert 'depth: 20' in result


@pytest.mark.parametrize(
    'qos_data',
    [
        pytest.param(
            Qos(history=HistoryPolicy.SYSTEM_DEFAULT, depth=0),
            id='system_default',
        ),
        pytest.param(
            Qos(history=HistoryPolicy.KEEP_LAST, depth=1),
            id='keep_last_1',
        ),
        pytest.param(
            Qos(history=HistoryPolicy.KEEP_ALL, depth=100),
            id='keep_all_100',
        ),
        pytest.param(
            Qos(reliability=ReliabilityPolicy.BEST_EFFORT),
            id='best_effort',
        ),
        pytest.param(
            Qos(durability=DurabilityPolicy.TRANSIENT_LOCAL),
            id='transient_local',
        ),
        pytest.param(
            Qos(
                history=HistoryPolicy.KEEP_LAST,
                depth=15,
                reliability=ReliabilityPolicy.RELIABLE,
                durability=DurabilityPolicy.VOLATILE,
                liveliness=LivelinessPolicy.AUTOMATIC,
            ),
            id='roundtrip_all_fields',
        ),
    ],
)
def test_qos_configurations(qos_data: Qos):
    """Test various QoS configurations."""
    yaml_str = dump_qos_list(qos_data)
    parsed = parse_qos_list(yaml_str)
    assert len(parsed) == 1
    assert parsed[0].history == qos_data.history
    assert parsed[0].depth == qos_data.depth
    assert parsed[0].reliability == qos_data.reliability
    assert parsed[0].durability == qos_data.durability
    assert parsed[0].liveliness == qos_data.liveliness


def test_qos_with_durations():
    """Test QoS with deadline and lifespan durations."""
    qos = Qos(
        deadline=QosDuration(sec=1, nsec=500000000),
        lifespan=QosDuration(sec=2, nsec=0),
    )
    yaml_str = dump_qos_list(qos)
    parsed = parse_qos_list(yaml_str)
    assert len(parsed) == 1
    assert parsed[0].deadline.sec == 1
    assert parsed[0].deadline.nsec == 500000000
    assert parsed[0].lifespan.sec == 2
    assert parsed[0].lifespan.nsec == 0
