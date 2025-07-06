import json
from io import StringIO
from pathlib import Path

import pytest

from kappe.utils.mcap_to_json import mcap_to_json


def test_file_not_found():
    """Test that FileNotFoundError is raised for non-existent files."""
    non_existent_file = Path('/non/existent/file.mcap')
    output_buffer = StringIO()

    with pytest.raises(FileNotFoundError, match='MCAP file not found'):
        mcap_to_json(non_existent_file, output_buffer)


def test_basic_conversion_with_valid_mcap(tmp_path: Path):
    """Test basic MCAP to JSONL conversion using existing test data."""
    # Use existing test data
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    # Create temporary MCAP file from existing JSONL
    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Convert back to JSONL
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer)

    # Verify output format
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) > 0

    # Each line should be valid JSON
    for line in lines:
        message = json.loads(line.strip())
        assert 'topic' in message
        assert 'log_time' in message
        assert 'publish_time' in message
        assert 'sequence' in message
        assert 'datatype' in message
        assert 'message' in message


def test_topic_filtering(tmp_path: Path):
    """Test topic filtering functionality."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Filter for specific topic
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=['/test_topic'])

    output_buffer.seek(0)
    lines = output_buffer.readlines()

    # All messages should be from the specified topic
    for line in lines:
        message = json.loads(line.strip())
        assert message['topic'] == '/test_topic'


def test_message_limit(tmp_path: Path):
    """Test message limit functionality."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Limit to 1 message
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=1)

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) <= 1


def test_empty_topics_list(tmp_path: Path):
    """Test behavior with empty topics list."""
    input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
    if not input_jsonl.exists():
        pytest.skip('Test data not available')

    from kappe.utils.json_to_mcap import json_to_mcap

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Empty topics list should return no messages (filters out all)
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=[])

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 0
