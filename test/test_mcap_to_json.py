import json
from io import StringIO
from pathlib import Path

import pytest

from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json

from .conftest import (
    create_test_data_message,
    create_test_jsonl,
)


def test_file_not_found():
    """Test that FileNotFoundError is raised for non-existent files."""
    non_existent_file = Path('/non/existent/file.mcap')
    output_buffer = StringIO()

    with pytest.raises(FileNotFoundError, match='MCAP file not found'):
        mcap_to_json(non_existent_file, output_buffer)


def test_topic_filtering(tmp_path: Path) -> None:
    """Test topic filtering functionality."""
    # Create test data with multiple topics
    filter_topic = '/test_topic'
    messages = [
        create_test_data_message(topic='/test_topic', sequence=0),
        create_test_data_message(topic='/other_topic', sequence=1),
        create_test_data_message(topic='/test_topic', sequence=2),
    ]

    input_jsonl = tmp_path / 'multi_topic.jsonl'
    create_test_jsonl(messages, input_jsonl)

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Filter for specific topic
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=[filter_topic])

    output_buffer.seek(0)
    lines = output_buffer.readlines()

    expected_messages = [msg for msg in messages if msg['topic'] == filter_topic]
    for expected_line, line in zip(expected_messages, lines, strict=True):
        message = json.loads(line.strip())
        assert message == expected_line


@pytest.mark.parametrize('topics', [[], ['/non_existent_topic']])
def test_empty_topics_list(tmp_path: Path, sample_bool_message: dict, topics: list[str]) -> None:
    """Test behavior with empty topics list."""
    input_jsonl = tmp_path / 'test.jsonl'
    input_jsonl.write_text(json.dumps(sample_bool_message))

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    # Empty topics list should return no messages (filters out all)
    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, topics=topics)

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 0


def test_invalid_mcap_file(tmp_path: Path):
    """Test handling of invalid MCAP file."""
    # Create a file that's not a valid MCAP
    invalid_mcap = tmp_path / 'invalid.mcap'
    invalid_mcap.write_text('This is not a valid MCAP file')

    output_buffer = StringIO()

    # This should raise an error for invalid MCAP format
    with pytest.raises(RuntimeError, match='Error reading MCAP file'):
        mcap_to_json(invalid_mcap, output_buffer)


def test_empty_mcap_file(tmp_path: Path):
    """Test handling of empty MCAP file."""
    # Create an empty MCAP file
    empty_mcap = tmp_path / 'empty.mcap'
    empty_mcap.write_bytes(b'')

    output_buffer = StringIO()

    # This should raise an error for empty MCAP file
    with pytest.raises(RuntimeError, match='Error reading MCAP file'):
        mcap_to_json(empty_mcap, output_buffer)


def test_valid_empty_mcap_file(tmp_path: Path):
    """Test handling of valid MCAP file with no messages (header and footer only)."""
    # Create an empty JSONL file
    empty_jsonl = tmp_path / 'empty.jsonl'
    empty_jsonl.write_text('')

    # Convert to MCAP - this creates a valid MCAP with header and footer but no messages
    temp_mcap = tmp_path / 'valid_empty.mcap'
    json_to_mcap(temp_mcap, empty_jsonl)

    output_buffer = StringIO()

    # This should succeed (no exceptions) for a valid empty MCAP
    mcap_to_json(temp_mcap, output_buffer)

    # Verify the output is empty (no messages)
    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == 0


@pytest.mark.parametrize(('limit', 'expected_count'), [(1, 1), (0, 3)])
def test_message_limit(tmp_path: Path, limit: int, expected_count: int) -> None:
    """Test message limit functionality."""
    # Create test data with multiple messages
    messages = [
        create_test_data_message(sequence=0),
        create_test_data_message(sequence=1),
        create_test_data_message(sequence=2),
    ]

    input_jsonl = tmp_path / 'multi_message.jsonl'
    create_test_jsonl(messages, input_jsonl)

    temp_mcap = tmp_path / 'test.mcap'
    json_to_mcap(temp_mcap, input_jsonl)

    output_buffer = StringIO()
    mcap_to_json(temp_mcap, output_buffer, limit=limit)

    output_buffer.seek(0)
    lines = output_buffer.readlines()
    assert len(lines) == expected_count

    for i in range(expected_count):
        assert messages[i] == json.loads(lines[i].strip())
