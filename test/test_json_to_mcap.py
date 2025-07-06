import json
from pathlib import Path

import pytest

from kappe.utils.json_to_mcap import McapJson, Message, json_to_mcap, load_json_and_validate


class TestLoadJsonAndValidate:
    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for non-existent files."""
        non_existent_file = Path('/non/existent/file.jsonl')

        with pytest.raises(FileNotFoundError, match='File not found'):
            load_json_and_validate(non_existent_file)

    def test_invalid_file_extension(self, tmp_path: Path):
        """Test that ValueError is raised for non-JSONL files."""
        invalid_file = tmp_path / 'test.json'
        invalid_file.write_text('{"test": "data"}')

        with pytest.raises(ValueError, match='File must be a JSONL file'):
            load_json_and_validate(invalid_file)

    def test_invalid_json_format(self, tmp_path: Path):
        """Test that ValueError is raised for invalid JSON."""
        invalid_jsonl = tmp_path / 'invalid.jsonl'
        invalid_jsonl.write_text('invalid json content')

        with pytest.raises(ValueError, match='Invalid JSON in file'):
            load_json_and_validate(invalid_jsonl)

    def test_empty_file(self, tmp_path: Path):
        """Test that ValueError is raised for empty files."""
        empty_file = tmp_path / 'empty.jsonl'
        empty_file.write_text('')

        with pytest.raises(ValueError, match='No valid messages found'):
            load_json_and_validate(empty_file)

    def test_valid_jsonl_loading(self, tmp_path: Path):
        """Test successful loading of valid JSONL file."""
        valid_jsonl = tmp_path / 'valid.jsonl'
        message_data = {
            'topic': '/test_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': True},
        }
        valid_jsonl.write_text(json.dumps(message_data))

        result = load_json_and_validate(valid_jsonl)

        assert isinstance(result, McapJson)
        assert len(result.messages) == 1
        assert result.messages[0].topic == '/test_topic'
        assert result.messages[0].datatype == 'std_msgs/msg/Bool'

    def test_invalid_message_data(self, tmp_path: Path):
        """Test that ValueError is raised for invalid message structure."""
        invalid_jsonl = tmp_path / 'invalid_msg.jsonl'
        # Missing required fields
        invalid_message = {'topic': '/test'}
        invalid_jsonl.write_text(json.dumps(invalid_message))

        with pytest.raises(ValueError, match='Error parsing message data'):
            load_json_and_validate(invalid_jsonl)

    def test_multiple_messages(self, tmp_path: Path):
        """Test loading multiple messages from JSONL."""
        multi_jsonl = tmp_path / 'multi.jsonl'
        messages = []
        for i in range(3):
            message_data = {
                'topic': f'/test_topic_{i}',
                'log_time': 1234567890 + i,
                'publish_time': 1234567890 + i,
                'sequence': i,
                'datatype': 'std_msgs/msg/Bool',
                'message': {'data': i % 2 == 0},
            }
            messages.append(json.dumps(message_data))

        multi_jsonl.write_text('\n'.join(messages))

        result = load_json_and_validate(multi_jsonl)

        assert len(result.messages) == 3
        assert all(msg.datatype == 'std_msgs/msg/Bool' for msg in result.messages)


class TestJsonToMcap:
    def test_basic_conversion(self, tmp_path: Path):
        """Test basic JSONL to MCAP conversion."""
        # Create test JSONL file
        test_jsonl = tmp_path / 'test.jsonl'
        message_data = {
            'topic': '/test_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': True},
        }
        test_jsonl.write_text(json.dumps(message_data))

        # Convert to MCAP
        output_mcap = tmp_path / 'output.mcap'
        json_to_mcap(output_mcap, test_jsonl)

        # Verify MCAP file was created
        assert output_mcap.exists()
        assert output_mcap.stat().st_size > 0

    def test_file_not_found(self, tmp_path: Path):
        """Test error handling for non-existent input file."""
        non_existent_jsonl = tmp_path / 'non_existent.jsonl'
        output_mcap = tmp_path / 'output.mcap'

        with pytest.raises(FileNotFoundError):
            json_to_mcap(output_mcap, non_existent_jsonl)

    def test_invalid_message_definition(self, tmp_path: Path):
        """Test error handling for unknown message types."""
        test_jsonl = tmp_path / 'test.jsonl'
        message_data = {
            'topic': '/test_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'unknown_msgs/msg/UnknownType',
            'message': {'data': True},
        }
        test_jsonl.write_text(json.dumps(message_data))

        output_mcap = tmp_path / 'output.mcap'

        with pytest.raises(ValueError, match='Message definition for .* not found'):
            json_to_mcap(output_mcap, test_jsonl)

    def test_multiple_message_types(self, tmp_path: Path):
        """Test conversion with multiple message types."""
        test_jsonl = tmp_path / 'test.jsonl'
        messages = [
            {
                'topic': '/bool_topic',
                'log_time': 1234567890,
                'publish_time': 1234567890,
                'sequence': 0,
                'datatype': 'std_msgs/msg/Bool',
                'message': {'data': True},
            },
            {
                'topic': '/bool_topic',
                'log_time': 1234567891,
                'publish_time': 1234567891,
                'sequence': 1,
                'datatype': 'std_msgs/msg/Bool',
                'message': {'data': False},
            },
        ]

        test_jsonl.write_text('\n'.join(json.dumps(msg) for msg in messages))

        output_mcap = tmp_path / 'output.mcap'
        json_to_mcap(output_mcap, test_jsonl)

        assert output_mcap.exists()
        assert output_mcap.stat().st_size > 0

    def test_round_trip_conversion(self, tmp_path: Path):
        """Test round-trip conversion: JSONL -> MCAP -> JSONL."""
        # Use existing test data if available
        input_jsonl = Path('test/e2e/simple_pass/simple_pass.input.jsonl')
        if not input_jsonl.exists():
            pytest.skip('Test data not available')

        # Convert to MCAP
        temp_mcap = tmp_path / 'temp.mcap'
        json_to_mcap(temp_mcap, input_jsonl)

        # Convert back to JSONL
        from io import StringIO

        from kappe.utils.mcap_to_json import mcap_to_json

        output_buffer = StringIO()
        mcap_to_json(temp_mcap, output_buffer)

        # Verify we get valid JSON output
        output_buffer.seek(0)
        lines = output_buffer.readlines()
        assert len(lines) > 0

        # Each line should be valid JSON with expected structure
        for line in lines:
            message = json.loads(line.strip())
            assert all(
                key in message
                for key in ['topic', 'log_time', 'publish_time', 'sequence', 'datatype', 'message']
            )


class TestMessageModels:
    def test_message_model_validation(self):
        """Test Message model validation."""
        valid_data = {
            'topic': '/test_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': True},
        }

        message = Message(**valid_data)
        assert message.topic == '/test_topic'
        assert message.datatype == 'std_msgs/msg/Bool'

    def test_message_model_invalid_data(self):
        """Test Message model with invalid data."""
        invalid_data = {
            'topic': '/test_topic',
            # Missing required fields
        }

        with pytest.raises(Exception):  # Pydantic validation error
            Message(**invalid_data)

    def test_mcap_json_model(self):
        """Test McapJson model validation."""
        message_data = {
            'topic': '/test_topic',
            'log_time': 1234567890,
            'publish_time': 1234567890,
            'sequence': 0,
            'datatype': 'std_msgs/msg/Bool',
            'message': {'data': True},
        }

        mcap_json = McapJson(messages=[Message(**message_data)])
        assert len(mcap_json.messages) == 1
        assert mcap_json.messages[0].topic == '/test_topic'
