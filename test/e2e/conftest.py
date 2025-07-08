import json
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import patch

from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap
from kappe.utils.mcap_to_json import mcap_to_json


def e2e_test_helper(
    input_messages: list[dict[str, Any]] | Path,
    config_content: str | Path,
    tmp_path: Path,
    command: str = 'convert',
) -> list[dict[str, Any]] | Path:
    """Run an end-to-end test with the given input and config.

    Args:
        input_messages: List of messages or path to input JSONL file
        config_content: Config content string or path to config file
        tmp_path: Temporary directory path
        command: Command to run ('convert' or 'cut')

    Returns:
        List of output messages for 'convert' command
    """
    # Handle input - can be messages or file path
    if isinstance(input_messages, Path):
        input_jsonl = input_messages
    else:
        input_jsonl = tmp_path / 'input.jsonl'
        with input_jsonl.open('w', encoding='utf-8') as f:
            for message in input_messages:
                f.write(json.dumps(message))
                f.write('\n')

    # Handle config - can be content string or file path
    if isinstance(config_content, Path):
        config_file = config_content
    else:
        config_file = tmp_path / 'config.yaml'
        config_file.write_text(config_content)

    # Convert to MCAP
    input_mcap = tmp_path / 'input.mcap'
    json_to_mcap(input_mcap, input_jsonl)

    # Run kappe command
    output_dir = tmp_path / 'output'
    output_dir.mkdir()

    if command == 'cut':
        argv = [
            'kappe',
            '--progress=false',
            'cut',
            '--config',
            str(config_file),
            '--output',
            str(output_dir),
            '--overwrite=true',
            str(input_mcap),
        ]
    else:
        argv = [
            'kappe',
            '--progress=false',
            command,
            '--config',
            str(config_file),
            str(input_mcap),
            str(output_dir),
        ]

    with patch('sys.argv', argv):
        kappe_main()

    # For cut command, return the output directory (caller handles verification)
    if command == 'cut':
        return output_dir

    # For convert command, read and return the output messages
    output_mcap = output_dir / 'input.mcap'
    output_buffer = StringIO()
    mcap_to_json(output_mcap, output_buffer)

    # Parse results
    output_buffer.seek(0)
    return [json.loads(line.strip()) for line in output_buffer.readlines()]
