import json
import tempfile
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap


class E2EResultOutput(BaseModel):
    """Output of an e2e test run."""

    exception: str
    exception_text: str


def e2e_test_helper(
    input_jsonl: Path,
    command: list[str],
    *,
    post_input_command: list[str] | None = None,
    error_json: Path | None = None,
) -> None:
    """Run an end-to-end test with the given input and config.

    :param input_jsonl: List of messages or path to input JSONL file
    :param command: Command to run
    :param post_input_command: Appended after the input file
    :param error_json: Path to json containing expected exitcodes / stderr
    """
    # Convert to MCAP

    if error_json and error_json.exists():
        output_result = E2EResultOutput(**json.loads(error_json.read_text(encoding='utf-8')))
    else:
        output_result = None

    ctx = pytest.raises(Exception) if output_result else nullcontext()  # noqa: PT011

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_mcap = Path(tmp_dir) / 'input.mcap'
        json_to_mcap(input_mcap, input_jsonl)

        with (
            patch(
                'sys.argv',
                [
                    'kappe',
                    '--progress=false',
                    *command,
                    str(input_mcap),
                    *(post_input_command or []),
                ],
            ),
            ctx as rst,
        ):
            kappe_main()

        if output_result:
            assert rst
            assert rst.value.__class__.__name__ == output_result.exception
            assert output_result.exception_text in str(rst.value)
