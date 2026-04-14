import subprocess
import sys
import tempfile
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from kappe.cli import KappeCLI
from kappe.cli import main as kappe_main
from kappe.utils.json_to_mcap import json_to_mcap

# Test constants
EXPECTED_HELP_KEYWORDS = ['usage:', 'kappe']


def test_main_module_execution():
    """Test that the main module can be executed."""
    # Test that we can import the main module without errors
    kappe_path = Path(__file__).parent.parent / 'src' / 'kappe'
    result = subprocess.run(  # noqa: S603
        [sys.executable, '-m', 'kappe', '--help'],
        capture_output=True,
        text=True,
        cwd=kappe_path.parent,
        check=False,
    )

    # Should not fail with import errors
    assert result.returncode == 0
    assert any(keyword in result.stdout.lower() for keyword in EXPECTED_HELP_KEYWORDS)


def test_main_module_import():
    """Test that the main module can be imported."""
    # This should not raise an ImportError
    from kappe import __main__  # noqa: F401


def test_version_command():
    """Test that the version command outputs version information."""
    cli = KappeCLI()
    output = StringIO()

    with patch('sys.stdout', output):
        cli.version()

    result = output.getvalue()
    assert 'Kappe' in result
    assert 'Python' in result


def test_single_file_output():
    """Test that single file input with file output writes directly to output path."""
    input_jsonl = (
        Path(__file__).parent / 'e2e' / 'convert' / 'simple_pass' / 'simple_pass.input.jsonl'
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        input_mcap = tmp_path / 'input.mcap'
        output_mcap = tmp_path / 'output.mcap'

        # Create input MCAP
        json_to_mcap(input_mcap, input_jsonl)

        # Run conversion with file output
        with patch(
            'sys.argv',
            ['kappe', '--progress=false', 'convert', str(input_mcap), str(output_mcap)],
        ):
            kappe_main()

        # Check that output.mcap exists directly (not output.mcap/input.mcap)
        assert output_mcap.exists()
        assert output_mcap.is_file()
        assert not (output_mcap / 'input.mcap').exists()


def test_single_file_output_directory():
    """Test that single file input with directory output appends filename."""
    input_jsonl = (
        Path(__file__).parent / 'e2e' / 'convert' / 'simple_pass' / 'simple_pass.input.jsonl'
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        input_mcap = tmp_path / 'input.mcap'
        output_dir = tmp_path / 'output_dir'

        # Create input MCAP
        json_to_mcap(input_mcap, input_jsonl)

        # Run conversion with directory output
        with patch(
            'sys.argv',
            ['kappe', '--progress=false', 'convert', str(input_mcap), str(output_dir)],
        ):
            kappe_main()

        # Check that output_dir/input.mcap exists
        assert (output_dir / 'input.mcap').exists()
        assert (output_dir / 'input.mcap').is_file()
