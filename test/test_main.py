import subprocess
import sys
from pathlib import Path

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
