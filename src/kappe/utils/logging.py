import logging
import sys
import traceback
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, TextIO, Union

from tqdm import tqdm


class LogLevel(str, Enum):
    """Log level enum for configuration."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class TqdmLoggingHandler(logging.Handler):
    """
    Logging handler for tqdm progress bars.
    
    Ensures that log messages don't interfere with progress bar display.
    """
    
    def emit(self, record: Any) -> None:
        """Write log message compatible with tqdm progress bars."""
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


class FileRotationHandler(logging.Handler):
    """
    A custom logging handler that implements log rotation based on file size.
    
    Args:
        filename: The log file to write to
        max_bytes: Maximum file size before rotation (default: 10MB)
        backup_count: Number of backup files to keep (default: 5)
    """
    
    def __init__(self, filename: Union[str, Path], max_bytes: int = 10*1024*1024, 
                 backup_count: int = 5, mode: str = 'a', encoding: Optional[str] = None):
        super().__init__()
        self.filename = Path(filename)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.mode = mode
        self.encoding = encoding
        self.stream: Optional[TextIO] = None
        
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        
    def emit(self, record: logging.LogRecord) -> None:
        """Write formatted log record to file with rotation if needed."""
        if self.stream is None:
            self.stream = open(self.filename, self.mode, encoding=self.encoding)
            
        msg = self.format(record)
        self.stream.write(f"{msg}\n")
        self.stream.flush()
        
        if self.stream.tell() >= self.max_bytes:
            self.stream.close()
            self.rotate_logs()
            self.stream = open(self.filename, self.mode, encoding=self.encoding)
    
    def rotate_logs(self) -> None:
        """Rotate log files, removing oldest if needed."""
        if self.backup_count > 0:
            # Remove oldest log file if it exists
            oldest = self.filename.with_suffix(f"{self.filename.suffix}.{self.backup_count}")
            if oldest.exists():
                oldest.unlink()
                
            for i in range(self.backup_count - 1, 0, -1):
                source = self.filename.with_suffix(f"{self.filename.suffix}.{i}")
                target = self.filename.with_suffix(f"{self.filename.suffix}.{i+1}")
                if source.exists():
                    source.rename(target)
            
            if self.filename.exists():
                self.filename.rename(self.filename.with_suffix(f"{self.filename.suffix}.1"))
    
    def close(self) -> None:
        """Close the file stream."""
        if self.stream is not None:
            self.stream.close()
            self.stream = None


def setup_logging(
    level: Union[str, LogLevel] = LogLevel.INFO,
    log_file: Optional[Union[str, Path]] = None,
    log_format: str = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    date_format: str = "%Y-%m-%d %H:%M:%S",
    show_line_number: bool = False,
    capture_warnings: bool = True,
) -> None:
    """
    Configure application-wide logging.
    
    Args:
        level: The minimum logging level to display
        log_file: Optional path to a log file (enables file logging)
        log_format: The format string for log messages
        date_format: The format string for timestamps
        show_line_number: Whether to include filename and line number in logs
        capture_warnings: Whether to capture warnings through the warnings module
    """
    if isinstance(level, str):
        level = LogLevel(level.upper())
    
    numeric_level = getattr(logging, level.upper())
    
    if show_line_number:
        log_format = "%(asctime)s | %(levelname)-7s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    
    handlers = [TqdmLoggingHandler()]
    
    if log_file:
        file_handler = FileRotationHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(file_handler)
    
    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
    )
    
    if capture_warnings:
        logging.captureWarnings(True)
        
    logger = logging.getLogger(__name__)
    logger.debug(f"Logging configured: level={level}, file={log_file}")


def log_exception(logger: logging.Logger, 
                  message: str, 
                  exc_info: Optional[Exception] = None,
                  level: int = logging.ERROR) -> None:
    """
    Log an exception with detailed traceback information.
    
    Args:
        logger: The logger instance to use
        message: The message to log
        exc_info: The exception info (uses sys.exc_info() if None)
        level: The log level to use (default: ERROR)
    """
    if exc_info is None:
        exc_type, exc_value, exc_traceback = sys.exc_info()
    else:
        exc_type = type(exc_info)
        exc_value = exc_info
        exc_traceback = exc_info.__traceback__
    
    if exc_type is not None and exc_value is not None and exc_traceback is not None:
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        tb_text = ''.join(tb_lines)
        logger.log(level, f"{message}\n{tb_text}")
    else:
        logger.log(level, f"{message} (no exception info available)")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    This is a convenience function that should be used instead of logging.getLogger()
    to ensure consistent logger configuration.
    
    Args:
        name: The logger name, typically __name__
        
    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)
