from typing import Any, Dict, List, Optional, Union


class KappeError(Exception):
    """Base exception for all kappe-specific errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)

    def __str__(self) -> str:
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} [{details_str}]"
        return self.message


class ConfigError(KappeError):
    """Raised when there's an issue with the configuration."""


class FileError(KappeError):
    """Raised when there's an issue with file operations."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, **kwargs: Any):
        details = kwargs.copy()
        if file_path:
            details["file_path"] = file_path
        super().__init__(message, details)


class ConversionError(KappeError):
    """Raised when there's an error during the conversion process."""


class ValidationError(KappeError):
    """Raised when input validation fails."""
    
    def __init__(self, message: str, errors: Optional[List[str]] = None, **kwargs: Any):
        details = kwargs.copy()
        if errors:
            details["errors"] = errors
        super().__init__(message, details)


class SchemaError(KappeError):
    """Raised when there's an issue with message schemas."""
    
    def __init__(self, message: str, schema_name: Optional[str] = None, **kwargs: Any):
        details = kwargs.copy()
        if schema_name:
            details["schema_name"] = schema_name
        super().__init__(message, details)


class PluginError(KappeError):
    """Raised when there's an issue with plugins."""
    
    def __init__(self, message: str, plugin_name: Optional[str] = None, **kwargs: Any):
        details = kwargs.copy()
        if plugin_name:
            details["plugin_name"] = plugin_name
        super().__init__(message, details)


class MessageError(KappeError):
    """Raised when there's an issue with message processing."""
    
    def __init__(self, message: str, topic: Optional[str] = None, **kwargs: Any):
        details = kwargs.copy()
        if topic:
            details["topic"] = topic
        super().__init__(message, details)


class TimeError(KappeError):
    """Raised when there's an issue with time-related operations."""


class TFError(KappeError):
    """Raised when there's an issue with transformation operations."""


class QoSError(KappeError):
    """Raised when there's an issue with QoS settings."""


class PointCloudError(KappeError):
    """Raised when there's an issue with point cloud processing."""
