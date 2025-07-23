import logging
import os
import sys
import traceback
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TextIO, Type, TypeVar, Union, cast

from kappe.utils.exceptions import KappeError

# Type variables for function decorators
F = TypeVar("F", bound=Callable[..., Any])
R = TypeVar("R")


def handle_errors(
    logger: Optional[logging.Logger] = None,
    exit_on_error: bool = False,
    exit_code: int = 1,
    show_traceback: bool = False,
    reraise: bool = False,
    expected_exceptions: Optional[List[Type[Exception]]] = None,
) -> Callable[[F], F]:
    """
    Decorator to handle exceptions in a consistent way.
    
    Args:
        logger: The logger to use for error messages
        exit_on_error: Whether to exit the program on error
        exit_code: The exit code to use when exiting
        show_traceback: Whether to show the full traceback in logs
        reraise: Whether to re-raise the exception after handling
        expected_exceptions: List of exception types that should be handled 
                            without treating them as unexpected errors
    
    Returns:
        A decorator function
    """
    expected_exceptions = expected_exceptions or [KappeError]
    
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal logger
            
            if logger is None:
                logger = logging.getLogger(func.__module__)
                
            try:
                return func(*args, **kwargs)
            except tuple(expected_exceptions) as e:
                if show_traceback:
                    logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
                else:
                    logger.error(f"Error in {func.__name__}: {str(e)}")
                
                if exit_on_error:
                    sys.exit(exit_code)
                
                if reraise:
                    raise
                
                return None
            except Exception as e:
                logger.critical(
                    f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True
                )
                
                if exit_on_error:
                    sys.exit(exit_code)
                
                if reraise:
                    raise
                
                return None
                
        return cast(F, wrapper)
    
    return decorator


def error_boundary(
    func: Callable[..., R],
    logger: Optional[logging.Logger] = None,
    default_return: Optional[R] = None,
    error_callback: Optional[Callable[[Exception], None]] = None,
) -> Callable[..., Optional[R]]:
    """
    Create an error boundary around a function.
    
    This is similar to handle_errors but is applied directly to a function call
    rather than decorating the function definition.
    
    Args:
        func: The function to call
        logger: The logger to use for error messages
        default_return: The value to return if an error occurs
        error_callback: A callback function to call with the exception
    
    Returns:
        A wrapped function that handles errors
    """
    if logger is None:
        logger = logging.getLogger(func.__module__)
        
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Optional[R]:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            
            if error_callback is not None:
                error_callback(e)
                
            return default_return
            
    return wrapper


def capture_exceptions(file_path: Optional[Union[str, Path]] = None) -> None:
    """
    Set up global exception handling to capture uncaught exceptions.
    
    Args:
        file_path: Optional path to write exceptions to (in addition to logging)
    """
    logger = logging.getLogger(__name__)
    
    def exception_handler(
        exc_type: Type[BaseException], 
        exc_value: BaseException, 
        exc_traceback: Optional[traceback.TracebackType]
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
            
        logger.critical(
            "Uncaught exception", 
            exc_info=(exc_type, exc_value, exc_traceback)
        )
        
        if file_path:
            try:
                path = Path(file_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(path, "a") as f:
                    f.write(f"\n{'=' * 80}\n")
                    f.write(f"UNCAUGHT EXCEPTION: {exc_value}\n")
                    if exc_traceback:
                        traceback.print_exception(
                            exc_type, exc_value, exc_traceback, file=f
                        )
            except Exception as e:
                logger.error(f"Failed to write exception to file: {e}")
    
    sys.excepthook = exception_handler
