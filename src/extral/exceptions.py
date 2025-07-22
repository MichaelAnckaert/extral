"""Custom exceptions for the Extral ETL tool."""

from typing import Optional, Any, Dict


class ExtralException(Exception):
    """Base exception for all Extral-specific errors."""
    
    def __init__(
        self, 
        message: str, 
        pipeline: Optional[str] = None,
        dataset: Optional[str] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.pipeline = pipeline
        self.dataset = dataset
        self.operation = operation
        self.details = details or {}
        
        # Build context message
        context_parts = []
        if pipeline:
            context_parts.append(f"pipeline={pipeline}")
        if dataset:
            context_parts.append(f"dataset={dataset}")
        if operation:
            context_parts.append(f"operation={operation}")
        
        if context_parts:
            context_str = f"[{', '.join(context_parts)}] "
        else:
            context_str = ""
            
        super().__init__(f"{context_str}{message}")
        

class ExtractException(ExtralException):
    """Raised when data extraction fails."""
    pass


class LoadException(ExtralException):
    """Raised when data loading fails."""
    pass


class ConnectionException(ExtralException):
    """Raised when database connection fails."""
    pass


class ConfigurationException(ExtralException):
    """Raised when configuration is invalid or missing."""
    pass


class StateException(ExtralException):
    """Raised when state management operations fail."""
    pass


class ValidationException(ExtralException):
    """Raised when validation checks fail."""
    pass


class RetryableException(ExtralException):
    """Base class for exceptions that can be retried."""
    
    def __init__(
        self,
        message: str,
        max_retries: int = 3,
        **kwargs: Any
    ):
        super().__init__(message, **kwargs)
        self.max_retries = max_retries