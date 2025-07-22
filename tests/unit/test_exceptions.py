# Copyright 2025 Michael Anckaert
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unit tests for exceptions module.
"""

import pytest

from extral.exceptions import (
    ExtralException,
    ExtractException,
    LoadException,
    ConnectionException,
    ConfigurationException,
    StateException,
    ValidationException,
    RetryableException
)


class TestExtralException:
    """Test ExtralException base class."""
    
    def test_extral_exception_basic(self):
        """Test basic ExtralException creation."""
        exception = ExtralException("Test error message")
        
        assert str(exception) == "Test error message"
        assert exception.pipeline is None
        assert exception.dataset is None
        assert exception.operation is None
        assert exception.details == {}
    
    def test_extral_exception_with_context(self):
        """Test ExtralException with context information."""
        exception = ExtralException(
            "Error occurred",
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract",
            details={"key": "value"}
        )
        
        assert exception.pipeline == "test_pipeline"
        assert exception.dataset == "test_table"
        assert exception.operation == "extract"
        assert exception.details == {"key": "value"}
        assert str(exception) == "[pipeline=test_pipeline, dataset=test_table, operation=extract] Error occurred"
    
    def test_extral_exception_partial_context(self):
        """Test ExtralException with partial context information."""
        exception = ExtralException(
            "Error occurred",
            pipeline="test_pipeline",
            operation="load"
            # No dataset
        )
        
        assert str(exception) == "[pipeline=test_pipeline, operation=load] Error occurred"
    
    def test_extral_exception_only_pipeline_context(self):
        """Test ExtralException with only pipeline context."""
        exception = ExtralException(
            "Error occurred",
            pipeline="test_pipeline"
        )
        
        assert str(exception) == "[pipeline=test_pipeline] Error occurred"
    
    def test_extral_exception_only_operation_context(self):
        """Test ExtralException with only operation context."""
        exception = ExtralException(
            "Error occurred",
            operation="extract"
        )
        
        assert str(exception) == "[operation=extract] Error occurred"


class TestExtractException:
    """Test ExtractException class."""
    
    def test_extract_exception_inheritance(self):
        """Test that ExtractException inherits from ExtralException."""
        exception = ExtractException("Extract failed")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, ExtractException)
    
    def test_extract_exception_with_context(self):
        """Test ExtractException with context."""
        exception = ExtractException(
            "Data extraction failed",
            pipeline="etl_pipeline",
            dataset="users_table",
            operation="extract",
            details={"batch_size": 1000, "records_processed": 500}
        )
        
        assert exception.pipeline == "etl_pipeline"
        assert exception.dataset == "users_table"
        assert exception.operation == "extract"
        assert exception.details["batch_size"] == 1000
        assert exception.details["records_processed"] == 500


class TestLoadException:
    """Test LoadException class."""
    
    def test_load_exception_inheritance(self):
        """Test that LoadException inherits from ExtralException."""
        exception = LoadException("Load failed")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, LoadException)
    
    def test_load_exception_with_context(self):
        """Test LoadException with context."""
        exception = LoadException(
            "Data loading failed",
            pipeline="etl_pipeline", 
            dataset="target_table",
            operation="load",
            details={"strategy": "merge", "merge_key": "id"}
        )
        
        assert exception.pipeline == "etl_pipeline"
        assert exception.dataset == "target_table"
        assert exception.operation == "load"
        assert exception.details["strategy"] == "merge"
        assert exception.details["merge_key"] == "id"


class TestConnectionException:
    """Test ConnectionException class."""
    
    def test_connection_exception_inheritance(self):
        """Test that ConnectionException inherits from ExtralException."""
        exception = ConnectionException("Connection failed")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, ConnectionException)
    
    def test_connection_exception_with_context(self):
        """Test ConnectionException with context."""
        exception = ConnectionException(
            "Database connection failed",
            pipeline="db_pipeline",
            operation="connect",
            details={
                "host": "localhost",
                "port": 3306,
                "database": "test_db",
                "error_code": 2003
            }
        )
        
        assert exception.pipeline == "db_pipeline"
        assert exception.operation == "connect"
        assert exception.details["host"] == "localhost"
        assert exception.details["port"] == 3306
        assert exception.details["database"] == "test_db"
        assert exception.details["error_code"] == 2003


class TestConfigurationException:
    """Test ConfigurationException class."""
    
    def test_configuration_exception_inheritance(self):
        """Test that ConfigurationException inherits from ExtralException."""
        exception = ConfigurationException("Configuration error")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, ConfigurationException)
    
    def test_configuration_exception_with_context(self):
        """Test ConfigurationException with context.""" 
        exception = ConfigurationException(
            "Invalid configuration parameter",
            operation="validation",
            details={"parameter": "batch_size", "value": -100, "expected": "positive integer"}
        )
        
        assert exception.operation == "validation"
        assert exception.details["parameter"] == "batch_size"
        assert exception.details["value"] == -100
        assert exception.details["expected"] == "positive integer"


class TestStateException:
    """Test StateException class."""
    
    def test_state_exception_inheritance(self):
        """Test that StateException inherits from ExtralException."""
        exception = StateException("State error")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, StateException)
    
    def test_state_exception_with_context(self):
        """Test StateException with context."""
        exception = StateException(
            "Failed to save state",
            pipeline="test_pipeline",
            operation="save_state",
            details={"state_file": "/path/to/state.json", "error": "Permission denied"}
        )
        
        assert exception.pipeline == "test_pipeline" 
        assert exception.operation == "save_state"
        assert exception.details["state_file"] == "/path/to/state.json"
        assert exception.details["error"] == "Permission denied"


class TestValidationException:
    """Test ValidationException class."""
    
    def test_validation_exception_inheritance(self):
        """Test that ValidationException inherits from ExtralException."""
        exception = ValidationException("Validation error")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, ValidationException)
    
    def test_validation_exception_with_context(self):
        """Test ValidationException with context."""
        exception = ValidationException(
            "Pipeline validation failed",
            pipeline="invalid_pipeline",
            operation="validate",
            details={"validation_type": "connectivity", "endpoint": "mysql://localhost:3306"}
        )
        
        assert exception.pipeline == "invalid_pipeline"
        assert exception.operation == "validate" 
        assert exception.details["validation_type"] == "connectivity"
        assert exception.details["endpoint"] == "mysql://localhost:3306"


class TestRetryableException:
    """Test RetryableException class."""
    
    def test_retryable_exception_inheritance(self):
        """Test that RetryableException inherits from ExtralException."""
        exception = RetryableException("Retryable error")
        
        assert isinstance(exception, ExtralException)
        assert isinstance(exception, RetryableException)
    
    def test_retryable_exception_default_max_retries(self):
        """Test RetryableException with default max_retries."""
        exception = RetryableException("Temporary error")
        
        assert exception.max_retries == 3
    
    def test_retryable_exception_custom_max_retries(self):
        """Test RetryableException with custom max_retries."""
        exception = RetryableException(
            "Network timeout",
            max_retries=5,
            pipeline="network_pipeline",
            operation="download"
        )
        
        assert exception.max_retries == 5
        assert exception.pipeline == "network_pipeline"
        assert exception.operation == "download"
    
    def test_retryable_exception_with_context(self):
        """Test RetryableException with full context."""
        exception = RetryableException(
            "Database connection timeout",
            max_retries=3,
            pipeline="db_pipeline",
            dataset="users",
            operation="query",
            details={"timeout_seconds": 30, "query": "SELECT * FROM users"}
        )
        
        assert exception.max_retries == 3
        assert exception.pipeline == "db_pipeline"
        assert exception.dataset == "users"
        assert exception.operation == "query"
        assert exception.details["timeout_seconds"] == 30
        assert exception.details["query"] == "SELECT * FROM users"


class TestExceptionChaining:
    """Test exception chaining and context preservation."""
    
    def test_exception_chaining_with_context(self):
        """Test that exception context is preserved when chaining."""
        try:
            # Simulate original exception
            raise ValueError("Original database error")
        except ValueError as original_error:
            # Wrap in ExtralException with context
            wrapped_exception = ConnectionException(
                "Failed to connect to database",
                pipeline="test_pipeline",
                operation="connect",
                details={"host": "localhost", "port": 3306}
            )
            
            # Chain the exceptions
            try:
                raise wrapped_exception from original_error
            except ConnectionException as chained_exception:
                # Verify context is preserved
                assert chained_exception.pipeline == "test_pipeline"
                assert chained_exception.operation == "connect"
                assert chained_exception.details["host"] == "localhost"
                assert chained_exception.details["port"] == 3306
                
                # Verify chaining works
                assert chained_exception.__cause__ is original_error
                assert isinstance(chained_exception.__cause__, ValueError)
                assert str(chained_exception.__cause__) == "Original database error"
    
    def test_multiple_exception_types_in_hierarchy(self):
        """Test creating different exception types with consistent context."""
        pipeline = "multi_step_pipeline"
        dataset = "users_table"
        
        # Extract phase exception
        extract_error = ExtractException(
            "Failed to extract data",
            pipeline=pipeline,
            dataset=dataset,
            operation="extract",
            details={"query": "SELECT * FROM users"}
        )
        
        # Load phase exception
        load_error = LoadException(
            "Failed to load data",
            pipeline=pipeline, 
            dataset=dataset,
            operation="load",
            details={"target_table": "users_staging"}
        )
        
        # Verify both have consistent context
        assert extract_error.pipeline == load_error.pipeline == pipeline
        assert extract_error.dataset == load_error.dataset == dataset
        assert extract_error.operation == "extract"
        assert load_error.operation == "load"