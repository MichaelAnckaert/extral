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
Unit tests for validation module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from extral.validation import (
    ValidationResult,
    PipelineValidationResult, 
    ValidationReport,
    PipelineValidator,
    format_validation_report
)
from extral.config import (
    Config,
    PipelineConfig,
    DatabaseConfig,
    FileConfig,
    TableConfig,
    FileItemConfig,
    LoadStrategy
)
from extral.exceptions import ValidationException, ConnectionException


@pytest.mark.unit
class TestValidationResult:
    """Test ValidationResult dataclass."""
    
    def test_validation_result_valid(self):
        """Test creating a valid ValidationResult."""
        result = ValidationResult(is_valid=True, warnings=["Warning message"])
        
        assert result.is_valid is True
        assert result.error_message is None
        assert result.warnings == ["Warning message"]
        assert result.details == {}
    
    def test_validation_result_invalid(self):
        """Test creating an invalid ValidationResult.""" 
        result = ValidationResult(
            is_valid=False,
            error_message="Error occurred",
            details={"key": "value"}
        )
        
        assert result.is_valid is False
        assert result.error_message == "Error occurred"
        assert result.warnings == []
        assert result.details == {"key": "value"}


@pytest.mark.unit
class TestPipelineValidationResult:
    """Test PipelineValidationResult dataclass."""
    
    def test_pipeline_validation_result_all_valid(self):
        """Test PipelineValidationResult when all validations pass."""
        connectivity_result = ValidationResult(is_valid=True)
        resource_result = ValidationResult(is_valid=True)
        config_result = ValidationResult(is_valid=True)
        
        result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=connectivity_result,
            resource_result=resource_result,
            config_result=config_result
        )
        
        assert result.pipeline_name == "test_pipeline"
        assert result.overall_valid is True
    
    def test_pipeline_validation_result_connectivity_failed(self):
        """Test PipelineValidationResult when connectivity fails."""
        connectivity_result = ValidationResult(is_valid=False, error_message="Connection failed")
        resource_result = ValidationResult(is_valid=True)
        config_result = ValidationResult(is_valid=True)
        
        result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=connectivity_result,
            resource_result=resource_result,
            config_result=config_result
        )
        
        assert result.overall_valid is False
    
    def test_pipeline_validation_result_resource_failed(self):
        """Test PipelineValidationResult when resource validation fails."""
        connectivity_result = ValidationResult(is_valid=True)
        resource_result = ValidationResult(is_valid=False, error_message="Resource conflict") 
        config_result = ValidationResult(is_valid=True)
        
        result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=connectivity_result,
            resource_result=resource_result,
            config_result=config_result
        )
        
        assert result.overall_valid is False


@pytest.mark.unit
class TestValidationReport:
    """Test ValidationReport dataclass."""
    
    def test_validation_report_all_valid(self):
        """Test ValidationReport when all pipelines are valid."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=True),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(is_valid=True)
        )
        
        assert report.overall_valid is True
        assert report.summary["total_pipelines"] == 1
        assert report.summary["valid_pipelines"] == 1
        assert report.summary["failed_pipelines"] == 0
    
    def test_validation_report_global_conflicts(self):
        """Test ValidationReport when global conflicts exist."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=True),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(is_valid=False, error_message="Global conflict")
        )
        
        assert report.overall_valid is False
        assert report.summary["has_global_conflicts"] is True
    
    def test_validation_report_pipeline_failed(self):
        """Test ValidationReport when a pipeline fails validation."""
        failed_pipeline_result = PipelineValidationResult(
            pipeline_name="failed_pipeline",
            connectivity_result=ValidationResult(is_valid=False, error_message="Connection failed"),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        valid_pipeline_result = PipelineValidationResult(
            pipeline_name="valid_pipeline", 
            connectivity_result=ValidationResult(is_valid=True),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={
                "failed_pipeline": failed_pipeline_result,
                "valid_pipeline": valid_pipeline_result
            },
            global_conflicts=ValidationResult(is_valid=True)
        )
        
        assert report.overall_valid is False
        assert report.summary["total_pipelines"] == 2
        assert report.summary["valid_pipelines"] == 1
        assert report.summary["failed_pipelines"] == 1


@pytest.mark.unit
class TestPipelineValidator:
    """Test PipelineValidator class."""
    
    def test_pipeline_validator_initialization(self):
        """Test PipelineValidator initialization."""
        validator = PipelineValidator()
        assert validator is not None
    
    @patch('extral.validation.MySQLConnector')
    @patch('extral.validation.PostgreSQLConnector')
    def test_validate_configuration_success(self, mock_pg_connector, mock_mysql_connector):
        """Test successful configuration validation."""
        # Setup mocks
        mock_mysql_instance = Mock()
        mock_mysql_instance.test_connection.return_value = True
        mock_mysql_connector.return_value = mock_mysql_instance
        
        mock_pg_instance = Mock()
        mock_pg_instance.test_connection.return_value = True
        mock_pg_connector.return_value = mock_pg_instance
        
        # Create test configuration
        source = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password", 
            database="test_db",
            tables=[TableConfig(name="test_table")]
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        config = Config(pipelines=[pipeline])
        
        # Test validation
        validator = PipelineValidator()
        report = validator.validate_configuration(config)
        
        assert report.overall_valid is True
        assert len(report.pipeline_results) == 1
        assert "test_pipeline" in report.pipeline_results
    
    @patch('extral.validation.MySQLConnector')
    def test_validate_configuration_connection_failure(self, mock_mysql_connector):
        """Test configuration validation with connection failure."""
        # Setup mock to raise ConnectionException
        mock_mysql_instance = Mock()
        mock_mysql_instance.test_connection.side_effect = ConnectionException(
            "Connection failed",
            operation="test_connection"
        )
        mock_mysql_connector.return_value = mock_mysql_instance
        
        # Create test configuration
        source = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db", 
            tables=[TableConfig(name="test_table")]
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        config = Config(pipelines=[pipeline])
        
        # Test validation
        validator = PipelineValidator()
        report = validator.validate_configuration(config)
        
        assert report.overall_valid is False
        assert not report.pipeline_results["test_pipeline"].connectivity_result.is_valid
    
    def test_validate_pipeline_resource_conflicts_database(self):
        """Test validation detects database resource conflicts."""
        source1 = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="user1",
            password="password",
            database="source_db",
            tables=[TableConfig(name="shared_table")]
        )
        
        source2 = DatabaseConfig(
            type="mysql", 
            host="localhost",
            port=3306,
            user="user2",
            password="password",
            database="source_db",
            tables=[TableConfig(name="shared_table")]
        )
        
        # Same destination for both pipelines - should cause conflict
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres", 
            password="password",
            database="target_db",
            schema="public"
        )
        
        pipeline1 = PipelineConfig(
            name="pipeline1",
            source=source1,
            destination=destination
        )
        
        pipeline2 = PipelineConfig(
            name="pipeline2",
            source=source2,
            destination=destination
        )
        
        config = Config(pipelines=[pipeline1, pipeline2])
        
        validator = PipelineValidator()
        report = validator.validate_configuration(config)
        
        # Should detect resource conflict
        assert not report.global_conflicts.is_valid
        assert "Resource conflicts detected" in report.global_conflicts.error_message
    
    def test_validate_pipeline_resources_no_tables(self):
        """Test validation fails when database source has no tables."""
        source = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            tables=[]  # No tables
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        validator = PipelineValidator()
        result = validator._validate_pipeline_resources(pipeline)
        
        assert not result.is_valid
        assert "Database source has no tables configured" in result.error_message
    
    def test_validate_pipeline_resources_duplicate_table_names(self):
        """Test validation fails when there are duplicate table names."""
        source = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user", 
            password="test_password",
            database="test_db",
            tables=[
                TableConfig(name="duplicate_table"),
                TableConfig(name="duplicate_table")  # Duplicate name
            ]
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        validator = PipelineValidator()
        result = validator._validate_pipeline_resources(pipeline)
        
        assert not result.is_valid
        assert "Duplicate table names found" in result.error_message
    
    def test_validate_pipeline_configuration_merge_strategy_without_key(self):
        """Test validation generates warning for merge strategy without merge key."""
        source = DatabaseConfig(
            type="mysql",
            host="localhost", 
            port=3306,
            user="test_user",
            password="test_password",
            database="test_db",
            tables=[
                TableConfig(
                    name="test_table",
                    strategy=LoadStrategy.MERGE
                    # No merge_key specified
                )
            ]
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        validator = PipelineValidator()
        result = validator._validate_pipeline_configuration(pipeline)
        
        assert result.is_valid  # Should still be valid, just a warning
        assert len(result.warnings) > 0
        assert "merge strategy but no merge_key specified" in result.warnings[0]
    
    def test_validate_pipeline_configuration_invalid_batch_size(self):
        """Test validation fails for invalid batch size."""
        source = DatabaseConfig(
            type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_password", 
            database="test_db",
            tables=[
                TableConfig(
                    name="test_table",
                    batch_size=-100  # Invalid batch size
                )
            ]
        )
        
        destination = DatabaseConfig(
            type="postgresql",
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="target_db"
        )
        
        pipeline = PipelineConfig(
            name="test_pipeline",
            source=source,
            destination=destination
        )
        
        validator = PipelineValidator()
        result = validator._validate_pipeline_configuration(pipeline)
        
        assert not result.is_valid
        assert "Invalid batch size" in result.error_message


@pytest.mark.unit
class TestFormatValidationReport:
    """Test format_validation_report function."""
    
    def test_format_validation_report_success(self):
        """Test formatting a successful validation report."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=True),
            resource_result=ValidationResult(is_valid=True), 
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(is_valid=True)
        )
        
        formatted = format_validation_report(report)
        
        assert "PASSED" in formatted
        assert "test_pipeline" in formatted
        assert "✅" in formatted
    
    def test_format_validation_report_failure(self):
        """Test formatting a failed validation report."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=False, error_message="Connection failed"),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(is_valid=True)
        )
        
        formatted = format_validation_report(report)
        
        assert "FAILED" in formatted
        assert "test_pipeline" in formatted
        assert "Connection failed" in formatted
        assert "❌" in formatted
    
    def test_format_validation_report_with_global_conflicts(self):
        """Test formatting a report with global conflicts."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=True),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True)
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(
                is_valid=False,
                error_message="Resource conflicts detected",
                details={"conflicts": ["Resource conflict 1", "Resource conflict 2"]}
            )
        )
        
        formatted = format_validation_report(report)
        
        assert "FAILED" in formatted
        assert "Global Resource Conflicts" in formatted 
        assert "Resource conflicts detected" in formatted
        assert "Resource conflict 1" in formatted
        assert "Resource conflict 2" in formatted
    
    def test_format_validation_report_with_warnings(self):
        """Test formatting a report with warnings."""
        pipeline_result = PipelineValidationResult(
            pipeline_name="test_pipeline",
            connectivity_result=ValidationResult(is_valid=True, warnings=["Connection warning"]),
            resource_result=ValidationResult(is_valid=True),
            config_result=ValidationResult(is_valid=True, warnings=["Config warning"])
        )
        
        report = ValidationReport(
            pipeline_results={"test_pipeline": pipeline_result},
            global_conflicts=ValidationResult(is_valid=True)
        )
        
        formatted = format_validation_report(report)
        
        assert "PASSED" in formatted
        assert "Warnings" in formatted
        assert "Connection warning" in formatted
        assert "Config warning" in formatted
        assert "⚠️" in formatted