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
Pre-flight validation system for ETL pipeline configurations.

This module provides comprehensive validation for multi-pipeline configurations,
including connectivity testing, resource conflict detection, and configuration
consistency checks.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

from extral.config import Config, PipelineConfig, DatabaseConfig, FileConfig
from extral.exceptions import ValidationException, ConnectionException
from extral.connectors.database.mysql import MySQLConnector
from extral.connectors.database.postgresql import PostgreSQLConnector
from extral.connectors.file.csv_connector import CSVConnector
from extral.connectors.file.json_connector import JSONConnector

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    
    is_valid: bool
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineValidationResult:
    """Validation result for a single pipeline."""
    
    pipeline_name: str
    connectivity_result: ValidationResult
    resource_result: ValidationResult
    config_result: ValidationResult
    overall_valid: bool = False
    
    def __post_init__(self):
        """Calculate overall validity after initialization."""
        self.overall_valid = (
            self.connectivity_result.is_valid and 
            self.resource_result.is_valid and 
            self.config_result.is_valid
        )


@dataclass
class ValidationReport:
    """Comprehensive validation report for all pipelines."""
    
    overall_valid: bool = False
    pipeline_results: Dict[str, PipelineValidationResult] = field(default_factory=dict)
    global_conflicts: ValidationResult = field(default_factory=lambda: ValidationResult(is_valid=True))
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate overall validity and summary after initialization."""
        self._update_summary()
    
    def _update_summary(self):
        """Update the summary and overall validity based on current state."""
        if self.pipeline_results:
            valid_pipelines = sum(1 for result in self.pipeline_results.values() if result.overall_valid)
            total_pipelines = len(self.pipeline_results)
            
            self.overall_valid = (
                self.global_conflicts.is_valid and 
                valid_pipelines == total_pipelines and 
                total_pipelines > 0
            )
            
            self.summary = {
                "total_pipelines": total_pipelines,
                "valid_pipelines": valid_pipelines,
                "failed_pipelines": total_pipelines - valid_pipelines,
                "has_global_conflicts": not self.global_conflicts.is_valid,
            }
        else:
            self.overall_valid = False
            self.summary = {
                "total_pipelines": 0,
                "valid_pipelines": 0,
                "failed_pipelines": 0,
                "has_global_conflicts": not self.global_conflicts.is_valid,
            }


class PipelineValidator:
    """Comprehensive pipeline validation system."""
    
    def __init__(self):
        """Initialize the validator."""
        self.logger = logging.getLogger(__name__)
    
    def validate_configuration(self, config: Config) -> ValidationReport:
        """
        Perform comprehensive validation of the entire configuration.
        
        Args:
            config: Configuration object to validate
            
        Returns:
            ValidationReport with detailed results
        """
        report = ValidationReport()
        
        try:
            # Validate global resource conflicts first
            report.global_conflicts = self._validate_global_resource_conflicts(config.pipelines)
            
            # Validate each pipeline individually
            for pipeline in config.pipelines:
                pipeline_result = self._validate_pipeline(pipeline)
                report.pipeline_results[pipeline.name] = pipeline_result
            
            # Update the summary and overall validity after populating all results
            report._update_summary()
                
            logger.info(f"Configuration validation completed. Overall valid: {report.overall_valid}")
            
        except Exception as e:
            logger.error(f"Validation failed with unexpected error: {str(e)}")
            report.overall_valid = False
            report.global_conflicts = ValidationResult(
                is_valid=False,
                error_message=f"Validation system error: {str(e)}",
                details={"exception_type": type(e).__name__}
            )
        
        return report
    
    def _validate_pipeline(self, pipeline: PipelineConfig) -> PipelineValidationResult:
        """
        Validate a single pipeline configuration.
        
        Args:
            pipeline: Pipeline configuration to validate
            
        Returns:
            PipelineValidationResult with validation details
        """
        logger.debug(f"Validating pipeline: {pipeline.name}")
        
        # Test connectivity
        connectivity_result = self._test_pipeline_connectivity(pipeline)
        
        # Validate resources
        resource_result = self._validate_pipeline_resources(pipeline)
        
        # Validate configuration consistency
        config_result = self._validate_pipeline_configuration(pipeline)
        
        return PipelineValidationResult(
            pipeline_name=pipeline.name,
            connectivity_result=connectivity_result,
            resource_result=resource_result,
            config_result=config_result
        )
    
    def _test_pipeline_connectivity(self, pipeline: PipelineConfig) -> ValidationResult:
        """
        Test connectivity for all components in a pipeline.
        
        Args:
            pipeline: Pipeline to test
            
        Returns:
            ValidationResult with connectivity test results
        """
        try:
            warnings: List[str] = []
            details: Dict[str, Any] = {}
            
            # Test source connectivity
            source_connector = self._create_connector(pipeline.source)
            try:
                source_connector.test_connection()
                details["source_connectivity"] = "success"
                logger.debug(f"Source connectivity test passed for pipeline: {pipeline.name}")
            except ConnectionException as e:
                details["source_connectivity"] = "failed"
                details["source_error"] = str(e)
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Source connectivity failed: {str(e)}",
                    details=details
                )
            
            # Test destination connectivity
            dest_connector = self._create_connector(pipeline.destination)
            try:
                dest_connector.test_connection()
                details["destination_connectivity"] = "success"
                logger.debug(f"Destination connectivity test passed for pipeline: {pipeline.name}")
            except ConnectionException as e:
                details["destination_connectivity"] = "failed"
                details["destination_error"] = str(e)
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Destination connectivity failed: {str(e)}",
                    details=details
                )
            
            return ValidationResult(is_valid=True, warnings=warnings, details=details)
            
        except Exception as e:
            logger.error(f"Connectivity test failed for pipeline {pipeline.name}: {str(e)}")
            return ValidationResult(
                is_valid=False,
                error_message=f"Connectivity test error: {str(e)}",
                details={"exception_type": type(e).__name__}
            )
    
    def _validate_pipeline_resources(self, pipeline: PipelineConfig) -> ValidationResult:
        """
        Validate pipeline resource configuration (tables, files).
        
        Args:
            pipeline: Pipeline to validate
            
        Returns:
            ValidationResult with resource validation results
        """
        try:
            warnings: List[str] = []
            details: Dict[str, Any] = {}
            
            # Validate source resources
            if isinstance(pipeline.source, DatabaseConfig):
                if not pipeline.source.tables:
                    return ValidationResult(
                        is_valid=False,
                        error_message="Database source has no tables configured",
                        details={"source_type": "database"}
                    )
                
                # Check for duplicate table names
                table_names = [table.name for table in pipeline.source.tables]
                if len(table_names) != len(set(table_names)):
                    duplicates = [name for name in set(table_names) if table_names.count(name) > 1]
                    return ValidationResult(
                        is_valid=False,
                        error_message=f"Duplicate table names found: {duplicates}",
                        details={"source_type": "database", "duplicates": duplicates}
                    )
                
                details["source_tables"] = len(table_names)
                
            elif isinstance(pipeline.source, FileConfig):
                if not pipeline.source.files:
                    return ValidationResult(
                        is_valid=False,
                        error_message="File source has no files configured",
                        details={"source_type": "file"}
                    )
                
                # Check for duplicate file names/paths
                file_paths = []
                for file_item in pipeline.source.files:
                    if file_item.file_path:
                        file_paths.append(file_item.file_path)
                    elif file_item.http_path:
                        file_paths.append(file_item.http_path)
                
                if len(file_paths) != len(set(file_paths)):
                    duplicates = [path for path in set(file_paths) if file_paths.count(path) > 1]
                    return ValidationResult(
                        is_valid=False,
                        error_message=f"Duplicate file paths found: {duplicates}",
                        details={"source_type": "file", "duplicates": duplicates}
                    )
                
                details["source_files"] = len(file_paths)
            
            return ValidationResult(is_valid=True, warnings=warnings, details=details)
            
        except Exception as e:
            logger.error(f"Resource validation failed for pipeline {pipeline.name}: {str(e)}")
            return ValidationResult(
                is_valid=False,
                error_message=f"Resource validation error: {str(e)}",
                details={"exception_type": type(e).__name__}
            )
    
    def _validate_pipeline_configuration(self, pipeline: PipelineConfig) -> ValidationResult:
        """
        Validate pipeline configuration consistency.
        
        Args:
            pipeline: Pipeline to validate
            
        Returns:
            ValidationResult with configuration validation results
        """
        try:
            warnings: List[str] = []
            details: Dict[str, Any] = {}
            
            # Validate worker count
            if pipeline.workers is not None and pipeline.workers <= 0:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Invalid worker count: {pipeline.workers}. Must be positive.",
                    details={"workers": pipeline.workers}
                )
            
            # Validate source-specific configuration
            if isinstance(pipeline.source, DatabaseConfig):
                # Validate table configurations
                for table in pipeline.source.tables:
                    if table.strategy.value == "merge" and not table.merge_key:
                        warnings.append(f"Table {table.name} uses merge strategy but no merge_key specified")
                    
                    if table.batch_size is not None and table.batch_size <= 0:
                        return ValidationResult(
                            is_valid=False,
                            error_message=f"Invalid batch size for table {table.name}: {table.batch_size}",
                            details={"table": table.name, "batch_size": table.batch_size}
                        )
            
            elif isinstance(pipeline.source, FileConfig):
                # Validate file configurations
                for file_item in pipeline.source.files:
                    if file_item.strategy.value == "merge" and not file_item.merge_key:
                        file_name = file_item.file_path or file_item.http_path or "unknown"
                        warnings.append(f"File {file_name} uses merge strategy but no merge_key specified")
                    
                    if file_item.batch_size is not None and file_item.batch_size <= 0:
                        file_name = file_item.file_path or file_item.http_path or "unknown"
                        return ValidationResult(
                            is_valid=False,
                            error_message=f"Invalid batch size for file {file_name}: {file_item.batch_size}",
                            details={"file": file_name, "batch_size": file_item.batch_size}
                        )
            
            return ValidationResult(is_valid=True, warnings=warnings, details=details)
            
        except Exception as e:
            logger.error(f"Configuration validation failed for pipeline {pipeline.name}: {str(e)}")
            return ValidationResult(
                is_valid=False,
                error_message=f"Configuration validation error: {str(e)}",
                details={"exception_type": type(e).__name__}
            )
    
    def _validate_global_resource_conflicts(self, pipelines: List[PipelineConfig]) -> ValidationResult:
        """
        Check for resource conflicts between pipelines.
        
        Args:
            pipelines: List of all pipelines to check for conflicts
            
        Returns:
            ValidationResult indicating if there are global conflicts
        """
        try:
            warnings: List[str] = []
            details: Dict[str, Any] = {}
            
            # Track database resources (host:port:database:schema:table)
            db_resources: Dict[str, List[str]] = {}
            
            # Track file resources (file paths)
            file_resources: Dict[str, List[str]] = {}
            
            for pipeline in pipelines:
                # Check database destination conflicts
                if isinstance(pipeline.destination, DatabaseConfig):
                    dest_key = f"{pipeline.destination.host}:{pipeline.destination.port}:{pipeline.destination.database}"
                    schema = getattr(pipeline.destination, 'schema', 'public')
                    
                    # For database sources, check table-level conflicts
                    if isinstance(pipeline.source, DatabaseConfig):
                        for table in pipeline.source.tables:
                            table_key = f"{dest_key}:{schema}:{table.name}"
                            if table_key not in db_resources:
                                db_resources[table_key] = []
                            db_resources[table_key].append(pipeline.name)
                    
                    # For file sources, use the destination database as the resource key
                    else:
                        if dest_key not in db_resources:
                            db_resources[dest_key] = []
                        db_resources[dest_key].append(pipeline.name)
                
                # Check file destination conflicts
                elif isinstance(pipeline.destination, FileConfig):
                    # File destinations could potentially conflict
                    for file_item in pipeline.destination.files:
                        dest_path = None
                        if file_item.file_path:
                            dest_path = file_item.file_path
                        elif file_item.http_path:
                            dest_path = file_item.http_path
                        
                        if dest_path:
                            if dest_path not in file_resources:
                                file_resources[dest_path] = []
                            file_resources[dest_path].append(pipeline.name)
            
            # Check for conflicts
            conflicts = []
            
            # Database conflicts
            for resource, pipelines_using in db_resources.items():
                if len(pipelines_using) > 1:
                    conflicts.append(f"Database resource '{resource}' used by pipelines: {', '.join(pipelines_using)}")
            
            # File conflicts  
            for resource, pipelines_using in file_resources.items():
                if len(pipelines_using) > 1:
                    conflicts.append(f"File resource '{resource}' used by pipelines: {', '.join(pipelines_using)}")
            
            if conflicts:
                return ValidationResult(
                    is_valid=False,
                    error_message="Resource conflicts detected between pipelines",
                    details={"conflicts": conflicts}
                )
            
            # Add summary details
            details["database_resources_checked"] = len(db_resources)
            details["file_resources_checked"] = len(file_resources)
            
            return ValidationResult(is_valid=True, warnings=warnings, details=details)
            
        except Exception as e:
            logger.error(f"Global resource conflict validation failed: {str(e)}")
            return ValidationResult(
                is_valid=False,
                error_message=f"Global validation error: {str(e)}",
                details={"exception_type": type(e).__name__}
            )
    
    def _create_connector(self, config: Union[DatabaseConfig, FileConfig]):
        """
        Create appropriate connector instance for testing connectivity.
        
        Args:
            config: Database or file configuration
            
        Returns:
            Connector instance for testing
            
        Raises:
            ValidationException: If connector type is not supported
        """
        if isinstance(config, DatabaseConfig):
            if config.type.lower() == "mysql":
                mysql_connector = MySQLConnector()
                mysql_connector.connect(config)
                return mysql_connector
            elif config.type.lower() == "postgresql":
                pg_connector = PostgreSQLConnector()
                pg_connector.connect(config)
                return pg_connector
            else:
                raise ValidationException(
                    f"Unsupported database type for connectivity testing: {config.type}",
                    operation="create_connector",
                    details={"database_type": config.type}
                )
        
        elif isinstance(config, FileConfig):
            # For file connectors, we need to create a connector with a file item
            # We'll use the first file item for testing purposes
            if not config.files:
                raise ValidationException(
                    "No files configured for file connector testing",
                    operation="create_connector"
                )
            
            file_item = config.files[0]
            
            if config.type.lower() == "csv":
                return CSVConnector(file_item)
            elif config.type.lower() == "json":
                return JSONConnector(file_item)
            else:
                raise ValidationException(
                    f"Unsupported file type for connectivity testing: {config.type}",
                    operation="create_connector",
                    details={"file_type": config.type}
                )
        
        else:
            raise ValidationException(
                f"Unsupported config type for connectivity testing: {type(config)}",
                operation="create_connector",
                details={"config_type": str(type(config))}
            )


def format_validation_report(report: ValidationReport) -> str:
    """
    Format a validation report for human-readable output.
    
    Args:
        report: Validation report to format
        
    Returns:
        Formatted string representation of the report
    """
    lines = []
    lines.append("=" * 60)
    lines.append("ETL PIPELINE VALIDATION REPORT")
    lines.append("=" * 60)
    lines.append("")
    
    # Overall status
    status = "✅ PASSED" if report.overall_valid else "❌ FAILED"
    lines.append(f"Overall Status: {status}")
    lines.append("")
    
    # Summary
    if report.summary:
        lines.append("Summary:")
        for key, value in report.summary.items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Global conflicts
    if not report.global_conflicts.is_valid:
        lines.append("🚨 Global Resource Conflicts:")
        lines.append(f"  Error: {report.global_conflicts.error_message}")
        if "conflicts" in report.global_conflicts.details:
            for conflict in report.global_conflicts.details["conflicts"]:
                lines.append(f"    - {conflict}")
        lines.append("")
    
    # Pipeline details
    lines.append("Pipeline Validation Details:")
    lines.append("-" * 40)
    
    for pipeline_name, result in report.pipeline_results.items():
        status = "✅" if result.overall_valid else "❌"
        lines.append(f"{status} Pipeline: {pipeline_name}")
        
        # Connectivity
        conn_status = "✅" if result.connectivity_result.is_valid else "❌"
        lines.append(f"  {conn_status} Connectivity Test")
        if not result.connectivity_result.is_valid:
            lines.append(f"    Error: {result.connectivity_result.error_message}")
        
        # Resources
        res_status = "✅" if result.resource_result.is_valid else "❌"
        lines.append(f"  {res_status} Resource Validation")
        if not result.resource_result.is_valid:
            lines.append(f"    Error: {result.resource_result.error_message}")
        
        # Configuration
        conf_status = "✅" if result.config_result.is_valid else "❌"
        lines.append(f"  {conf_status} Configuration Validation")
        if not result.config_result.is_valid:
            lines.append(f"    Error: {result.config_result.error_message}")
        
        # Warnings
        all_warnings = []
        all_warnings.extend(result.connectivity_result.warnings)
        all_warnings.extend(result.resource_result.warnings)
        all_warnings.extend(result.config_result.warnings)
        
        if all_warnings:
            lines.append("  ⚠️  Warnings:")
            for warning in all_warnings:
                lines.append(f"    - {warning}")
        
        lines.append("")
    
    return "\n".join(lines)