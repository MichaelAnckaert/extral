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
Unit tests for error tracking module.
"""

import pytest
import json
import tempfile
from datetime import datetime
from pathlib import Path

from extral.error_tracking import ErrorDetails, ErrorReport, ErrorTracker
from extral.exceptions import ExtractException, LoadException, ConnectionException


class TestErrorDetails:
    """Test ErrorDetails dataclass."""
    
    def test_error_details_creation(self):
        """Test creating an ErrorDetails instance."""
        error_details = ErrorDetails(
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract",
            error_type="ExtractException",
            error_message="Data extraction failed",
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            duration_seconds=30.5,
            records_processed=1000
        )
        
        assert error_details.pipeline == "test_pipeline"
        assert error_details.dataset == "test_table" 
        assert error_details.operation == "extract"
        assert error_details.error_type == "ExtractException"
        assert error_details.error_message == "Data extraction failed"
        assert error_details.timestamp == datetime(2023, 1, 1, 12, 0, 0)
        assert error_details.duration_seconds == 30.5
        assert error_details.records_processed == 1000
    
    def test_error_details_defaults(self):
        """Test ErrorDetails with default values."""
        error_details = ErrorDetails(
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract",
            error_type="ExtractException",
            error_message="Error occurred"
        )
        
        assert error_details.duration_seconds is None
        assert error_details.records_processed is None
        assert error_details.retry_count == 0
        assert error_details.stack_trace is None


class TestErrorReport:
    """Test ErrorReport dataclass."""
    
    def test_error_report_creation(self):
        """Test creating an ErrorReport instance."""
        error1 = ErrorDetails(
            pipeline="pipeline1", 
            dataset="table1",
            operation="extract",
            error_type="ExtractException",
            error_message="Error 1"
        )
        
        error2 = ErrorDetails(
            pipeline="pipeline2",
            dataset="table2", 
            operation="load",
            error_type="LoadException",
            error_message="Error 2"
        )
        
        report = ErrorReport(
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            end_time=datetime(2023, 1, 1, 11, 0, 0),
            errors=[error1, error2],
            total_pipelines=2,
            successful_pipelines=0,
            failed_pipelines=2,
            total_datasets=4,
            successful_datasets=2,
            failed_datasets=2
        )
        
        assert len(report.errors) == 2
        assert report.total_pipelines == 2
        assert report.successful_pipelines == 0
        assert report.failed_pipelines == 2
        assert report.total_datasets == 4
        assert report.successful_datasets == 2
        assert report.failed_datasets == 2
    
    def test_error_report_summary(self):
        """Test ErrorReport summary generation."""
        error = ErrorDetails(
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract",
            error_type="ExtractException", 
            error_message="Test error"
        )
        
        report = ErrorReport(
            start_time=datetime(2023, 1, 1, 10, 0, 0),
            end_time=datetime(2023, 1, 1, 11, 0, 0),
            errors=[error]
        )
        
        summary = report.get_summary()
        
        assert "Start Time" in summary
        assert "End Time" in summary
        assert "Total Errors: 1" in summary
        assert "Pipeline Summary" in summary
        assert "Dataset Summary" in summary
    
    def test_error_report_errors_by_pipeline(self):
        """Test ErrorReport grouping errors by pipeline."""
        error1 = ErrorDetails(
            pipeline="pipeline1",
            dataset="table1", 
            operation="extract",
            error_type="ExtractException",
            error_message="Error 1"
        )
        
        error2 = ErrorDetails(
            pipeline="pipeline1",
            dataset="table2",
            operation="load", 
            error_type="LoadException",
            error_message="Error 2"
        )
        
        error3 = ErrorDetails(
            pipeline="pipeline2",
            dataset="table3",
            operation="extract",
            error_type="ExtractException",
            error_message="Error 3"
        )
        
        report = ErrorReport(errors=[error1, error2, error3])
        errors_by_pipeline = report.get_errors_by_pipeline()
        
        assert "pipeline1" in errors_by_pipeline
        assert "pipeline2" in errors_by_pipeline
        assert len(errors_by_pipeline["pipeline1"]) == 2
        assert len(errors_by_pipeline["pipeline2"]) == 1


class TestErrorTracker:
    """Test ErrorTracker class."""
    
    def test_error_tracker_initialization(self):
        """Test ErrorTracker initialization."""
        tracker = ErrorTracker()
        
        assert len(tracker.errors) == 0
        assert tracker.start_time is not None
    
    def test_error_tracker_track_error(self):
        """Test tracking an error."""
        tracker = ErrorTracker()
        
        exception = ExtractException(
            "Data extraction failed",
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract"
        )
        
        tracker.track_error(exception, duration_seconds=30.0, records_processed=500)
        
        assert len(tracker.errors) == 1
        error = tracker.errors[0]
        assert error.pipeline == "test_pipeline"
        assert error.dataset == "test_table"
        assert error.operation == "extract"
        assert error.error_type == "ExtractException"
        assert error.error_message == "[pipeline=test_pipeline, dataset=test_table, operation=extract] Data extraction failed"
        assert error.duration_seconds == 30.0
        assert error.records_processed == 500
    
    def test_error_tracker_track_error_with_stack_trace(self):
        """Test tracking an error with stack trace."""
        tracker = ErrorTracker()
        
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            exception = LoadException(
                "Load failed due to value error",
                pipeline="test_pipeline",
                dataset="test_table", 
                operation="load"
            )
            tracker.track_error(exception, include_stack_trace=True)
        
        assert len(tracker.errors) == 1
        error = tracker.errors[0]
        assert error.stack_trace is not None
        assert "ValueError" in error.stack_trace
    
    def test_error_tracker_track_multiple_errors(self):
        """Test tracking multiple errors."""
        tracker = ErrorTracker()
        
        exception1 = ExtractException(
            "First error",
            pipeline="pipeline1",
            dataset="table1", 
            operation="extract"
        )
        
        exception2 = LoadException(
            "Second error",
            pipeline="pipeline2",
            dataset="table2",
            operation="load"
        )
        
        tracker.track_error(exception1)
        tracker.track_error(exception2)
        
        assert len(tracker.errors) == 2
        assert tracker.errors[0].pipeline == "pipeline1"
        assert tracker.errors[1].pipeline == "pipeline2"
    
    def test_error_tracker_get_report(self):
        """Test generating an error report."""
        tracker = ErrorTracker()
        
        exception = ExtractException(
            "Test error",
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract"
        )
        
        tracker.track_error(exception)
        
        report = tracker.get_report(
            total_pipelines=2,
            successful_pipelines=1,
            failed_pipelines=1,
            total_datasets=4, 
            successful_datasets=3,
            failed_datasets=1
        )
        
        assert isinstance(report, ErrorReport)
        assert len(report.errors) == 1
        assert report.total_pipelines == 2
        assert report.successful_pipelines == 1
        assert report.failed_pipelines == 1
        assert report.total_datasets == 4
        assert report.successful_datasets == 3
        assert report.failed_datasets == 1
        assert report.start_time == tracker.start_time
        assert report.end_time is not None
    
    def test_error_tracker_save_report_to_file(self):
        """Test saving error report to file."""
        tracker = ErrorTracker()
        
        exception = ExtractException(
            "Test error",
            pipeline="test_pipeline",
            dataset="test_table",
            operation="extract"
        )
        
        tracker.track_error(exception)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file_path = f.name
        
        try:
            report = tracker.get_report(
                total_pipelines=1,
                successful_pipelines=0,
                failed_pipelines=1,
                total_datasets=1,
                successful_datasets=0,
                failed_datasets=1
            )
            
            tracker.save_report_to_file(report, temp_file_path)
            
            # Verify file was created and contains expected data
            assert Path(temp_file_path).exists()
            
            with open(temp_file_path, 'r') as f:
                saved_data = json.load(f)
            
            assert "start_time" in saved_data
            assert "end_time" in saved_data
            assert "errors" in saved_data
            assert len(saved_data["errors"]) == 1
            assert saved_data["total_pipelines"] == 1
            
        finally:
            # Cleanup
            Path(temp_file_path).unlink(missing_ok=True)
    
    def test_error_tracker_has_errors(self):
        """Test checking if tracker has errors."""
        tracker = ErrorTracker()
        
        assert not tracker.has_errors()
        
        exception = ExtractException(
            "Test error",
            pipeline="test_pipeline",
            dataset="test_table", 
            operation="extract"
        )
        
        tracker.track_error(exception)
        
        assert tracker.has_errors()
    
    def test_error_tracker_get_pipeline_errors(self):
        """Test getting errors for a specific pipeline."""
        tracker = ErrorTracker()
        
        exception1 = ExtractException(
            "Error 1",
            pipeline="pipeline1",
            dataset="table1",
            operation="extract"
        )
        
        exception2 = LoadException(
            "Error 2", 
            pipeline="pipeline2",
            dataset="table2",
            operation="load"
        )
        
        exception3 = ExtractException(
            "Error 3",
            pipeline="pipeline1",
            dataset="table3",
            operation="extract" 
        )
        
        tracker.track_error(exception1)
        tracker.track_error(exception2) 
        tracker.track_error(exception3)
        
        pipeline1_errors = tracker.get_pipeline_errors("pipeline1")
        pipeline2_errors = tracker.get_pipeline_errors("pipeline2")
        
        assert len(pipeline1_errors) == 2
        assert len(pipeline2_errors) == 1
        assert all(error.pipeline == "pipeline1" for error in pipeline1_errors)
        assert pipeline2_errors[0].pipeline == "pipeline2"
    
    def test_error_tracker_get_error_summary_by_type(self):
        """Test getting error summary grouped by error type."""
        tracker = ErrorTracker()
        
        # Add different types of errors
        extract_error1 = ExtractException(
            "Extract error 1",
            pipeline="pipeline1", 
            dataset="table1",
            operation="extract"
        )
        
        extract_error2 = ExtractException(
            "Extract error 2",
            pipeline="pipeline1",
            dataset="table2",
            operation="extract"
        )
        
        load_error = LoadException(
            "Load error",
            pipeline="pipeline2",
            dataset="table3",
            operation="load"
        )
        
        connection_error = ConnectionException(
            "Connection error",
            pipeline="pipeline3",
            dataset="table4",
            operation="connection"
        )
        
        tracker.track_error(extract_error1)
        tracker.track_error(extract_error2)
        tracker.track_error(load_error)
        tracker.track_error(connection_error)
        
        summary = tracker.get_error_summary_by_type()
        
        assert summary["ExtractException"] == 2
        assert summary["LoadException"] == 1
        assert summary["ConnectionException"] == 1