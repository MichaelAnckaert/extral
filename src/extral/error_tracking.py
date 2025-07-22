"""Error tracking and reporting for the Extral ETL tool."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict


logger = logging.getLogger(__name__)


@dataclass
class ErrorDetails:
    """Details of a single error occurrence."""
    pipeline: str
    dataset: str
    operation: str
    error_type: str
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    duration_seconds: Optional[float] = None
    records_processed: Optional[int] = None
    retry_count: int = 0
    stack_trace: Optional[str] = None


@dataclass
class ErrorReport:
    """Summary report of all errors during ETL execution."""
    start_time: str
    end_time: Optional[str] = None
    total_pipelines: int = 0
    successful_pipelines: int = 0
    failed_pipelines: int = 0
    total_datasets: int = 0
    successful_datasets: int = 0
    failed_datasets: int = 0
    errors: List[ErrorDetails] = field(default_factory=list)
    
    def add_error(self, error: ErrorDetails):
        """Add an error to the report."""
        self.errors.append(error)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary for serialization."""
        return asdict(self)
    
    def save_to_file(self, filepath: Path):
        """Save error report to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
            
    def get_summary(self) -> str:
        """Get a human-readable summary of the error report."""
        lines = [
            "ETL Execution Summary",
            "=" * 50,
            f"Start Time: {self.start_time}",
            f"End Time: {self.end_time or 'In Progress'}",
            "",
            "Pipeline Summary:",
            f"  Total: {self.total_pipelines}",
            f"  Successful: {self.successful_pipelines}",
            f"  Failed: {self.failed_pipelines}",
            "",
            "Dataset Summary:",
            f"  Total: {self.total_datasets}",
            f"  Successful: {self.successful_datasets}",
            f"  Failed: {self.failed_datasets}",
            "",
            f"Total Errors: {len(self.errors)}",
        ]
        
        if self.errors:
            lines.extend([
                "",
                "Error Details:",
                "-" * 50,
            ])
            
            # Group errors by pipeline
            errors_by_pipeline: Dict[str, List[ErrorDetails]] = {}
            for error in self.errors:
                if error.pipeline not in errors_by_pipeline:
                    errors_by_pipeline[error.pipeline] = []
                errors_by_pipeline[error.pipeline].append(error)
            
            for pipeline, pipeline_errors in errors_by_pipeline.items():
                lines.append(f"\nPipeline: {pipeline}")
                for error in pipeline_errors:
                    lines.extend([
                        f"  Dataset: {error.dataset}",
                        f"    Operation: {error.operation}",
                        f"    Error Type: {error.error_type}",
                        f"    Message: {error.error_message}",
                        f"    Timestamp: {error.timestamp}",
                    ])
                    if error.retry_count > 0:
                        lines.append(f"    Retries: {error.retry_count}")
                    if error.duration_seconds is not None:
                        lines.append(f"    Duration: {error.duration_seconds:.2f}s")
                    if error.records_processed is not None:
                        lines.append(f"    Records Processed: {error.records_processed}")
                    lines.append("")
        
        return "\n".join(lines)


class ErrorTracker:
    """Tracks errors during ETL execution."""
    
    def __init__(self):
        self.report = ErrorReport(start_time=datetime.now().isoformat())
        self.pipeline_errors: Dict[str, List[ErrorDetails]] = {}
        self.dataset_errors: Dict[str, List[ErrorDetails]] = {}
        
    def track_error(
        self,
        pipeline: str,
        dataset: str,
        operation: str,
        exception: Exception,
        duration_seconds: Optional[float] = None,
        records_processed: Optional[int] = None,
        retry_count: int = 0,
        include_stack_trace: bool = False
    ):
        """Track an error occurrence."""
        import traceback
        
        error = ErrorDetails(
            pipeline=pipeline,
            dataset=dataset,
            operation=operation,
            error_type=type(exception).__name__,
            error_message=str(exception),
            duration_seconds=duration_seconds,
            records_processed=records_processed,
            retry_count=retry_count,
            stack_trace=traceback.format_exc() if include_stack_trace else None
        )
        
        self.report.add_error(error)
        
        # Track by pipeline
        if pipeline not in self.pipeline_errors:
            self.pipeline_errors[pipeline] = []
        self.pipeline_errors[pipeline].append(error)
        
        # Track by dataset
        dataset_key = f"{pipeline}::{dataset}"
        if dataset_key not in self.dataset_errors:
            self.dataset_errors[dataset_key] = []
        self.dataset_errors[dataset_key].append(error)
        
        logger.error(
            f"Error tracked - Pipeline: {pipeline}, Dataset: {dataset}, "
            f"Operation: {operation}, Type: {error.error_type}, Message: {error.error_message}"
        )
        
    def finalize_report(
        self,
        total_pipelines: int,
        successful_pipelines: int,
        total_datasets: int,
        successful_datasets: int
    ):
        """Finalize the error report with summary statistics."""
        self.report.end_time = datetime.now().isoformat()
        self.report.total_pipelines = total_pipelines
        self.report.successful_pipelines = successful_pipelines
        self.report.failed_pipelines = total_pipelines - successful_pipelines
        self.report.total_datasets = total_datasets
        self.report.successful_datasets = successful_datasets
        self.report.failed_datasets = total_datasets - successful_datasets
        
    def has_errors_for_pipeline(self, pipeline: str) -> bool:
        """Check if a pipeline has any errors."""
        return pipeline in self.pipeline_errors
    
    def has_errors_for_dataset(self, pipeline: str, dataset: str) -> bool:
        """Check if a dataset has any errors."""
        dataset_key = f"{pipeline}::{dataset}"
        return dataset_key in self.dataset_errors
    
    def get_errors_for_pipeline(self, pipeline: str) -> List[ErrorDetails]:
        """Get all errors for a specific pipeline."""
        return self.pipeline_errors.get(pipeline, [])
    
    def get_errors_for_dataset(self, pipeline: str, dataset: str) -> List[ErrorDetails]:
        """Get all errors for a specific dataset."""
        dataset_key = f"{pipeline}::{dataset}"
        return self.dataset_errors.get(dataset_key, [])