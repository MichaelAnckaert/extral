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
"""Statistics handler for tracking ETL metrics."""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from extral.events import Event, EventHandler, EventType

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """Statistics for a single pipeline."""
    name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_datasets: int = 0
    successful_datasets: int = 0
    failed_datasets: int = 0
    skipped_datasets: int = 0
    status: str = "pending"
    error_message: Optional[str] = None


@dataclass
class DatasetStats:
    """Statistics for a single dataset."""
    pipeline: str
    name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    extract_duration: Optional[float] = None
    load_duration: Optional[float] = None
    total_duration: Optional[float] = None
    records_extracted: Optional[int] = None
    records_loaded: Optional[int] = None
    status: str = "pending"
    error_message: Optional[str] = None
    error_operation: Optional[str] = None


@dataclass
class ErrorDetail:
    """Detailed error information."""
    pipeline: str
    dataset: str
    operation: str
    error_type: str
    error_message: str
    timestamp: datetime
    duration_seconds: Optional[float] = None
    stack_trace: Optional[str] = None


@dataclass
class ExecutionStats:
    """Overall execution statistics."""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_pipelines: int = 0
    successful_pipelines: int = 0
    failed_pipelines: int = 0
    total_datasets: int = 0
    successful_datasets: int = 0
    failed_datasets: int = 0
    skipped_datasets: int = 0
    errors: List[ErrorDetail] = field(default_factory=list)
    pipelines: Dict[str, PipelineStats] = field(default_factory=dict)
    datasets: Dict[str, DatasetStats] = field(default_factory=dict)  # key: "pipeline::dataset"
    active_workers: int = 0


class StatsHandler(EventHandler):
    """Handler that tracks execution statistics."""
    
    def __init__(self):
        self.stats = ExecutionStats()
        self._dataset_start_times: Dict[str, datetime] = {}
        self._extract_start_times: Dict[str, datetime] = {}
        self._load_start_times: Dict[str, datetime] = {}
        
    def handle(self, event: Event) -> None:
        """Update statistics based on event."""
        if event.type == EventType.PIPELINE_STARTED:
            self._handle_pipeline_started(event)
        elif event.type == EventType.PIPELINE_COMPLETED:
            self._handle_pipeline_completed(event)
        elif event.type == EventType.PIPELINE_FAILED:
            self._handle_pipeline_failed(event)
        elif event.type == EventType.DATASET_STARTED:
            self._handle_dataset_started(event)
        elif event.type == EventType.DATASET_COMPLETED:
            self._handle_dataset_completed(event)
        elif event.type == EventType.DATASET_FAILED:
            self._handle_dataset_failed(event)
        elif event.type == EventType.DATASET_SKIPPED:
            self._handle_dataset_skipped(event)
        elif event.type == EventType.EXTRACT_STARTED:
            self._handle_extract_started(event)
        elif event.type == EventType.EXTRACT_COMPLETED:
            self._handle_extract_completed(event)
        elif event.type == EventType.LOAD_STARTED:
            self._handle_load_started(event)
        elif event.type == EventType.LOAD_COMPLETED:
            self._handle_load_completed(event)
        elif event.type == EventType.ERROR_OCCURRED:
            self._handle_error_occurred(event)
        elif event.type == EventType.WORKER_UPDATE:
            self._handle_worker_update(event)
            
    def _handle_pipeline_started(self, event: Event) -> None:
        """Handle pipeline started event."""
        if not event.pipeline:
            return
            
        pipeline_stats = PipelineStats(
            name=event.pipeline,
            start_time=event.timestamp,
            total_datasets=event.data.get("dataset_count", 0),
            status="running"
        )
        self.stats.pipelines[event.pipeline] = pipeline_stats
        self.stats.total_pipelines += 1
        self.stats.total_datasets += pipeline_stats.total_datasets
        
    def _handle_pipeline_completed(self, event: Event) -> None:
        """Handle pipeline completed event."""
        if not event.pipeline:
            return
            
        if event.pipeline in self.stats.pipelines:
            pipeline = self.stats.pipelines[event.pipeline]
            pipeline.end_time = event.timestamp
            pipeline.status = "completed"
            pipeline.successful_datasets = event.data.get("successful_datasets", 0)
            pipeline.failed_datasets = event.data.get("failed_datasets", 0)
            self.stats.successful_pipelines += 1
            
    def _handle_pipeline_failed(self, event: Event) -> None:
        """Handle pipeline failed event."""
        if not event.pipeline:
            return
            
        if event.pipeline in self.stats.pipelines:
            pipeline = self.stats.pipelines[event.pipeline]
            pipeline.end_time = event.timestamp
            pipeline.status = "failed"
            pipeline.error_message = event.data.get("error_message")
            self.stats.failed_pipelines += 1
            
    def _handle_dataset_started(self, event: Event) -> None:
        """Handle dataset started event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        self._dataset_start_times[key] = event.timestamp
        
        dataset_stats = DatasetStats(
            pipeline=event.pipeline,
            name=event.dataset,
            start_time=event.timestamp,
            status="running"
        )
        self.stats.datasets[key] = dataset_stats
        
    def _handle_dataset_completed(self, event: Event) -> None:
        """Handle dataset completed event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        if key in self.stats.datasets:
            dataset = self.stats.datasets[key]
            dataset.end_time = event.timestamp
            dataset.status = "completed"
            
            if key in self._dataset_start_times:
                dataset.total_duration = (event.timestamp - self._dataset_start_times[key]).total_seconds()
                del self._dataset_start_times[key]
                
            self.stats.successful_datasets += 1
            
    def _handle_dataset_failed(self, event: Event) -> None:
        """Handle dataset failed event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        if key in self.stats.datasets:
            dataset = self.stats.datasets[key]
            dataset.end_time = event.timestamp
            dataset.status = "failed"
            dataset.error_message = event.data.get("error_message")
            
            if key in self._dataset_start_times:
                dataset.total_duration = (event.timestamp - self._dataset_start_times[key]).total_seconds()
                del self._dataset_start_times[key]
                
            self.stats.failed_datasets += 1
            
    def _handle_dataset_skipped(self, event: Event) -> None:
        """Handle dataset skipped event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        dataset_stats = DatasetStats(
            pipeline=event.pipeline,
            name=event.dataset,
            status="skipped"
        )
        self.stats.datasets[key] = dataset_stats
        self.stats.skipped_datasets += 1
        
    def _handle_extract_started(self, event: Event) -> None:
        """Handle extract started event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        self._extract_start_times[key] = event.timestamp
        
    def _handle_extract_completed(self, event: Event) -> None:
        """Handle extract completed event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        if key in self.stats.datasets:
            dataset = self.stats.datasets[key]
            dataset.records_extracted = event.data.get("records")
            
            if key in self._extract_start_times:
                dataset.extract_duration = (event.timestamp - self._extract_start_times[key]).total_seconds()
                del self._extract_start_times[key]
                
    def _handle_load_started(self, event: Event) -> None:
        """Handle load started event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        self._load_start_times[key] = event.timestamp
        
    def _handle_load_completed(self, event: Event) -> None:
        """Handle load completed event."""
        if not event.pipeline or not event.dataset:
            return
            
        key = f"{event.pipeline}::{event.dataset}"
        if key in self.stats.datasets:
            dataset = self.stats.datasets[key]
            dataset.records_loaded = event.data.get("records")
            
            if key in self._load_start_times:
                dataset.load_duration = (event.timestamp - self._load_start_times[key]).total_seconds()
                del self._load_start_times[key]
                
    def _handle_error_occurred(self, event: Event) -> None:
        """Handle error occurred event."""
        if not event.pipeline or not event.dataset:
            return
            
        error = ErrorDetail(
            pipeline=event.pipeline,
            dataset=event.dataset,
            operation=event.data.get("operation", "unknown"),
            error_type=event.data.get("error_type", "Unknown"),
            error_message=event.data.get("error_message", ""),
            timestamp=event.timestamp,
            duration_seconds=event.data.get("duration_seconds"),
            stack_trace=event.data.get("stack_trace")
        )
        self.stats.errors.append(error)
        
        # Update dataset error info
        key = f"{event.pipeline}::{event.dataset}"
        if key in self.stats.datasets:
            dataset = self.stats.datasets[key]
            dataset.error_message = error.error_message
            dataset.error_operation = error.operation
            
    def _handle_worker_update(self, event: Event) -> None:
        """Handle worker update event."""
        self.stats.active_workers = event.data.get("active_count", 0)
        
    def get_summary(self) -> str:
        """Get a human-readable summary of statistics."""
        lines = [
            "ETL Execution Summary",
            "=" * 50,
            f"Start Time: {self.stats.start_time.isoformat()}",
            f"End Time: {self.stats.end_time.isoformat() if self.stats.end_time else 'In Progress'}",
            "",
            "Pipeline Summary:",
            f"  Total: {self.stats.total_pipelines}",
            f"  Successful: {self.stats.successful_pipelines}",
            f"  Failed: {self.stats.failed_pipelines}",
            "",
            "Dataset Summary:",
            f"  Total: {self.stats.total_datasets}",
            f"  Successful: {self.stats.successful_datasets}",
            f"  Failed: {self.stats.failed_datasets}",
            f"  Skipped: {self.stats.skipped_datasets}",
            "",
            f"Total Errors: {len(self.stats.errors)}",
        ]
        
        if self.stats.errors:
            lines.extend([
                "",
                "Error Details:",
                "-" * 50,
            ])
            
            # Group errors by pipeline
            errors_by_pipeline: Dict[str, List[ErrorDetail]] = defaultdict(list)
            for error in self.stats.errors:
                errors_by_pipeline[error.pipeline].append(error)
                
            for pipeline, errors in errors_by_pipeline.items():
                lines.append(f"\nPipeline: {pipeline}")
                for error in errors:
                    lines.extend([
                        f"  Dataset: {error.dataset}",
                        f"    Operation: {error.operation}",
                        f"    Error Type: {error.error_type}",
                        f"    Message: {error.error_message}",
                        f"    Timestamp: {error.timestamp.isoformat()}",
                    ])
                    if error.duration_seconds is not None:
                        lines.append(f"    Duration: {error.duration_seconds:.2f}s")
                    lines.append("")
                    
        return "\n".join(lines)
        
    def save_report(self, filepath: Path) -> None:
        """Save detailed statistics report to JSON file."""
        # Convert stats to dict, handling datetime objects
        def json_encoder(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if hasattr(obj, "__dict__"):
                return obj.__dict__
            return str(obj)
            
        report_data = {
            "execution_stats": asdict(self.stats),
            "summary": self.get_summary()
        }
        
        with open(filepath, "w") as f:
            json.dump(report_data, f, indent=2, default=json_encoder)
            
    def stop(self) -> None:
        """Finalize statistics on stop."""
        self.stats.end_time = datetime.now()