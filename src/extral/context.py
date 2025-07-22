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
"""Application context for managing events and state."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from extral.config import Config
from extral.events import Event, EventBus, EventType
from extral.handlers import ConsoleHandler, StatsHandler, TUIHandler
from extral.state import State

logger = logging.getLogger(__name__)


class ApplicationContext:
    """Central context that manages events, state, and configuration."""
    
    def __init__(self, config: Config, use_tui: bool = False):
        self.config = config
        self.event_bus = EventBus()
        self.state = State()
        self._start_time = datetime.now()
        self.stats_handler = StatsHandler()
        self.tui_handler: Optional[TUIHandler] = None
        self.use_tui = use_tui or config.logging.mode == "tui"
        
        # Set up file logging and clean console handlers for TUI mode
        self._setup_file_logging()
        
        # Register handlers based on configuration
        self.event_bus.register_handler(self.stats_handler)
        
        if self.use_tui:
            self.tui_handler = TUIHandler()
            self.tui_handler.set_context(self)
            self.event_bus.register_handler(self.tui_handler)
        else:
            level = config.logging.level.upper()
            self.event_bus.register_handler(ConsoleHandler(level=level))
        
    def start(self) -> None:
        """Start the application context."""
        self.event_bus.start()
        logger.debug("Application context started")
        
    def stop(self) -> None:
        """Stop the application context."""
        self.event_bus.stop()
        logger.debug("Application context stopped")
        
    def get_stats_summary(self) -> str:
        """Get execution statistics summary."""
        return self.stats_handler.get_summary()
        
    def save_stats_report(self, filepath: str) -> None:
        """Save statistics report to file."""
        from pathlib import Path
        self.stats_handler.save_report(Path(filepath))
        
    # Pipeline events
    
    def pipeline_started(self, name: str, dataset_count: int, workers: int = 4) -> None:
        """Emit pipeline started event."""
        self.event_bus.emit(Event(
            type=EventType.PIPELINE_STARTED,
            pipeline=name,
            data={
                "name": name,
                "dataset_count": dataset_count,
                "workers": workers,
            }
        ))
        
    def pipeline_completed(self, name: str, success: bool = True, 
                         successful_datasets: int = 0, failed_datasets: int = 0,
                         error_message: Optional[str] = None) -> None:
        """Emit pipeline completed event."""
        event_type = EventType.PIPELINE_COMPLETED if success else EventType.PIPELINE_FAILED
        self.event_bus.emit(Event(
            type=event_type,
            pipeline=name,
            data={
                "name": name,
                "success": success,
                "successful_datasets": successful_datasets,
                "failed_datasets": failed_datasets,
                "error_message": error_message,
            }
        ))
        
    # Dataset events
    
    def dataset_started(self, pipeline: str, dataset: str) -> None:
        """Emit dataset started event."""
        self.event_bus.emit(Event(
            type=EventType.DATASET_STARTED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
            }
        ))
        
    def dataset_completed(self, pipeline: str, dataset: str, success: bool = True,
                        error: Optional[Exception] = None, 
                        duration_seconds: Optional[float] = None) -> None:
        """Emit dataset completed event."""
        event_type = EventType.DATASET_COMPLETED if success else EventType.DATASET_FAILED
        self.event_bus.emit(Event(
            type=event_type,
            pipeline=pipeline,
            dataset=dataset,
            error=error,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "success": success,
                "error_message": str(error) if error else None,
                "duration_seconds": duration_seconds,
            }
        ))
        
    def dataset_skipped(self, pipeline: str, dataset: str, reason: str) -> None:
        """Emit dataset skipped event."""
        self.event_bus.emit(Event(
            type=EventType.DATASET_SKIPPED,
            pipeline=pipeline,
            dataset=dataset,
            message=reason,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "reason": reason,
            }
        ))
        
    # Operation events
    
    def extract_started(self, pipeline: str, dataset: str) -> None:
        """Emit extract started event."""
        self.event_bus.emit(Event(
            type=EventType.EXTRACT_STARTED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
            }
        ))
        
    def extract_completed(self, pipeline: str, dataset: str, 
                        records: Optional[int] = None,
                        file_path: Optional[str] = None) -> None:
        """Emit extract completed event."""
        self.event_bus.emit(Event(
            type=EventType.EXTRACT_COMPLETED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "records": records,
                "file_path": file_path,
            }
        ))
        
    def load_started(self, pipeline: str, dataset: str) -> None:
        """Emit load started event."""
        self.event_bus.emit(Event(
            type=EventType.LOAD_STARTED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
            }
        ))
        
    def load_completed(self, pipeline: str, dataset: str, 
                     records: Optional[int] = None) -> None:
        """Emit load completed event."""
        self.event_bus.emit(Event(
            type=EventType.LOAD_COMPLETED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "records": records,
            }
        ))
        
    # System events
    
    def error_occurred(self, pipeline: str, dataset: str, operation: str,
                     error: Exception, duration_seconds: Optional[float] = None,
                     include_stack_trace: bool = False) -> None:
        """Emit error occurred event."""
        import traceback
        
        self.event_bus.emit(Event(
            type=EventType.ERROR_OCCURRED,
            pipeline=pipeline,
            dataset=dataset,
            error=error,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "operation": operation,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "duration_seconds": duration_seconds,
                "stack_trace": traceback.format_exc() if include_stack_trace else None,
            }
        ))
        
    def log_message(self, level: str, message: str, **kwargs: Any) -> None:
        """Emit log message event."""
        self.event_bus.emit(Event(
            type=EventType.LOG_MESSAGE,
            message=message,
            data={
                "level": level,
                "message": message,
                **kwargs
            }
        ))
        
    def update_workers(self, active_count: int) -> None:
        """Emit worker update event."""
        self.event_bus.emit(Event(
            type=EventType.WORKER_UPDATE,
            data={
                "active_count": active_count,
            }
        ))
        
    def update_state(self, pipeline: str, dataset: str, 
                   state_data: Dict[str, Any]) -> None:
        """Emit state update event."""
        self.event_bus.emit(Event(
            type=EventType.STATE_UPDATE,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "state_data": state_data,
            }
        ))
        
    # Progress events
    
    def records_processed(self, pipeline: str, dataset: str, 
                        count: int, total: Optional[int] = None) -> None:
        """Emit records processed event."""
        self.event_bus.emit(Event(
            type=EventType.RECORDS_PROCESSED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "count": count,
                "total": total,
            }
        ))
        
    def batch_completed(self, pipeline: str, dataset: str, 
                      batch_num: int, batch_size: int) -> None:
        """Emit batch completed event."""
        self.event_bus.emit(Event(
            type=EventType.BATCH_COMPLETED,
            pipeline=pipeline,
            dataset=dataset,
            data={
                "pipeline": pipeline,
                "dataset": dataset,
                "batch_num": batch_num,
                "batch_size": batch_size,
            }
        ))
        
    def _setup_file_logging(self) -> None:
        """Set up file logging for the application."""
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Create timestamped log file
        timestamp = self._start_time.strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"extral_{timestamp}.log"
        
        # Configure file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        # Get root logger and configure it properly
        root_logger = logging.getLogger()
        
        # If using TUI, remove ALL existing handlers to prevent console interference
        if self.use_tui:
            # Clear all existing handlers
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # Also clear handlers from any child loggers that might interfere
            for name in logging.Logger.manager.loggerDict:
                child_logger = logging.getLogger(name)
                if child_logger.handlers:
                    for handler in child_logger.handlers[:]:
                        if isinstance(handler, logging.StreamHandler):
                            child_logger.removeHandler(handler)
        
        # Add only the file handler
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.DEBUG)
        
        # Store log file path for reference
        self.log_file_path = str(log_file)
        logger.info(f"File logging enabled: {self.log_file_path}")
        
    def get_log_file_path(self) -> Optional[str]:
        """Get the current log file path."""
        return getattr(self, 'log_file_path', None)
        
    @property
    def runtime(self) -> float:
        """Get the runtime in seconds since context started."""
        return (datetime.now() - self._start_time).total_seconds()