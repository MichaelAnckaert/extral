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
"""Console handler for standard logging output."""

import logging
import sys
from typing import Optional, TextIO

from extral.events import Event, EventHandler, EventType

logger = logging.getLogger(__name__)


class ConsoleHandler(EventHandler):
    """Handler that outputs events to console in a clean, user-friendly format."""
    
    def __init__(self, level: str = "WARNING", output: Optional[TextIO] = None):
        self.level = getattr(logging, level.upper(), logging.WARNING)
        self.output = output or sys.stdout
        self._pipeline_datasets: dict[str, int] = {}  # Track datasets per pipeline
        
    def handle(self, event: Event) -> None:
        """Handle an event by outputting to console."""
        # Map event types to log levels
        level_map = {
            EventType.ERROR_OCCURRED: logging.ERROR,
            EventType.PIPELINE_FAILED: logging.ERROR,
            EventType.DATASET_FAILED: logging.ERROR,
            EventType.PIPELINE_STARTED: logging.WARNING,
            EventType.PIPELINE_COMPLETED: logging.WARNING,
            EventType.DATASET_STARTED: logging.WARNING,  # Show dataset processing at WARNING level
            EventType.DATASET_COMPLETED: logging.WARNING,  # Show completion at WARNING level
            EventType.DATASET_SKIPPED: logging.WARNING,
            EventType.LOG_MESSAGE: self._get_log_level(event),
        }
        
        level = level_map.get(event.type, logging.DEBUG)
        
        # Only output if level is high enough
        if level < self.level:
            return
            
        message = self._format_message(event)
        if message:
            self.output.write(message + "\n")
            self.output.flush()
            
    def _get_log_level(self, event: Event) -> int:
        """Get log level from LOG_MESSAGE event."""
        level_str = event.data.get("level", "INFO").upper()
        return getattr(logging, level_str, logging.INFO)
        
    def _format_message(self, event: Event) -> Optional[str]:
        """Format event into a user-friendly message."""
        if event.type == EventType.PIPELINE_STARTED:
            count = event.data.get("dataset_count", 0)
            if event.pipeline:
                self._pipeline_datasets[event.pipeline] = count
            return f"Processing pipeline: {event.pipeline}"
            
        elif event.type == EventType.PIPELINE_COMPLETED:
            success = event.data.get("successful_datasets", 0)
            failed = event.data.get("failed_datasets", 0)
            total = self._pipeline_datasets.get(event.pipeline, success + failed) if event.pipeline else success + failed
            if failed == 0:
                return f"Successfully completed pipeline: {event.pipeline}"
            else:
                return (f"Pipeline '{event.pipeline}' completed with errors. "
                       f"Successful datasets: {success}/{total}")
                       
        elif event.type == EventType.PIPELINE_FAILED:
            error_msg = event.data.get("error_message", "Unknown error")
            return f"Pipeline '{event.pipeline}' failed: {error_msg}"
            
        elif event.type == EventType.DATASET_STARTED:
            return f"Processing dataset: {event.dataset}"
                
        elif event.type == EventType.DATASET_COMPLETED:
            return f"Completed processing dataset '{event.dataset}' in pipeline '{event.pipeline}'"
                
        elif event.type == EventType.DATASET_FAILED:
            error_msg = event.data.get("error_message", "Unknown error")
            return f"Error processing dataset '{event.dataset}': {error_msg}"
            
        elif event.type == EventType.DATASET_SKIPPED:
            reason = event.data.get("reason", "as requested")
            return f"Skipping dataset '{event.dataset}': {reason}"
            
        elif event.type == EventType.ERROR_OCCURRED:
            dataset = event.dataset
            operation = event.data.get("operation", "unknown")
            error_msg = event.data.get("error_message", "Unknown error")
            return f"Error in {operation} for dataset '{dataset}': {error_msg}"
            
        elif event.type == EventType.LOG_MESSAGE:
            return event.message
            
        elif event.type == EventType.EXTRACT_COMPLETED:
            if event.data.get("records") is None:
                return f"Skipping dataset load for '{event.dataset}' as there is no data extracted."
                
        return None
        
    def start(self) -> None:
        """Initialize console handler."""
        # Could add banner or initial message here
        pass
        
    def stop(self) -> None:
        """Cleanup console handler."""
        # Ensure all output is flushed
        self.output.flush()