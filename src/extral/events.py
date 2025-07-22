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
"""Event system for unified logging and statistics tracking."""

import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from queue import Queue
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of events that can occur during ETL execution."""
    
    # Pipeline events
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    
    # Dataset events
    DATASET_STARTED = "dataset_started"
    DATASET_COMPLETED = "dataset_completed"
    DATASET_FAILED = "dataset_failed"
    DATASET_SKIPPED = "dataset_skipped"
    
    # Operation events
    EXTRACT_STARTED = "extract_started"
    EXTRACT_COMPLETED = "extract_completed"
    LOAD_STARTED = "load_started"
    LOAD_COMPLETED = "load_completed"
    
    # System events
    ERROR_OCCURRED = "error_occurred"
    LOG_MESSAGE = "log_message"
    WORKER_UPDATE = "worker_update"
    STATE_UPDATE = "state_update"
    
    # Progress events
    RECORDS_PROCESSED = "records_processed"
    BATCH_COMPLETED = "batch_completed"


@dataclass
class Event:
    """Represents a single event in the system."""
    
    type: EventType
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    
    # Common fields that many events will have
    pipeline: Optional[str] = None
    dataset: Optional[str] = None
    error: Optional[Exception] = None
    message: Optional[str] = None
    
    def __post_init__(self):
        """Extract common fields from data if not explicitly set."""
        if not self.pipeline and "pipeline" in self.data:
            self.pipeline = self.data["pipeline"]
        if not self.dataset and "dataset" in self.data:
            self.dataset = self.data["dataset"]
        if not self.error and "error" in self.data:
            self.error = self.data["error"]
        if not self.message and "message" in self.data:
            self.message = self.data["message"]


class EventHandler(ABC):
    """Abstract base class for event handlers."""
    
    @abstractmethod
    def handle(self, event: Event) -> None:
        """Handle an event."""
        pass
    
    def start(self) -> None:
        """Called when the handler is started. Override if needed."""
        pass
    
    def stop(self) -> None:
        """Called when the handler is stopped. Override if needed."""
        pass


class EventBus:
    """Central event bus that distributes events to registered handlers."""
    
    def __init__(self):
        self.handlers: List[EventHandler] = []
        self.queue: Queue[Union[Event, None]] = Queue()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="EventBus")
        self.running = False
        
    def register_handler(self, handler: EventHandler) -> None:
        """Register a new event handler."""
        self.handlers.append(handler)
        logger.debug(f"Registered handler: {handler.__class__.__name__}")
        
    def emit(self, event: Event) -> None:
        """Emit an event to all handlers."""
        self.queue.put(event)
        
    def start(self) -> None:
        """Start processing events."""
        if self.running:
            return
            
        self.running = True
        
        # Start all handlers
        for handler in self.handlers:
            try:
                handler.start()
            except Exception as e:
                logger.error(f"Error starting handler {handler.__class__.__name__}: {e}")
        
        # Start event processing thread
        self.executor.submit(self._process_events)
        logger.debug("Event bus started")
        
    def stop(self) -> None:
        """Stop processing events."""
        if not self.running:
            return
            
        self.running = False
        
        # Send a None to wake up the processing thread
        self.queue.put(None)
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        # Stop all handlers
        for handler in self.handlers:
            try:
                handler.stop()
            except Exception as e:
                logger.error(f"Error stopping handler {handler.__class__.__name__}: {e}")
                
        logger.debug("Event bus stopped")
        
    def _process_events(self) -> None:
        """Process events from the queue."""
        while self.running:
            try:
                event = self.queue.get()
                
                # None is our signal to stop
                if event is None:
                    break
                    
                # Distribute to all handlers
                for handler in self.handlers:
                    try:
                        handler.handle(event)
                    except Exception as e:
                        logger.error(
                            f"Error in handler {handler.__class__.__name__} "
                            f"processing event {event.type}: {e}"
                        )
                        
            except Exception as e:
                logger.error(f"Error processing event: {e}")
                
        logger.debug("Event processing thread exiting")


class CompositeHandler(EventHandler):
    """Handler that delegates to multiple other handlers."""
    
    def __init__(self, handlers: List[EventHandler]):
        self.handlers = handlers
        
    def handle(self, event: Event) -> None:
        """Distribute event to all sub-handlers."""
        for handler in self.handlers:
            handler.handle(event)
            
    def start(self) -> None:
        """Start all sub-handlers."""
        for handler in self.handlers:
            handler.start()
            
    def stop(self) -> None:
        """Stop all sub-handlers."""
        for handler in self.handlers:
            handler.stop()