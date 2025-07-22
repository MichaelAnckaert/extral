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
"""TUI handler for ncurses-based interface."""

import curses
import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from extral.context import ApplicationContext

from extral.events import Event, EventHandler, EventType

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class DatasetStatus:
    status: TaskStatus = TaskStatus.PENDING
    error_message: Optional[str] = None


@dataclass
class PipelineStatus:
    name: str
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    datasets: Dict[str, DatasetStatus] = field(default_factory=dict)
    workers: int = 4
    error_message: Optional[str] = None
    total_datasets: int = 0
    completed_datasets: int = 0
    failed_datasets: int = 0


@dataclass
class Statistics:
    start_time: datetime = field(default_factory=datetime.now)
    total_pipelines: int = 0
    completed_pipelines: int = 0
    failed_pipelines: int = 0
    total_datasets: int = 0
    completed_datasets: int = 0
    failed_datasets: int = 0
    skipped_datasets: int = 0
    active_workers: int = 0


class TUIHandler(EventHandler):
    """Handler that displays events in an ncurses TUI interface."""
    
    def __init__(self):
        self.pipelines: Dict[str, PipelineStatus] = {}
        self.statistics = Statistics()
        self.log_entries: deque[Tuple[str, str, str]] = deque(maxlen=100)
        self.recent_errors: deque[Tuple[str, str, str]] = deque(maxlen=10)
        self.lock = threading.Lock()
        self.stdscr: Optional[curses.window] = None
        self.stats_win: Optional[curses.window] = None
        self.pipeline_win: Optional[curses.window] = None
        self.log_win: Optional[curses.window] = None
        self.running = False
        self.ui_thread: Optional[threading.Thread] = None
        self.dirty_stats = True
        self.dirty_pipelines = True
        self.dirty_logs = True
        self.last_log_count = 0
        self.shutdown_requested = False
        self.shutdown_callback: Optional[Callable[[], None]] = None
        self.log_scroll_offset = 0
        self.view_mode = "normal"  # "normal" or "fullscreen_logs"
        self.context: Optional['ApplicationContext'] = None
        self.last_stats_update = 0.0
        self.last_pipeline_update = 0.0
        self.stats_update_interval = 1.0  # Update stats max once per second
        self.pipeline_update_interval = 2.0  # Update pipelines max once per 2 seconds
        
    def handle(self, event: Event) -> None:
        """Handle an event by updating TUI state."""
        with self.lock:
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
            elif event.type == EventType.ERROR_OCCURRED:
                self._handle_error_occurred(event)
            elif event.type == EventType.LOG_MESSAGE:
                self._handle_log_message(event)
            elif event.type == EventType.WORKER_UPDATE:
                self._handle_worker_update(event)
                
    def _handle_pipeline_started(self, event: Event) -> None:
        """Handle pipeline started event."""
        if not event.pipeline:
            return
            
        pipeline = PipelineStatus(
            name=event.pipeline,
            status=TaskStatus.RUNNING,
            start_time=event.timestamp,
            workers=event.data.get("workers", 4),
            total_datasets=event.data.get("dataset_count", 0)
        )
        self.pipelines[event.pipeline] = pipeline
        self.statistics.total_pipelines += 1
        self.statistics.total_datasets += pipeline.total_datasets
        self.dirty_pipelines = True
        self.dirty_stats = True
        
    def _handle_pipeline_completed(self, event: Event) -> None:
        """Handle pipeline completed event."""
        if not event.pipeline or event.pipeline not in self.pipelines:
            return
            
        pipeline = self.pipelines[event.pipeline]
        pipeline.status = TaskStatus.COMPLETED
        pipeline.end_time = event.timestamp
        pipeline.completed_datasets = event.data.get("successful_datasets", 0)
        pipeline.failed_datasets = event.data.get("failed_datasets", 0)
        self.statistics.completed_pipelines += 1
        self.dirty_pipelines = True
        self.dirty_stats = True
        
    def _handle_pipeline_failed(self, event: Event) -> None:
        """Handle pipeline failed event."""
        if not event.pipeline or event.pipeline not in self.pipelines:
            return
            
        pipeline = self.pipelines[event.pipeline]
        pipeline.status = TaskStatus.FAILED
        pipeline.end_time = event.timestamp
        pipeline.error_message = event.data.get("error_message")
        self.statistics.failed_pipelines += 1
        self.dirty_pipelines = True
        self.dirty_stats = True
        
    def _handle_dataset_started(self, event: Event) -> None:
        """Handle dataset started event."""
        if not event.pipeline or not event.dataset:
            return
            
        if event.pipeline in self.pipelines:
            self.pipelines[event.pipeline].datasets[event.dataset] = DatasetStatus(
                status=TaskStatus.RUNNING
            )
            self.dirty_pipelines = True
            
        # Add to log output
        timestamp = event.timestamp.strftime("%H:%M:%S")
        self.log_entries.append((timestamp, "INFO", f"Processing dataset: {event.dataset}"))
        self.dirty_logs = True
            
    def _handle_dataset_completed(self, event: Event) -> None:
        """Handle dataset completed event."""
        if not event.pipeline or not event.dataset:
            return
            
        if event.pipeline in self.pipelines:
            pipeline = self.pipelines[event.pipeline]
            if event.dataset in pipeline.datasets:
                pipeline.datasets[event.dataset].status = TaskStatus.COMPLETED
                pipeline.completed_datasets += 1
            self.statistics.completed_datasets += 1
            self.dirty_pipelines = True
            self.dirty_stats = True
            
        # Add to log output
        timestamp = event.timestamp.strftime("%H:%M:%S")
        self.log_entries.append((timestamp, "INFO", f"Completed processing dataset: {event.dataset}"))
        self.dirty_logs = True
            
    def _handle_dataset_failed(self, event: Event) -> None:
        """Handle dataset failed event."""
        if not event.pipeline or not event.dataset:
            return
            
        if event.pipeline in self.pipelines:
            pipeline = self.pipelines[event.pipeline]
            if event.dataset in pipeline.datasets:
                dataset_status = pipeline.datasets[event.dataset]
                dataset_status.status = TaskStatus.FAILED
                dataset_status.error_message = event.data.get("error_message")
                pipeline.failed_datasets += 1
                
            # Track recent errors
            if event.data.get("error_message"):
                self.recent_errors.append((
                    event.pipeline,
                    event.dataset,
                    event.data["error_message"]
                ))
                
            self.statistics.failed_datasets += 1
            self.dirty_pipelines = True
            self.dirty_stats = True
            
    def _handle_dataset_skipped(self, event: Event) -> None:
        """Handle dataset skipped event."""
        if not event.pipeline or not event.dataset:
            return
            
        if event.pipeline in self.pipelines:
            self.pipelines[event.pipeline].datasets[event.dataset] = DatasetStatus(
                status=TaskStatus.SKIPPED
            )
            self.statistics.skipped_datasets += 1
            self.dirty_pipelines = True
            self.dirty_stats = True
            
        # Add to log output
        timestamp = event.timestamp.strftime("%H:%M:%S")
        reason = event.data.get("reason", "Unknown reason")
        self.log_entries.append((timestamp, "INFO", f"Skipped dataset {event.dataset}: {reason}"))
        self.dirty_logs = True
            
    def _handle_error_occurred(self, event: Event) -> None:
        """Handle error occurred event."""
        # Add to log
        timestamp = event.timestamp.strftime("%H:%M:%S")
        self.log_entries.append((timestamp, "ERROR", event.data.get("error_message", "")))
        self.dirty_logs = True
        
        # Track in recent errors
        if event.pipeline and event.dataset:
            self.recent_errors.append((
                event.pipeline,
                event.dataset,
                event.data.get("error_message", "Unknown error")
            ))
            self.dirty_stats = True
            
    def _handle_log_message(self, event: Event) -> None:
        """Handle log message event."""
        timestamp = event.timestamp.strftime("%H:%M:%S")
        level = event.data.get("level", "INFO")
        message = event.message or ""
        self.log_entries.append((timestamp, level, message))
        self.dirty_logs = True
        
    def _handle_worker_update(self, event: Event) -> None:
        """Handle worker update event."""
        self.statistics.active_workers = event.data.get("active_count", 0)
        self.dirty_stats = True
        
    def start(self) -> None:
        """Start the TUI interface."""
        if self.running:
            return
            
        self.running = True
        self.ui_thread = threading.Thread(target=self._run_ui, daemon=True)
        self.ui_thread.start()
        
    def stop(self) -> None:
        """Stop the TUI interface."""
        self.running = False
        if self.ui_thread:
            self.ui_thread.join(timeout=1.0)
            
    def set_shutdown_callback(self, callback: Callable[[], None]) -> None:
        """Set callback to execute when shutdown is requested."""
        self.shutdown_callback = callback
        
    def set_context(self, context: 'ApplicationContext') -> None:
        """Set reference to application context."""
        self.context = context
            
    def _run_ui(self) -> None:
        """Run the UI in a separate thread."""
        try:
            curses.wrapper(self._init_curses)
        except Exception as e:
            logger.error(f"TUI error: {e}")
            
    def _init_curses(self, stdscr: Any) -> None:
        """Initialize curses and start the main loop."""
        self.stdscr = stdscr
        curses.start_color()
        stdscr.nodelay(1)  # Non-blocking input
        stdscr.timeout(1000)   # Refresh every 1000ms (less frequent)

        # Initialize color pairs
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)  # Success
        curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)    # Error
        curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK) # Warning/Running
        curses.init_pair(4, curses.COLOR_BLUE, curses.COLOR_BLACK)   # Info
        curses.init_pair(5, curses.COLOR_CYAN, curses.COLOR_BLACK)   # Header

        # Create windows layout
        self._create_windows()
        self._initial_draw()

        # Main loop - only update when needed
        while self.running:
            try:
                key = stdscr.getch()
                if key == ord('q') or key == 27:  # q or ESC
                    if self.view_mode == "fullscreen_logs":
                        self.view_mode = "normal"
                        self._handle_resize()  # Recreate layout
                    else:
                        if self._show_exit_confirmation():
                            self._request_shutdown()
                            break
                elif key == ord('l') or key == ord('L'):  # Toggle log view
                    self._toggle_log_view()
                elif key == curses.KEY_UP:  # Scroll up in logs
                    self._scroll_logs(-1)
                elif key == curses.KEY_DOWN:  # Scroll down in logs
                    self._scroll_logs(1)
                elif key == curses.KEY_PPAGE:  # Page up in logs
                    self._scroll_logs(-10)
                elif key == curses.KEY_NPAGE:  # Page down in logs
                    self._scroll_logs(10)
                elif key == curses.KEY_HOME:  # Go to top of logs
                    self.log_scroll_offset = 0
                    self.dirty_logs = True
                elif key == curses.KEY_END:  # Go to bottom of logs
                    self.log_scroll_offset = max(0, len(self.log_entries) - 10)
                    self.dirty_logs = True
                elif key == curses.KEY_RESIZE:
                    self._handle_resize()
                elif key == -1:  # Timeout - check for updates
                    self._update_if_needed()
            except KeyboardInterrupt:
                if self._show_exit_confirmation():
                    self._request_shutdown()
                    break

    def _create_windows(self) -> None:
        """Create separate windows for each section."""
        if not self.stdscr:
            return
            
        height, width = self.stdscr.getmaxyx()
        
        if self.view_mode == "fullscreen_logs":
            # Full-screen log view
            self.stats_win = None
            self.pipeline_win = None
            self.log_win = curses.newwin(height - 4, width, 2, 0)
        else:
            # Normal view with all sections
            # Statistics window (top section)
            self.stats_win = curses.newwin(10, width, 2, 0)
            
            # Pipeline window (middle section)
            pipeline_height = max(5, (height - 20) // 2)
            self.pipeline_win = curses.newwin(pipeline_height, width, 12, 0)
            
            # Log window (bottom section)
            log_start = 12 + pipeline_height + 1
            log_height = height - log_start - 2
            self.log_win = curses.newwin(log_height, width, log_start, 0)
        
        # Enable scrolling for log window
        if self.log_win:
            self.log_win.scrollok(True)
            
    def _initial_draw(self) -> None:
        """Draw the initial screen layout."""
        if not self.stdscr:
            return
            
        height, width = self.stdscr.getmaxyx()
        
        # Draw static elements
        self.stdscr.erase()
        self._draw_header(width)
        self._draw_footer(height - 1, width)
        self.stdscr.refresh()
        
        # Mark all sections as dirty for initial draw
        self.dirty_stats = True
        self.dirty_pipelines = True
        self.dirty_logs = True
        self._update_if_needed()
        
    def _handle_resize(self) -> None:
        """Handle terminal resize."""
        curses.endwin()
        curses.initscr()
        self._create_windows()
        self._initial_draw()
        
    def _update_if_needed(self) -> None:
        """Only update sections that have changed."""
        if not self.stdscr:
            return
            
        try:
            with self.lock:
                import time
                current_time = time.time()
                
                if self.view_mode == "fullscreen_logs":
                    # Only update logs in full-screen mode
                    if self.dirty_logs or len(self.log_entries) != self.last_log_count:
                        self._update_logs()
                        self.dirty_logs = False
                        self.last_log_count = len(self.log_entries)
                else:
                    # Normal mode - update all sections with throttling
                    # Update statistics with time-based throttling
                    if self.dirty_stats or (self._has_running_pipelines() and 
                                          current_time - self.last_stats_update > self.stats_update_interval):
                        self._update_statistics()
                        self.dirty_stats = False
                        self.last_stats_update = current_time
                        
                    # Update pipelines with time-based throttling  
                    if self.dirty_pipelines or (self._has_running_pipelines() and
                                               current_time - self.last_pipeline_update > self.pipeline_update_interval):
                        self._update_pipelines()
                        self.dirty_pipelines = False
                        self.last_pipeline_update = current_time
                        
                    # Update logs (more responsive for user interaction)
                    if self.dirty_logs or len(self.log_entries) != self.last_log_count:
                        self._update_logs()
                        self.dirty_logs = False
                        self.last_log_count = len(self.log_entries)
        except Exception:
            pass
            
    def _has_running_pipelines(self) -> bool:
        """Check if any pipelines are currently running (need time updates)."""
        return any(pipeline.status == TaskStatus.RUNNING for pipeline in self.pipelines.values())
            
    def _draw_header(self, width: int) -> None:
        """Draw the header with title."""
        if not self.stdscr:
            return
        title = "Extral ETL Tool - TUI Mode"
        self.stdscr.addstr(0, (width - len(title)) // 2, title, 
                          curses.color_pair(5) | curses.A_BOLD)
        self.stdscr.addstr(1, 0, "─" * width, curses.color_pair(5))
        
    def _draw_footer(self, row: int, width: int) -> None:   
        """Draw the footer with instructions."""
        if not self.stdscr:
            return
            
        if self.view_mode == "fullscreen_logs":
            footer = "Press 'q'/ESC: back | ↑↓: scroll | PgUp/PgDn: page | Home/End: top/bottom"
        else:
            footer = "Press 'q'/ESC: exit | 'l': logs | ↑↓: scroll logs"
            
        # Truncate if too long for screen
        if len(footer) > width - 4:
            footer = footer[:width-7] + "..."
            
        self.stdscr.addstr(row, (width - len(footer)) // 2, footer, 
                          curses.color_pair(5))
                          
    def _update_statistics(self) -> None:
        """Update the statistics window."""
        if not self.stats_win:
            return
            
        # Only clear if window dimensions might have changed
        height, width = self.stats_win.getmaxyx()
        self.stats_win.erase()  # erase() is less jarring than clear()
        self.stats_win.addstr(0, 0, "Statistics", 
                             curses.color_pair(5) | curses.A_BOLD)
        
        runtime = datetime.now() - self.statistics.start_time
        hours, remainder = divmod(int(runtime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        stats_lines = [
            f"Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}",
            f"Active Workers: {self.statistics.active_workers}",
            f"Pipelines: {self.statistics.completed_pipelines}/{self.statistics.total_pipelines} (Failed: {self.statistics.failed_pipelines})",
            f"Datasets: {self.statistics.completed_datasets}/{self.statistics.total_datasets} (Failed: {self.statistics.failed_datasets}, Skipped: {self.statistics.skipped_datasets})",
        ]
        
        # Add log file info
        if self.context and hasattr(self.context, 'log_file_path'):
            log_file = self.context.log_file_path
            if log_file:
                # Show just the filename to save space
                from pathlib import Path
                log_filename = Path(log_file).name
                stats_lines.append(f"Log file: {log_filename}")
        
        # Add recent errors if any
        if self.recent_errors:
            stats_lines.append("")
            stats_lines.append("Recent Errors:")
            for pipeline, dataset, error in list(self.recent_errors)[-3:]:
                error_line = f"  {pipeline}/{dataset}: {error[:50]}..."
                stats_lines.append(error_line)
        
        for i, line in enumerate(stats_lines):
            if i + 1 < self.stats_win.getmaxyx()[0]:
                self.stats_win.addstr(i + 1, 2, line)
                
        self.stats_win.refresh()
        
    def _update_pipelines(self) -> None:
        """Update the pipeline status window with three columns."""
        if not self.pipeline_win:
            return
            
        height, width = self.pipeline_win.getmaxyx()
        self.pipeline_win.erase()  # Use erase() instead of clear()
        self.pipeline_win.addstr(0, 0, "Pipeline Status", 
                                curses.color_pair(5) | curses.A_BOLD)
        
        # Calculate column widths (divide available space into 3 columns)
        col_width = (width - 6) // 3  # -6 for padding/borders
        
        row = 1
        for pipeline_name, pipeline in list(self.pipelines.items()):
            if row >= height - 1:
                break
                
            # Pipeline header with status
            status_color = self._get_status_color(pipeline.status)
            status_symbol = self._get_status_symbol(pipeline.status)
            
            pipeline_line = f"{status_symbol} {pipeline_name}"
            if pipeline.start_time and pipeline.status == TaskStatus.RUNNING:
                elapsed = datetime.now() - pipeline.start_time
                pipeline_line += f" ({int(elapsed.total_seconds())}s)"
            
            # Truncate pipeline name if too long
            if len(pipeline_line) > width - 4:
                pipeline_line = pipeline_line[:width-7] + "..."
            
            self.pipeline_win.addstr(row, 2, pipeline_line, status_color | curses.A_BOLD)
            row += 1
            
            if row >= height - 1:
                break
                
            # Column headers
            if row < height - 1:
                self.pipeline_win.addstr(row, 4, "Active", curses.color_pair(3) | curses.A_UNDERLINE)
                self.pipeline_win.addstr(row, 4 + col_width, "Finished", curses.color_pair(1) | curses.A_UNDERLINE)  
                self.pipeline_win.addstr(row, 4 + 2 * col_width, "Waiting", curses.color_pair(4) | curses.A_UNDERLINE)
                row += 1
            
            # Categorize datasets
            active_datasets = []
            finished_datasets = []
            waiting_datasets = []
            
            for dataset_name, dataset_info in pipeline.datasets.items():
                if dataset_info.status == TaskStatus.RUNNING:
                    active_datasets.append(dataset_name)
                elif dataset_info.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED]:
                    finished_datasets.append(dataset_name)
                else:  # PENDING
                    waiting_datasets.append(dataset_name)
            
            # Find datasets that haven't been started yet (not in pipeline.datasets)
            if hasattr(pipeline, 'total_datasets') and pipeline.total_datasets > 0:
                # If we know total count but don't have all datasets tracked yet
                missing_count = pipeline.total_datasets - len(pipeline.datasets)
                if missing_count > 0:
                    for i in range(missing_count):
                        waiting_datasets.append(f"dataset_{i+len(pipeline.datasets)+1}")
            
            # Display datasets in columns (limit to available height)
            max_rows_per_section = min(5, height - row - 2)  # Leave space for next pipeline
            
            for i in range(max_rows_per_section):
                if row >= height - 1:
                    break
                    
                # Active column
                if i < len(active_datasets):
                    name = active_datasets[i]
                    if len(name) > col_width - 2:
                        name = name[:col_width-5] + "..."
                    self.pipeline_win.addstr(row, 4, f"▶ {name}"[:col_width-1], curses.color_pair(3))
                
                # Finished column  
                if i < len(finished_datasets):
                    name = finished_datasets[i]
                    if len(name) > col_width - 2:
                        name = name[:col_width-5] + "..."
                    # Get the actual status for the finished dataset
                    dataset_status = pipeline.datasets.get(name)
                    if dataset_status:
                        symbol = self._get_status_symbol(dataset_status.status)
                        color = self._get_status_color(dataset_status.status)
                    else:
                        symbol = "✓"
                        color = curses.color_pair(1)
                    self.pipeline_win.addstr(row, 4 + col_width, f"{symbol} {name}"[:col_width-1], color)
                
                # Waiting column
                if i < len(waiting_datasets):
                    name = waiting_datasets[i]
                    if len(name) > col_width - 2:
                        name = name[:col_width-5] + "..."
                    self.pipeline_win.addstr(row, 4 + 2 * col_width, f"○ {name}"[:col_width-1], curses.color_pair(4))
                    
                row += 1
            
            # Show "more" indicators if there are additional datasets
            if row < height - 1:
                more_indicators = []
                if len(active_datasets) > max_rows_per_section:
                    more_indicators.append((f"+{len(active_datasets) - max_rows_per_section} more", 4, curses.color_pair(3)))
                if len(finished_datasets) > max_rows_per_section:
                    more_indicators.append((f"+{len(finished_datasets) - max_rows_per_section} more", 4 + col_width, curses.color_pair(1)))
                if len(waiting_datasets) > max_rows_per_section:
                    more_indicators.append((f"+{len(waiting_datasets) - max_rows_per_section} more", 4 + 2 * col_width, curses.color_pair(4)))
                
                if more_indicators:
                    for text, x_pos, color in more_indicators:
                        self.pipeline_win.addstr(row, x_pos, text[:col_width-1], color)
                    row += 1
            
            # Add spacing between pipelines
            row += 1
                
        self.pipeline_win.refresh()
        
    def _update_logs(self) -> None:
        """Update the log output window with scrolling support."""
        if not self.log_win:
            return
            
        height, width = self.log_win.getmaxyx()
        self.log_win.erase()  # Use erase() instead of clear()
        
        # Header with scroll info
        if self.view_mode == "fullscreen_logs":
            scroll_info = f" (showing {self.log_scroll_offset + 1}-{min(self.log_scroll_offset + height - 2, len(self.log_entries))} of {len(self.log_entries)})"
            header = f"Log Output{scroll_info}"
        else:
            header = "Log Output"
            
        self.log_win.addstr(0, 0, header[:width-4], 
                           curses.color_pair(5) | curses.A_BOLD)
        
        # Calculate visible logs based on scroll offset
        all_logs = list(self.log_entries)
        if self.view_mode == "fullscreen_logs":
            # In fullscreen mode, use scroll offset for navigation
            start_idx = max(0, len(all_logs) - height + 2 - self.log_scroll_offset)
            end_idx = len(all_logs) - self.log_scroll_offset
            visible_logs = all_logs[start_idx:end_idx]
        else:
            # In normal mode, show recent logs with limited scrolling
            available_lines = height - 2
            if self.log_scroll_offset == 0:
                # Show most recent logs
                visible_logs = all_logs[-available_lines:]
            else:
                # Show logs based on scroll offset
                start_idx = max(0, len(all_logs) - available_lines - self.log_scroll_offset)
                end_idx = len(all_logs) - self.log_scroll_offset
                visible_logs = all_logs[start_idx:end_idx]
        
        # Display logs
        for i, log_entry in enumerate(visible_logs):
            row = i + 1
            if row < height:
                timestamp, level, message = log_entry
                log_line = f"[{timestamp}] {level}: {message}"
                
                color = curses.color_pair(0)
                if level == "ERROR":
                    color = curses.color_pair(2)
                elif level == "WARNING":
                    color = curses.color_pair(3)
                elif level == "INFO":
                    color = curses.color_pair(4)
                
                self.log_win.addstr(row, 2, log_line[:width-4], color)
                
        self.log_win.refresh()
        
    def _get_status_color(self, status: TaskStatus) -> int:
        """Get color pair for status."""
        if status == TaskStatus.COMPLETED:
            return curses.color_pair(1)  # Green
        elif status == TaskStatus.FAILED:
            return curses.color_pair(2)  # Red
        elif status == TaskStatus.RUNNING:
            return curses.color_pair(3)  # Yellow
        elif status == TaskStatus.SKIPPED:
            return curses.color_pair(4)  # Blue
        else:
            return curses.color_pair(0)  # Default
            
    def _get_status_symbol(self, status: TaskStatus) -> str:
        """Get symbol for status."""
        if status == TaskStatus.COMPLETED:
            return "✓"
        elif status == TaskStatus.FAILED:
            return "✗"
        elif status == TaskStatus.RUNNING:
            return "▶"
        elif status == TaskStatus.SKIPPED:
            return "⊘"
        else:
            return "○"
            
    def _show_exit_confirmation(self) -> bool:
        """Show exit confirmation dialog. Returns True if user confirms exit."""
        if not self.stdscr:
            return True
            
        # Check if there are active pipelines
        active_pipelines = [p for p in self.pipelines.values() if p.status == TaskStatus.RUNNING]
        
        height, width = self.stdscr.getmaxyx()
        
        # Create confirmation dialog
        dialog_height = 8 if active_pipelines else 6
        dialog_width = 60
        dialog_y = (height - dialog_height) // 2
        dialog_x = (width - dialog_width) // 2
        
        # Create dialog window
        dialog = curses.newwin(dialog_height, dialog_width, dialog_y, dialog_x)
        dialog.box()
        
        try:
            # Show title
            title = "Exit Confirmation"
            dialog.addstr(1, (dialog_width - len(title)) // 2, title, curses.A_BOLD)
            
            row = 3
            if active_pipelines:
                warning = "WARNING: Processing is still active!"
                dialog.addstr(row, (dialog_width - len(warning)) // 2, warning, 
                            curses.color_pair(2) | curses.A_BOLD)
                row += 1
                active_msg = f"{len(active_pipelines)} pipeline(s) are still running."
                dialog.addstr(row, (dialog_width - len(active_msg)) // 2, active_msg)
                row += 1
            
            question = "Are you sure you want to exit?"
            dialog.addstr(row, (dialog_width - len(question)) // 2, question)
            row += 1
            
            options = "Press Y to exit, N to cancel"
            dialog.addstr(row, (dialog_width - len(options)) // 2, options)
            
            dialog.refresh()
            
            # Wait for user input
            while True:
                key = dialog.getch()
                if key == ord('y') or key == ord('Y'):
                    return True
                elif key == ord('n') or key == ord('N') or key == 27:  # N, n, or ESC
                    return False
                    
        finally:
            # Clean up dialog
            dialog.erase()
            dialog.refresh()
            del dialog
            
            # Refresh main screen
            if self.stdscr:
                self.stdscr.refresh()
                self._update_if_needed()
                
    def _request_shutdown(self) -> None:
        """Request application shutdown."""
        self.shutdown_requested = True
        if self.shutdown_callback:
            self.shutdown_callback()
            
    def _toggle_log_view(self) -> None:
        """Toggle between normal and full-screen log view."""
        if self.view_mode == "normal":
            self.view_mode = "fullscreen_logs"
        else:
            self.view_mode = "normal"
        self._handle_resize()  # Recreate layout
        
    def _scroll_logs(self, delta: int) -> None:
        """Scroll the log view by delta lines."""
        max_offset = max(0, len(self.log_entries) - 10)
        self.log_scroll_offset = max(0, min(max_offset, self.log_scroll_offset + delta))
        self.dirty_logs = True