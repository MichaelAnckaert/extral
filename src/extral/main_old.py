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
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from extral import __version__
from extral.config import Config, ConnectorConfig, TableConfig, FileItemConfig, DatabaseConfig
from extral.extract import extract_table
from extral.load import load_data
from extral.state import state
from extral.error_tracking import ErrorTracker
from extral.validation import PipelineValidator, format_validation_report
from extral.tui_logger import TUILogger, TUIHandler

import argparse

logger = logging.getLogger(__name__)

DEFAULT_WORKER_COUNT = 4


def process_table(
    source_config: ConnectorConfig,
    destination_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    pipeline_name: str,
    error_tracker: ErrorTracker,
    tui_logger: Optional[TUILogger] = None,
) -> bool:
    """Process a single table/dataset. Returns True if successful, False otherwise."""
    start_time = time.time()
    try:
        if tui_logger:
            tui_logger.start_dataset(pipeline_name, dataset_config.name)
        else:
            logger.warning(f"Processing dataset: {dataset_config.name}")

        # Extract phase
        try:
            file_path, schema_path = extract_table(
                source_config, dataset_config, pipeline_name
            )
            if file_path is None or schema_path is None:
                skip_msg = f"Skipping dataset load for '{dataset_config.name}' as there is no data extracted."
                if tui_logger:
                    tui_logger.add_log_entry("INFO", skip_msg)
                    tui_logger.complete_dataset(pipeline_name, dataset_config.name, True)
                else:
                    logger.warning(skip_msg)
                return True
        except Exception as e:
            duration = time.time() - start_time
            error_tracker.track_error(
                pipeline=pipeline_name,
                dataset=dataset_config.name,
                operation="extract",
                exception=e,
                duration_seconds=duration,
                include_stack_trace=True,
            )
            if tui_logger:
                tui_logger.complete_dataset(pipeline_name, dataset_config.name, False, str(e))
            raise

        # Load phase
        try:
            load_data(
                destination_config,
                dataset_config,
                file_path,
                schema_path,
                pipeline_name,
            )
        except Exception as e:
            duration = time.time() - start_time
            error_tracker.track_error(
                pipeline=pipeline_name,
                dataset=dataset_config.name,
                operation="load",
                exception=e,
                duration_seconds=duration,
                include_stack_trace=True,
            )
            if tui_logger:
                tui_logger.complete_dataset(pipeline_name, dataset_config.name, False, str(e))
            raise

        if tui_logger:
            tui_logger.complete_dataset(pipeline_name, dataset_config.name, True)
        return True

    except Exception as e:
        error_msg = f"Error processing dataset '{dataset_config.name}': {e}"
        logger.error(error_msg)
        if tui_logger:
            tui_logger.add_log_entry("ERROR", error_msg)
            # Dataset completion with error already handled in extract/load phase
        return False


def _setup_logging(args: argparse.Namespace, tui_logger: Optional[TUILogger] = None):
    config = Config.read_config(args.config)
    logging_config = config.logging

    # Map string levels to logging constants
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO, 
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    
    level = level_map.get(logging_config.level.lower(), logging.WARNING)
    
    if logging_config.mode == "tui" and tui_logger:
        # TUI mode - minimal console output, send logs to TUI
        logging.basicConfig(
            level=logging.ERROR,  # Only show errors on console
            format="%(levelname)s: %(message)s"
        )
        # Add TUI handler for all logs
        root_logger = logging.getLogger()
        tui_handler = TUIHandler(tui_logger)
        tui_handler.setLevel(level)
        root_logger.addHandler(tui_handler)
    else:
        # Standard mode - clean, simple format
        logging.basicConfig(
            level=level,
            format="%(message)s"  # Clean output without timestamps/logger names
        )


def main():
    parser = argparse.ArgumentParser(
        description=f"Extract and Load Data Tool (v{__version__})"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="config.yaml",
        help="Path to the configuration file. Defaults to 'config.yaml'.",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the version of the tool.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing even if some datasets fail.",
    )
    parser.add_argument(
        "--skip-datasets",
        type=str,
        nargs="+",
        help="Skip specified datasets during processing.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate the configuration without executing pipelines.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform validation and show execution plan without running pipelines.",
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Use TUI (Text User Interface) mode for enhanced visual output.",
    )

    args = parser.parse_args()

    # Initialize TUI if requested
    tui_logger = None
    if args.tui:
        tui_logger = TUILogger()

    _setup_logging(args, tui_logger)

    logger.debug(f"Parsed arguments: {args} ")

    config_file_path = args.config
    
    if args.tui and tui_logger:
        # Run in TUI mode
        run_with_tui(
            tui_logger,
            config_file_path, 
            args.continue_on_error, 
            args.skip_datasets or [], 
            args.validate_only, 
            args.dry_run
        )
    else:
        # Run in standard mode  
        run(
            config_file_path, 
            args.continue_on_error, 
            args.skip_datasets or [], 
            args.validate_only, 
            args.dry_run
        )


def run_with_tui(
    tui_logger: TUILogger,
    config_file_path: str,
    continue_on_error: bool = False,
    skip_datasets: Optional[list[str]] = None,
    validate_only: bool = False,
    dry_run: bool = False,
):
    """Run the ETL process with TUI interface."""
    try:
        # Start TUI in background thread
        import threading
        tui_thread = threading.Thread(target=tui_logger.start, daemon=True)
        tui_thread.start()
        
        # Run the standard process but with TUI updates
        run(config_file_path, continue_on_error, skip_datasets, validate_only, dry_run, tui_logger)
        
    finally:
        tui_logger.stop()


def run(
    config_file_path: str,
    continue_on_error: bool = False,
    skip_datasets: Optional[list[str]] = None,
    validate_only: bool = False,
    dry_run: bool = False,
    tui_logger: Optional[TUILogger] = None,
):
    if skip_datasets is None:
        skip_datasets = []

    state.load_state()
    config = Config.read_config(config_file_path)
    
    # Perform pre-flight validation
    if not tui_logger:
        logger.warning("Performing pre-flight validation...")
    validator = PipelineValidator()
    validation_report = validator.validate_configuration(config)
    
    # Print validation report (only in standard mode)
    if not tui_logger:
        print(format_validation_report(validation_report))
    
    # Handle validation-only mode
    if validate_only:
        if not tui_logger:
            logger.warning("Validation complete. Exiting (--validate-only mode).")
        sys.exit(0 if validation_report.overall_valid else 1)
    
    # Check if validation failed
    if not validation_report.overall_valid:
        logger.error("Configuration validation failed. Cannot proceed with execution.")
        if not tui_logger:
            logger.error("Use --validate-only for detailed validation report.")
        sys.exit(1)
    
    # Handle dry-run mode
    if dry_run:
        if tui_logger:
            tui_logger.add_log_entry("INFO", "=== DRY RUN MODE ===")
        else:
            logger.warning("=== DRY RUN MODE ===")
            logger.warning("Validation passed. Execution plan:")
            for pipeline in config.pipelines:
                logger.warning(f"Pipeline: {pipeline.name}")
                if isinstance(pipeline.source, DatabaseConfig):
                    logger.warning(f"  Source: {pipeline.source.type} ({len(pipeline.source.tables)} tables)")
                    for table in pipeline.source.tables:
                        logger.warning(f"    - Table: {table.name} (strategy: {table.strategy.value})")
                else:  # FileConfig
                    logger.warning(f"  Source: {pipeline.source.type} ({len(pipeline.source.files)} files)")
                    for file_item in pipeline.source.files:
                        file_name = file_item.file_path or file_item.http_path or "unknown"
                        logger.warning(f"    - File: {file_name} (strategy: {file_item.strategy.value})")
                logger.warning(f"  Destination: {pipeline.destination.type}")
                logger.warning(f"  Workers: {pipeline.workers or DEFAULT_WORKER_COUNT}")
            logger.warning("Dry run complete. Exiting (--dry-run mode).")
        sys.exit(0)

    if not config.pipelines:
        logger.error("No pipelines specified in the configuration.")
        sys.exit(1)

    # Initialize error tracker
    error_tracker = ErrorTracker()

    # Initialize TUI with pipeline info
    if tui_logger:
        for pipeline in config.pipelines:
            pipeline_datasets: list[TableConfig | FileItemConfig] = []
            if hasattr(pipeline.source, "tables"):
                pipeline_datasets = getattr(pipeline.source, "tables", [])
            elif hasattr(pipeline.source, "files"):
                pipeline_datasets = getattr(pipeline.source, "files", [])
            
            worker_count = pipeline.workers or config.processing.workers or DEFAULT_WORKER_COUNT
            tui_logger.add_pipeline(pipeline.name, len(pipeline_datasets), worker_count)
            
            for dataset in pipeline_datasets:
                tui_logger.add_dataset(pipeline.name, dataset.name)
    
    # Log configuration options (only in standard mode)
    if not tui_logger:
        if continue_on_error:
            logger.warning("Running in continue-on-error mode")
        if skip_datasets:
            logger.warning(f"Skipping datasets: {', '.join(skip_datasets)}")

    # Track overall statistics
    total_pipelines = len(config.pipelines)
    successful_pipelines = 0
    total_datasets = 0
    successful_datasets = 0

    # Process pipelines sequentially
    for pipeline in config.pipelines:
        if tui_logger:
            tui_logger.start_pipeline(pipeline.name)
        else:
            logger.warning(f"Processing pipeline: {pipeline.name}")
        pipeline_start = time.time()
        pipeline_success = True

        # Get worker count (pipeline-specific or global default)
        worker_count = (
            pipeline.workers or config.processing.workers or DEFAULT_WORKER_COUNT
        )

        # Get tables/datasets from the source configuration
        datasets: list[TableConfig | FileItemConfig] = []
        if hasattr(pipeline.source, "tables"):
            datasets = getattr(pipeline.source, "tables", [])
        elif hasattr(pipeline.source, "files"):
            datasets = getattr(pipeline.source, "files", [])

        if not datasets:
            error_msg = f"No datasets (tables or files) found in pipeline '{pipeline.name}'"
            logger.error(error_msg)
            if tui_logger:
                tui_logger.add_log_entry("ERROR", error_msg)
                tui_logger.complete_pipeline(pipeline.name, False, "No datasets found in configuration")
            error_tracker.track_error(
                pipeline=pipeline.name,
                dataset="N/A",
                operation="pipeline_setup",
                exception=Exception("No datasets found in pipeline configuration"),
                duration_seconds=time.time() - pipeline_start,
            )
            continue

        if not tui_logger:
            logger.warning(f"Found {len(datasets)} datasets to process in pipeline '{pipeline.name}'")
        total_datasets += len(datasets)

        # Track datasets for this pipeline
        pipeline_dataset_success = 0

        # Filter out skipped datasets
        datasets_to_process = []
        for dataset in datasets:
            if dataset.name in skip_datasets:
                skip_msg = f"Skipping dataset '{dataset.name}' as requested"
                if tui_logger:
                    tui_logger.add_log_entry("INFO", skip_msg)
                else:
                    logger.warning(skip_msg)
            else:
                datasets_to_process.append(dataset)

        if not datasets_to_process:
            skip_all_msg = f"All datasets in pipeline '{pipeline.name}' were skipped"
            if tui_logger:
                tui_logger.add_log_entry("INFO", skip_all_msg)
                tui_logger.complete_pipeline(pipeline.name, True)
            else:
                logger.warning(skip_all_msg)
            continue

        # Process datasets in parallel within the pipeline
        if tui_logger:
            tui_logger.update_workers(worker_count)
            
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    process_table,
                    pipeline.source,
                    pipeline.destination,
                    dataset,
                    pipeline.name,
                    error_tracker,
                    tui_logger,
                ): dataset
                for dataset in datasets_to_process
            }
            for future in as_completed(futures):
                dataset = futures[future]
                try:
                    success = future.result()
                    if success:
                        pipeline_dataset_success += 1
                        successful_datasets += 1
                        if not tui_logger:  # TUI already shows this
                            logger.warning(
                                f"Completed processing dataset '{dataset.name}' in pipeline '{pipeline.name}'"
                            )
                    else:
                        pipeline_success = False
                except Exception as e:
                    pipeline_success = False
                    error_msg = f"Error processing dataset '{dataset.name}' in pipeline '{pipeline.name}': {e}"
                    logger.error(error_msg)
                    if tui_logger:
                        tui_logger.add_log_entry("ERROR", error_msg)
                    if not continue_on_error:
                        logger.error(
                            "Stopping execution due to error (use --continue-on-error to proceed)"
                        )
                        # Finalize report and exit
                        error_tracker.finalize_report(
                            total_pipelines=total_pipelines,
                            successful_pipelines=successful_pipelines,
                            total_datasets=total_datasets,
                            successful_datasets=successful_datasets,
                        )
                        logger.info("\n" + error_tracker.report.get_summary())
                        if error_tracker.report.errors:
                            error_report_path = Path("extral_error_report.json")
                            error_tracker.report.save_to_file(error_report_path)
                            logger.info(f"Error report saved to: {error_report_path}")
                        sys.exit(1)

        # Update TUI worker count and pipeline completion
        if tui_logger:
            tui_logger.update_workers(0)
            # Determine pipeline error message if failed
            pipeline_error = None
            if not (pipeline_success and pipeline_dataset_success == len(datasets_to_process)):
                failed_count = len(datasets_to_process) - pipeline_dataset_success
                pipeline_error = f"{failed_count} dataset(s) failed"
            tui_logger.complete_pipeline(pipeline.name, pipeline_success and pipeline_dataset_success == len(datasets_to_process), pipeline_error)
        
        if pipeline_success and pipeline_dataset_success == len(datasets_to_process):
            successful_pipelines += 1
            if not tui_logger:
                logger.warning(f"Successfully completed pipeline: {pipeline.name}")
        else:
            if not tui_logger:
                logger.warning(
                    f"Pipeline '{pipeline.name}' completed with errors. "
                    f"Successful datasets: {pipeline_dataset_success}/{len(datasets_to_process)}"
                )

    # Finalize error report
    error_tracker.finalize_report(
        total_pipelines=total_pipelines,
        successful_pipelines=successful_pipelines,
        total_datasets=total_datasets,
        successful_datasets=successful_datasets,
    )

    # Display error summary
    summary = error_tracker.report.get_summary()
    if tui_logger:
        tui_logger.add_log_entry("INFO", "Execution completed")
        tui_logger.add_log_entry("INFO", summary.replace("\n", " "))
    else:
        logger.warning("\n" + summary)

    # Save error report if there were errors
    if error_tracker.report.errors:
        error_report_path = Path("extral_error_report.json")
        error_tracker.report.save_to_file(error_report_path)
        report_msg = f"Error report saved to: {error_report_path}"
        if tui_logger:
            tui_logger.add_log_entry("INFO", report_msg)
        else:
            logger.warning(report_msg)

    # Store state
    state.store_state()

    # Exit with error code if there were failures
    if (
        error_tracker.report.failed_pipelines > 0
        or error_tracker.report.failed_datasets > 0
    ):
        sys.exit(1)


if __name__ == "__main__":
    main()
