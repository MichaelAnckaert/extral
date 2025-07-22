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
from typing import Optional

from extral import __version__
from extral.config import Config, ConnectorConfig, TableConfig, FileItemConfig, DatabaseConfig
from extral.context import ApplicationContext
from extral.extract import extract_table
from extral.load import load_data
from extral.state import state
from extral.validation import PipelineValidator, format_validation_report

import argparse

logger = logging.getLogger(__name__)

DEFAULT_WORKER_COUNT = 4

# Global shutdown flag
shutdown_requested = False


def request_shutdown():
    """Request application shutdown."""
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown requested - stopping processing...")
    logger.info("Setting shutdown flag - workers should exit soon")


def is_shutdown_requested():
    """Check if shutdown has been requested."""
    return shutdown_requested


def process_table(
    source_config: ConnectorConfig,
    destination_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    pipeline_name: str,
    context: ApplicationContext,
) -> bool:
    """Process a single table/dataset. Returns True if successful, False otherwise."""
    start_time = time.time()
    try:
        # Check for shutdown before starting
        if is_shutdown_requested():
            logger.info(f"Dataset {dataset_config.name} - shutdown requested, skipping")
            context.dataset_skipped(pipeline_name, dataset_config.name, "Shutdown requested")
            return True
            
        context.dataset_started(pipeline_name, dataset_config.name)

        # Extract phase
        try:
            # Check for shutdown before extracting
            if is_shutdown_requested():
                context.dataset_skipped(pipeline_name, dataset_config.name, "Shutdown requested")
                return True
                
            file_path, schema_path = extract_table(
                source_config, dataset_config, pipeline_name, is_shutdown_requested
            )
            if file_path is None or schema_path is None:
                context.dataset_skipped(
                    pipeline_name, 
                    dataset_config.name, 
                    "No data extracted"
                )
                return True
        except Exception as e:
            duration = time.time() - start_time
            context.error_occurred(
                pipeline=pipeline_name,
                dataset=dataset_config.name,
                operation="extract",
                error=e,
                duration_seconds=duration,
                include_stack_trace=True,
            )
            context.dataset_completed(
                pipeline_name, 
                dataset_config.name, 
                success=False, 
                error=e, 
                duration_seconds=duration
            )
            raise

        # Load phase
        try:
            # Check for shutdown before loading
            if is_shutdown_requested():
                context.dataset_skipped(pipeline_name, dataset_config.name, "Shutdown requested")
                return True
                
            load_data(
                destination_config,
                dataset_config,
                file_path,
                schema_path,
                pipeline_name,
                is_shutdown_requested,
            )
        except Exception as e:
            duration = time.time() - start_time
            context.error_occurred(
                pipeline=pipeline_name,
                dataset=dataset_config.name,
                operation="load",
                error=e,
                duration_seconds=duration,
                include_stack_trace=True,
            )
            context.dataset_completed(
                pipeline_name, 
                dataset_config.name, 
                success=False, 
                error=e, 
                duration_seconds=duration
            )
            raise

        context.dataset_completed(
            pipeline_name, 
            dataset_config.name, 
            success=True, 
            duration_seconds=time.time() - start_time
        )
        return True

    except Exception as e:
        context.log_message("ERROR", f"Error processing dataset '{dataset_config.name}': {e}")
        return False


def _setup_basic_logging():
    """Setup minimal logging for startup errors."""
    logging.basicConfig(
        level=logging.ERROR,
        format="%(levelname)s: %(message)s"
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

    _setup_basic_logging()

    config_file_path = args.config
    run(
        config_file_path, 
        args.continue_on_error, 
        args.skip_datasets or [], 
        args.validate_only, 
        args.dry_run,
        args.tui
    )


def run(
    config_file_path: str,
    continue_on_error: bool = False,
    skip_datasets: Optional[list[str]] = None,
    validate_only: bool = False,
    dry_run: bool = False,
    use_tui: bool = False,
):
    if skip_datasets is None:
        skip_datasets = []

    state.load_state()
    config = Config.read_config(config_file_path)
    
    # Initialize context
    context = ApplicationContext(config, use_tui=use_tui)
    
    try:
        # Start context (this starts event processing)
        context.start()
        
        # Set up TUI shutdown callback if using TUI
        if context.tui_handler:
            context.tui_handler.set_shutdown_callback(request_shutdown)
        
        # Perform pre-flight validation
        context.log_message("INFO", "Performing pre-flight validation...")
        validator = PipelineValidator()
        validation_report = validator.validate_configuration(config)
        
        # Print validation report (only in console mode)
        if not use_tui:
            print(format_validation_report(validation_report))
        
        # Handle validation-only mode
        if validate_only:
            context.log_message("INFO", "Validation complete. Exiting (--validate-only mode).")
            sys.exit(0 if validation_report.overall_valid else 1)
    
        # Check if validation failed
        if not validation_report.overall_valid:
            context.log_message("ERROR", "Configuration validation failed. Cannot proceed with execution.")
            if not use_tui:
                context.log_message("ERROR", "Use --validate-only for detailed validation report.")
            sys.exit(1)
    
        # Handle dry-run mode
        if dry_run:
            context.log_message("INFO", "=== DRY RUN MODE ===")
            if not use_tui:
                print("Validation passed. Execution plan:")
                for pipeline in config.pipelines:
                    print(f"Pipeline: {pipeline.name}")
                    if isinstance(pipeline.source, DatabaseConfig):
                        print(f"  Source: {pipeline.source.type} ({len(pipeline.source.tables)} tables)")
                        for table in pipeline.source.tables:
                            print(f"    - Table: {table.name} (strategy: {table.strategy.value})")
                    else:  # FileConfig
                        print(f"  Source: {pipeline.source.type} ({len(pipeline.source.files)} files)")
                        for file_item in pipeline.source.files:
                            file_name = file_item.file_path or file_item.http_path or "unknown"
                            print(f"    - File: {file_name} (strategy: {file_item.strategy.value})")
                    print(f"  Destination: {pipeline.destination.type}")
                    print(f"  Workers: {pipeline.workers or DEFAULT_WORKER_COUNT}")
                print("Dry run complete. Exiting (--dry-run mode).")
            sys.exit(0)

        if not config.pipelines:
            context.log_message("ERROR", "No pipelines specified in the configuration.")
            sys.exit(1)

        # Log configuration options
        if continue_on_error:
            context.log_message("INFO", "Running in continue-on-error mode")
        if skip_datasets:
            context.log_message("INFO", f"Skipping datasets: {', '.join(skip_datasets)}")

        # Track overall statistics
        total_pipelines = len(config.pipelines)
        successful_pipelines = 0
        total_datasets = 0
        successful_datasets = 0

        # Process pipelines sequentially
        for pipeline in config.pipelines:
            # Check for shutdown before starting each pipeline
            if is_shutdown_requested():
                context.log_message("INFO", "Processing stopped by user request")
                break
                
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
                
            # Start pipeline
            context.pipeline_started(pipeline.name, len(datasets), worker_count)

            if not datasets:
                context.pipeline_completed(
                    pipeline.name, 
                    success=False, 
                    error_message="No datasets found in configuration"
                )
                continue

            context.log_message("INFO", f"Found {len(datasets)} datasets to process in pipeline '{pipeline.name}'")
            total_datasets += len(datasets)

            # Track datasets for this pipeline
            pipeline_dataset_success = 0

            # Filter out skipped datasets
            datasets_to_process = []
            for dataset in datasets:
                if dataset.name in skip_datasets:
                    context.dataset_skipped(pipeline.name, dataset.name, "Skipped as requested")
                else:
                    datasets_to_process.append(dataset)

            if not datasets_to_process:
                context.pipeline_completed(pipeline.name, success=True)
                continue

            # Process datasets in parallel within the pipeline
            context.update_workers(worker_count)
            
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = {
                    executor.submit(
                        process_table,
                        pipeline.source,
                        pipeline.destination,
                        dataset,
                        pipeline.name,
                        context,
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
                        else:
                            pipeline_success = False
                    except Exception as e:
                        pipeline_success = False
                        context.log_message("ERROR", f"Error processing dataset '{dataset.name}' in pipeline '{pipeline.name}': {e}")
                        if not continue_on_error:
                            context.log_message("ERROR", "Stopping execution due to error (use --continue-on-error to proceed)")
                            # Show final summary
                            print("\n" + context.get_stats_summary())
                            context.save_stats_report("extral_error_report.json")
                            context.log_message("INFO", "Error report saved to: extral_error_report.json")
                            sys.exit(1)
                    
                    # Check for shutdown after each completed task
                    if is_shutdown_requested():
                        context.log_message("INFO", "Shutdown requested - cancelling remaining tasks")
                        
                        # Show waiting message for remaining tasks
                        remaining_tasks = sum(1 for f in futures if not f.done())
                        if remaining_tasks > 0:
                            if use_tui:
                                # Print to stderr to avoid interfering with TUI on stdout
                                print(f"\nShutting down - waiting for {remaining_tasks} remaining task(s) to complete...", file=sys.stderr)
                                print("Tasks are being cancelled gracefully. Press Ctrl+C to force quit.", file=sys.stderr)
                            else:
                                context.log_message("INFO", f"Waiting for {remaining_tasks} remaining task(s) to complete...")
                        
                        # Cancel remaining futures
                        for remaining_future in futures:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break

            # Update worker count and pipeline completion
            context.update_workers(0)
            
            # Complete pipeline
            pipeline_complete_success = pipeline_success and pipeline_dataset_success == len(datasets_to_process)
            pipeline_error = None
            if not pipeline_complete_success:
                failed_count = len(datasets_to_process) - pipeline_dataset_success
                pipeline_error = f"{failed_count} dataset(s) failed"
                
            context.pipeline_completed(
                pipeline.name,
                success=pipeline_complete_success,
                successful_datasets=pipeline_dataset_success,
                failed_datasets=len(datasets_to_process) - pipeline_dataset_success,
                error_message=pipeline_error
            )
            
            if pipeline_complete_success:
                successful_pipelines += 1

        # Display final summary
        summary = context.get_stats_summary()
        context.log_message("INFO", "Execution completed")
        if not use_tui:
            print("\n" + summary)

        # Save detailed report
        context.save_stats_report("extral_execution_report.json")
        context.log_message("INFO", "Detailed report saved to: extral_execution_report.json")

        # Store state
        state.store_state()

        # Exit with error code if there were failures
        failed_pipelines = total_pipelines - successful_pipelines
        failed_datasets = total_datasets - successful_datasets
        if failed_pipelines > 0 or failed_datasets > 0:
            sys.exit(1)
            
    finally:
        # Always stop the context
        context.stop()


if __name__ == "__main__":
    main()
