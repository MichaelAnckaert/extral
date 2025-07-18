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
from concurrent.futures import ThreadPoolExecutor, as_completed

from extral import __version__
from extral.config import Config, ConnectorConfig, TableConfig, FileItemConfig
from extral.extract import extract_table
from extral.load import load_data
from extral.state import state

import argparse

logger = logging.getLogger(__name__)

DEFAULT_WORKER_COUNT = 4


def process_table(
    source_config: ConnectorConfig,
    destination_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    pipeline_name: str,
):
    try:
        logger.info(f"Processing dataset: {dataset_config.name}")
        file_path, schema_path = extract_table(source_config, dataset_config, pipeline_name)
        if file_path is None or schema_path is None:
            logger.info(
                f"Skipping dataset load for '{dataset_config.name}' as there is no data extracted."
            )
            return
        load_data(destination_config, dataset_config, file_path, schema_path)
    except Exception as e:
        logger.error(f"Error processing dataset '{dataset_config.name}': {e}")


def _setup_logging(args: argparse.Namespace):
    config = Config.read_config(args.config)
    logging_config = config.logging
    
    if logging_config.level == "debug":
        level = logging.DEBUG
    elif logging_config.level == "info":
        level = logging.INFO
    elif logging_config.level == "warning":
        level = logging.WARNING
    elif logging_config.level == "error":
        level = logging.ERROR
    elif logging_config.level == "critical":
        level = logging.CRITICAL
    else:
        logger.warning(
            f"Unknown logging level '{logging_config.level}', defaulting to INFO."
        )
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(description=f"Extract and Load Data Tool (v{__version__})")
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
    
    args = parser.parse_args()

    _setup_logging(args)

    logger.debug(f"Parsed arguments: {args} ")

    config_file_path = args.config
    run(config_file_path)


def run(config_file_path: str):
    state.load_state()
    config = Config.read_config(config_file_path)
    
    if not config.pipelines:
        logger.error("No pipelines specified in the configuration.")
        sys.exit(1)

    # Process pipelines sequentially
    for pipeline in config.pipelines:
        logger.info(f"Processing pipeline: {pipeline.name}")
        
        # Get worker count (pipeline-specific or global default)
        worker_count = pipeline.workers or config.processing.workers or DEFAULT_WORKER_COUNT
        
        # Get tables/datasets from the source configuration
        if hasattr(pipeline.source, 'tables') and pipeline.source.tables:
            datasets = pipeline.source.tables
        elif hasattr(pipeline.source, 'files') and pipeline.source.files:
            datasets = pipeline.source.files
        else:
            logger.error(f"No datasets (tables or files) found in pipeline '{pipeline.name}'")
            continue
        
        logger.info(f"Found {len(datasets)} datasets to process in pipeline '{pipeline.name}'")
        
        # Process datasets in parallel within the pipeline
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    process_table, pipeline.source, pipeline.destination, dataset, pipeline.name
                ): dataset
                for dataset in datasets
            }
            for future in as_completed(futures):
                dataset = futures[future]
                try:
                    future.result()
                    logger.info(f"Completed processing dataset '{dataset.name}' in pipeline '{pipeline.name}'")
                except Exception as e:
                    logger.error(f"Error processing dataset '{dataset.name}' in pipeline '{pipeline.name}': {e}")
        
        logger.info(f"Completed pipeline: {pipeline.name}")

    state.store_state()


if __name__ == "__main__":
    main()
