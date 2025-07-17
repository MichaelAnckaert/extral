# Copyright 2025 Sinax GCV
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

from extral import config, __version__
from extral.extract import extract_table
from extral.load import load_data
from extral.state import state

import argparse

logger = logging.getLogger(__name__)


def process_table(
    source_config: config.DatabaseConfig,
    destination_config: config.DatabaseConfig,
    table_config: config.TableConfig,
):
    try:
        logger.info(f"Processing table: {table_config['name']}")
        file_path, schema_path = extract_table(source_config, table_config)
        if file_path is None or schema_path is None:
            logger.info(
                f"Skipping table load for '{table_config['name']}' as there is no data extracted."
            )
            return
        load_data(destination_config, table_config, file_path, schema_path)
    except Exception as e:
        logger.error(f"Error processing table '{table_config['name']}': {e}")


def _setup_logging(args: argparse.Namespace):
    logging_config = config.get_logging_config(args.config)
    if logging_config["level"] == "debug":
        level = logging.DEBUG
    elif logging_config["level"] == "info":
        level = logging.INFO
    elif logging_config["level"] == "warning":
        level = logging.WARNING
    elif logging_config["level"] == "error":
        level = logging.ERROR
    elif logging_config["level"] == "critical":
        level = logging.CRITICAL
    else:
        logger.warning(
            f"Unknown logging level '{logging_config['level']}', defaulting to INFO."
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
    source_config = config.get_source_config(config_file_path)
    destination_config = config.get_destination_config(config_file_path)
    tables_config = config.get_tables_config(config_file_path)

    if not tables_config:
        logger.error("No tables specified in the configuration.")
        sys.exit(1)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                process_table, source_config, destination_config, table
            ): table
            for table in tables_config
        }
        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result()
                logger.info(f"Completed processing table '{table}'")
            except Exception as e:
                logger.error(f"Error processing table '{table}': {e}")

    state.store_state()


if __name__ == "__main__":
    main()
