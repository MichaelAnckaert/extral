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
from typing import Literal, Optional, TypedDict

import yaml

logger = logging.getLogger(__name__)

LoggingConfig = TypedDict(
    "LoggingConfig",
    {
        "level": str,
    },
)

ProcessingConfig = TypedDict(
    "ProcessingConfig",
    {
        "workers": int,
    },
)

DatabaseConfig = TypedDict(
    "DatabaseConfig",
    {
        "type": str,
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
        "schema": Optional[str],  # Optional schema for PostgreSQL
        "charset": str,
    },
)

IncrementalConfig = TypedDict(
    "IncrementalConfig",
    {
        "field": str,
        "type": str,
        "initial_value": Optional[str],
    },
)

ReplaceConfig = TypedDict(
    "ReplaceConfig",
    {
        "how": Optional[Literal["truncate", "recreate"]],
    },
)

TableConfig = TypedDict(
    "TableConfig",
    {
        "name": str,
        "strategy": Optional[Literal["replace", "merge", "append"]],
        "merge_key": Optional[str],
        "batch_size": Optional[int],
        "incremental": Optional[IncrementalConfig],
        "replace": Optional[ReplaceConfig],
    },
)

TableListConfig = TypedDict(
    "TableListConfig",
    {
        "tables": list[TableConfig],
    },
)

Config = TypedDict(
    "Config",
    {
        "logging": list[LoggingConfig],
        "processing": list[ProcessingConfig],
        "source": list[DatabaseConfig],
        "destination": list[DatabaseConfig],
        "tables": list[TableConfig],
    },
)


def _read_yaml_config(file_path: str) -> Config:
    """Read YAML configuration file."""
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def read_config(path: str) -> Config:
    """Read configuration from config.yaml."""
    config = _read_yaml_config(path)
    return config


def get_logging_config(path: str) -> LoggingConfig:
    """Get logging configuration from the config."""
    config = read_config(path)
    logging_config = config.get("logging")
    if not logging_config:
        return LoggingConfig(level="INFO")  # Default logging level
    return logging_config[0]


def get_source_config(path: str) -> DatabaseConfig:
    """Get source configuration from the config."""
    config = read_config(path)
    db_config = config.get("source")
    if not db_config:
        logger.error("Source configuration not found in config.yaml")
        sys.exit(1)
    return db_config[0]  # Return the first source configuration


def get_tables_config(path: str) -> list[TableConfig]:
    """Get table configuration from the config."""
    config = read_config(path)
    tables_config = config.get("tables")
    if not tables_config:
        logger.error("Table configuration not found in config.yaml")
        sys.exit(1)
    return tables_config  # Return all table configurations


def get_destination_config(path: str) -> DatabaseConfig:
    """Get destination configuration from the config."""
    config = read_config(path)
    db_config = config.get("destination")
    if not db_config:
        logger.error("Destination configuration not found in config.yaml")
        sys.exit(1)
    return db_config[0]  # Return the first destination configuration


def get_processing_config(path: str) -> ProcessingConfig:
    """Get processing configuration from the config."""
    config = read_config(path)
    processing_config = config.get("processing")
    if not processing_config:
        raise KeyError("Processing configuration not found in config file")
    return processing_config[0]
