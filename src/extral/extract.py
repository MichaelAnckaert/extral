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
import json
import logging
from typing import Callable, Generator, Optional, Tuple, cast

from extral import encoder
from extral.config import (
    ExtractConfig,
    TableConfig,
    FileItemConfig,
    FileConfig,
    DatabaseConfig,
    ConnectorConfig,
)
from extral.connectors.connector import Connector
from extral.connectors.database import MySQLConnector, PostgreSQLConnector
from extral.connectors.file import CSVConnector, JSONConnector
from extral.database import DatabaseRecord
from extral.exceptions import ExtractException, ConnectionException
from extral.schema import (
    SchemaCreateException,
    TargetDatabaseSchema,
    infer_schema,
)
from extral.state import state, DatasetState
from extral.store import compress_data, store_data

logger = logging.getLogger(__name__)


def extract_schema_from_source(
    source_config: ConnectorConfig, table_name: str
) -> Optional[TargetDatabaseSchema]:
    """Determine the schema for a given table."""
    source_type = source_config.type

    if source_type == "mysql":
        if not hasattr(source_config, "host"):
            raise ValueError("MySQL source requires DatabaseConfig")
        mysql_connector = MySQLConnector()
        mysql_connector.connect(cast(DatabaseConfig, source_config))
        schema = mysql_connector.extract_schema_for_table(table_name)
        mysql_connector.disconnect()
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from MySQL source"
            )
    elif source_type == "postgresql":
        if not hasattr(source_config, "host"):
            raise ValueError("PostgreSQL source requires DatabaseConfig")
        postgresql_connector = PostgreSQLConnector()
        postgresql_connector.connect(cast(DatabaseConfig, source_config))
        schema = postgresql_connector.extract_schema_for_table(table_name)
        postgresql_connector.disconnect()
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from PostgreSQL source"
            )
    elif source_type in ["csv", "json"]:
        # For file connectors, find the specific file configuration
        if not hasattr(source_config, "files"):
            raise ValueError("File source config must have files attribute")

        file_config = cast(FileConfig, source_config)

        # Find the file item config that matches the table_name
        file_item_config = None
        for file_item in file_config.files:
            if file_item.name == table_name:
                file_item_config = file_item
                break

        if not file_item_config:
            raise ValueError(f"No file configuration found for dataset '{table_name}'")

        # Create connector based on source type
        connector: Connector
        if source_type == "csv":
            connector = CSVConnector(file_item_config)
        elif source_type == "json":
            connector = JSONConnector(file_item_config)
        else:
            raise ValueError(f"Unsupported file format: {source_type}")

        # Infer schema from file
        schema_dict = connector.infer_schema(table_name)

        # Convert to TargetDatabaseSchema format
        inferred_schema: TargetDatabaseSchema = {
            "schema_source": f"file_{source_type}",
            "schema": schema_dict,
        }
        return inferred_schema
    else:
        logger.error(f"Unsupported source type: {source_type}")
        raise ValueError(f"Unsupported source type: {source_type}")

    db_inferred_schema: TargetDatabaseSchema = {
        "schema_source": source_type,
        "schema": {},
    }
    for column in schema:
        column_name = column["Field"]
        column_type = column["type"]  # Changed from "Type" to match new connector
        is_nullable = column.get("nullable", column.get("Null") == "YES")
        db_inferred_schema["schema"][column_name] = {
            "type": column_type,
            "nullable": is_nullable,
        }

    return db_inferred_schema


def _extract_data(
    source_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    extract_config: ExtractConfig,
) -> Generator[list[DatabaseRecord], None, None]:
    source_type = source_config.type
    dataset_name = dataset_config.name

    connector: Connector
    if source_type == "mysql":
        if not hasattr(source_config, "host"):
            raise ValueError("MySQL source requires DatabaseConfig")
        connector = MySQLConnector()
        connector.connect(cast(DatabaseConfig, source_config))
        return connector.extract_data(dataset_name, extract_config)
    elif source_type == "postgresql":
        if not hasattr(source_config, "host"):
            raise ValueError("PostgreSQL source requires DatabaseConfig")
        connector = PostgreSQLConnector()
        connector.connect(cast(DatabaseConfig, source_config))
        return connector.extract_data(dataset_name, extract_config)
    elif source_type in ["csv", "json"]:
        # For file connectors, use the dataset_config which contains file-specific info
        if not isinstance(dataset_config, FileItemConfig):
            raise ValueError("File source requires FileItemConfig")
        if source_type == "csv":
            connector = CSVConnector(dataset_config)
        elif source_type == "json":
            connector = JSONConnector(dataset_config)
        else:
            raise ValueError(f"Unsupported file format: {source_type}")

        return connector.extract_data(dataset_name, extract_config)
    else:
        logger.error(f"Unsupported source type: {source_type}")
        raise ValueError(f"Unsupported source type: {source_type}")


def _infer_schema_from_data(
    full_data: list[dict[str, Optional[str]]],
) -> Optional[TargetDatabaseSchema]:
    """Infer schema from the provided data."""
    offset = 0
    size = 100
    has_schema = False
    schema = None

    while not has_schema:
        sample_records = full_data[offset : offset + size]
        try:
            schema = infer_schema(sample_records)
            logger.info(
                "Schema created after %d iterations: %s",
                (offset // size) + 1,
                schema,
            )
            has_schema = True
        except SchemaCreateException as e:
            offset += size
            if offset >= len(full_data):
                logger.error("Unable to create schema after exhausting all records")
                raise e

    return schema


def extract_table(
    source_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    pipeline_name: str,
    quit_check: Optional[Callable[[], bool]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    dataset_name = dataset_config.name
    incremental = getattr(dataset_config, "incremental", None)

    # Use dataset_id to identify the table (for now, just use dataset_name)
    dataset_id = dataset_name

    try:
        logger.info("Starting extraction from dataset: %s", dataset_name)

        # Set up extraction config
        extract_config = ExtractConfig(
            extract_type="FULL", batch_size=getattr(dataset_config, "batch_size", None)
        )

        # Handle incremental extraction with state management
        if incremental:
            logger.debug(
                f"Incremental extraction configured for dataset '{dataset_name}' with field '{incremental.field}'"
            )
            incremental_field = incremental.field
            incremental_type = incremental.type
            initial_value: str | int | None = incremental.initial_value

            if initial_value is None:
                if incremental_type in ["int", "float"]:
                    initial_value = 0
                elif incremental_type == "datetime":
                    initial_value = "1970-01-01 00:00:00"
                elif incremental_type == "date":
                    initial_value = "1970-01-01"
                else:
                    logger.error(f"Unsupported incremental type: {incremental_type}")
                    initial_value = "''"

            # Check existing state for this dataset in this pipeline
            existing_state = state.get_dataset_state(pipeline_name, dataset_id)

            if existing_state:
                state_incremental = existing_state.get("incremental", {})
                if state_incremental:
                    if state_incremental.get("field") != incremental_field:
                        logger.warning(
                            f"Incremental field mismatch for dataset '{dataset_name}': expected '{incremental_field}', found '{state_incremental.get('field')}'. Will proceed with full extraction."
                        )
                    else:
                        last_value = state_incremental.get("last_value", initial_value)
                        if incremental_type not in ["int"]:
                            extract_config.last_value = f"'{last_value}'"
                        else:
                            extract_config.last_value = last_value
                        extract_config.extract_type = "INCREMENTAL"
                        extract_config.incremental_field = incremental_field
                else:
                    # No previous state, use initial value
                    last_value = initial_value
                    if incremental_type not in ["int"]:
                        extract_config.last_value = f"'{last_value}'"
                    else:
                        extract_config.last_value = last_value
                    extract_config.extract_type = "INCREMENTAL"
                    extract_config.incremental_field = incremental_field
            else:
                # No previous state, use initial value
                last_value = initial_value
                if incremental_type not in ["int"]:
                    extract_config.last_value = f"'{last_value}'"
                else:
                    extract_config.last_value = last_value
                extract_config.extract_type = "INCREMENTAL"
                extract_config.incremental_field = incremental_field

        datagen = _extract_data(source_config, dataset_config, extract_config)

        full_data: list[DatabaseRecord] = []
        for data in datagen:
            # Check for quit signal
            if quit_check and quit_check():
                logger.info("Extract cancelled for dataset '%s' - quit requested", dataset_name)
                return None, None
                
            if not data:
                logger.info("No data extracted from dataset '%s'", dataset_name)
                continue

            logger.debug(
                f"Batch: Extracted {len(data):,} records from dataset '{dataset_name}'"
            )
            full_data.extend(data)

        logger.info(
            f"Total records extracted from dataset '{dataset_name}': {len(full_data):,}"
        )

        if len(full_data) == 0:
            # No data extracted, return None and skip further processing
            return None, None

        # Handle incremental config
        if incremental:
            incremental_field = incremental.field
            last_value = full_data[-1][incremental_field] if full_data else None

            dataset_state: DatasetState = {
                "incremental": {
                    "field": incremental_field,
                    "last_value": last_value,
                }
            }
            state.set_dataset_state(pipeline_name, dataset_id, dataset_state)
            logger.debug(f"Updated state for incremental extraction: {dataset_state}")

        schema = extract_schema_from_source(source_config, dataset_name)
        if not schema:
            logger.error(
                f"No schema could be extracted for dataset '{dataset_name}' from source '{source_config.type}'. Inferring schema from data."
            )
            schema = _infer_schema_from_data(full_data)

        schema_path = f"output/schema_{dataset_name}.json"
        logger.info(f"Storing schema to file: {schema_path}")
        with open(schema_path, "w") as file:
            json.dump(schema, file)

        file_path = f"output/compressed_{dataset_name}.jsonl.gz"
        logger.info(f"Storing data for dataset '{dataset_name}' in file: {file_path}")
        data_bytes = encoder.encode_data(full_data)
        compressed_data = compress_data(data_bytes)
        store_data(compressed_data, file_path)

        logger.info(f"Extraction finished for dataset '{dataset_name}'")
        return file_path, schema_path
    except ConnectionException:
        # Re-raise connection exceptions with enhanced context
        raise
    except SchemaCreateException as e:
        # Convert schema creation exceptions to extract exceptions
        raise ExtractException(
            f"Failed to extract or create schema: {str(e)}",
            pipeline=pipeline_name,
            dataset=dataset_name,
            operation="schema_extraction",
        ) from e
    except Exception as e:
        logger.error("Error processing dataset '%s': %s", dataset_name, e)
        # Wrap generic exceptions with context
        raise ExtractException(
            f"Extraction failed: {str(e)}",
            pipeline=pipeline_name,
            dataset=dataset_name,
            operation="extract",
        ) from e
