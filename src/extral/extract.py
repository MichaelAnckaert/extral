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
from typing import Generator, Optional, Tuple

from extral import encoder
from extral.config import ExtractConfig, IncrementalConfig, TableConfig, ConnectorConfig
from extral.connectors import MySQLConnector, PostgreSQLConnector
from extral.connectors.file import CSVConnector, JSONConnector
from extral.database import DatabaseRecord
from extral.schema import (
    SchemaCreateException,
    TargetDatabaseSchema,
    infer_schema,
)
from extral.state import state
from extral.store import compress_data, store_data

logger = logging.getLogger(__name__)


def extract_schema_from_source(
    source_config: ConnectorConfig, table_name: str
) -> Optional[TargetDatabaseSchema]:
    """Determine the schema for a given table."""
    source_type = source_config.type

    if source_type == "mysql":
        mysql_connector = MySQLConnector()
        mysql_connector.connect(source_config)
        schema = mysql_connector.extract_schema_for_table(table_name)
        mysql_connector.disconnect()
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from MySQL source"
            )
    elif source_type == "postgresql":
        postgresql_connector = PostgreSQLConnector()
        postgresql_connector.connect(source_config)
        schema = postgresql_connector.extract_schema_for_table(table_name)
        postgresql_connector.disconnect()
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from PostgreSQL source"
            )
    elif source_type == "file":
        # For file connectors, we need to use the infer_schema method
        file_config = source_config  # type: ignore
        if file_config.format == "csv":
            connector = CSVConnector(file_config)
        elif file_config.format == "json":
            connector = JSONConnector(file_config)
        else:
            raise ValueError(f"Unsupported file format: {file_config.format}")
        
        # Infer schema from file
        schema_dict = connector.infer_schema(table_name)
        
        # Convert to TargetDatabaseSchema format
        inferred_schema: TargetDatabaseSchema = {
            "schema_source": f"file_{file_config.format}",
            "schema": schema_dict
        }
        return inferred_schema
    else:
        logger.error(f"Unsupported source type: {source_type}")
        raise ValueError(f"Unsupported source type: {source_type}")

    db_inferred_schema: TargetDatabaseSchema = {"schema_source": source_type, "schema": {}}
    for column in schema:
        column_name = column["Field"]
        column_type = column["Type"]
        is_nullable = column["Null"] == "YES"
        db_inferred_schema["schema"][column_name] = {
            "type": column_type,
            "nullable": is_nullable,
        }

    return db_inferred_schema


def _extract_data(
    source_config: ConnectorConfig,
    table_config: TableConfig,
    incremental: Optional[IncrementalConfig],
) -> Generator[list[DatabaseRecord], None, None]:
    source_type = source_config.type
    table_name = table_config.name

    extract_config = ExtractConfig(
        extract_type="FULL",
        batch_size=table_config.batch_size
    )
    
    if incremental:
        logger.debug(
            f"Incremental extraction configured for table '{table_name}' with field '{incremental.field}'"
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

        if table_name in state.tables:
            state_incremental = state.tables[table_name].get("incremental", {})
            if state_incremental:
                if state_incremental.get("field") != incremental_field:
                    logger.warning(
                        f"Incremental field mismatch for table '{table_name}': expected '{incremental_field}', found '{state_incremental.get('field')}'. Will proceed with full extraction."
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

    if source_type == "mysql":
        connector = MySQLConnector()
        connector.connect(source_config)
        return connector.extract_data(table_name, extract_config)
    elif source_type == "postgresql":
        connector = PostgreSQLConnector()
        connector.connect(source_config)
        return connector.extract_data(table_name, extract_config)
    elif source_type == "file":
        # For file connectors
        file_config = source_config  # type: ignore
        if file_config.format == "csv":
            connector = CSVConnector(file_config)
        elif file_config.format == "json":
            connector = JSONConnector(file_config)
        else:
            raise ValueError(f"Unsupported file format: {file_config.format}")
        
        return connector.extract_data(table_name, extract_config)
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
    source_config: ConnectorConfig, table_config: TableConfig
) -> Tuple[Optional[str], Optional[str]]:
    table_name = table_config.name
    incremental = table_config.incremental

    try:
        logger.info("Starting extraction from table: %s", table_name)
        datagen = _extract_data(source_config, table_config, incremental)

        full_data: list[DatabaseRecord] = []
        for data in datagen:
            if not data:
                logger.info("No data extracted from table '%s'", table_name)
                continue

            logger.debug(
                f"Batch: Extracted {len(data):,} records from table '{table_name}'"
            )
            full_data.extend(data)

        logger.info(
            f"Total records extracted from table '{table_name}': {len(full_data):,}"
        )

        if len(full_data) == 0:
            # No data extracted, return None and skip further processing
            return None, None

        # Handle incremental config
        if incremental:
            incremental_field = incremental.field
            last_value = full_data[-1][incremental_field] if full_data else None

            state.tables[table_name] = {
                "incremental": {
                    "field": incremental_field,
                    "last_value": last_value,
                }
            }
            logger.debug(
                f"Updated state for incremental extraction: {state.tables[table_name]}"
            )

        schema = extract_schema_from_source(source_config, table_name)
        if not schema:
            logger.error(
                f"No schema could be extracted for table '{table_name}' from source '{source_config['type']}'. Inferring schema from data."
            )
            schema = _infer_schema_from_data(full_data)

        schema_path = f"output/schema_{table_name}.json"
        logger.info(f"Storing schema to file: {schema_path}")
        with open(schema_path, "w") as file:
            json.dump(schema, file)

        file_path = f"output/compressed_{table_name}.jsonl.gz"
        logger.info(f"Storing data for table '{table_name}' in file: {file_path}")
        data_bytes = encoder.encode_data(full_data)
        compressed_data = compress_data(data_bytes)
        store_data(compressed_data, file_path)

        logger.info(f"Extraction finished for table '{table_name}'")
        return file_path, schema_path
    except Exception as e:
        logger.error("Error processing table '%s': %s", table_name, e)
        raise e
