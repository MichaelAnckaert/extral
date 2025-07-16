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
import json
import logging
from typing import Generator, Optional, Tuple

import encoder
from config import DatabaseConfig, IncrementalConfig, TableConfig
from connectors import DatabaseInterface, mysql, postgresql
from database import DatabaseRecord
from schema import (
    SchemaCreateException,
    TargetDatabaseSchema,
    infer_schema,
)
from state import state
from store import compress_data, store_data

logger = logging.getLogger(__name__)


def extract_schema_from_source(
    source_config: DatabaseConfig, table_name: str
) -> Optional[TargetDatabaseSchema]:
    """Determine the schema for a given table."""
    source_type = source_config.get("type")

    if source_type == "mysql":
        mysql_connector = mysql.MySQLConnector()
        mysql_connector.connect(source_config)
        schema = mysql_connector.extract_schema_for_table(table_name)
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from MySQL source"
            )
    elif source_type == "postgresql":
        postgresql_connector = postgresql.PostgresqlConnector()
        postgresql_connector.connect(source_config)
        schema = postgresql_connector.extract_schema_for_table(table_name)
        if not schema:
            raise SchemaCreateException(
                f"Could not extract schema for table '{table_name}' from PostgreSQL source"
            )
    else:
        logger.error(f"Unsupported source type: {source_type}")
        raise ValueError(f"Unsupported source type: {source_type}")

    inferred_schema: TargetDatabaseSchema = {"schema_source": source_type, "schema": {}}
    for column in schema:
        column_name = column["Field"]
        column_type = column["Type"]
        is_nullable = column["Null"] == "YES"
        inferred_schema["schema"][column_name] = {
            "type": column_type,
            "nullable": is_nullable,
        }

    return inferred_schema


def _extract_data(
    source_config: DatabaseConfig,
    table_config: TableConfig,
    incremental: Optional[IncrementalConfig],
) -> Generator[list[DatabaseRecord], None, None]:
    source_type = source_config.get("type")
    table_name = table_config["name"]

    extract_config: dict[str, Optional[str | int]] = {
        "extract_type": "FULL",
        "last_value": None,
        "incremental_field": None,
    }
    if incremental:
        logger.debug(
            f"Incremental extraction configured for table '{table_name}' with field '{incremental.get('field')}'"
        )
        incremental_field = incremental.get("field")
        incremental_type = incremental.get("type")
        initial_value: str | int | None = incremental.get("initial_value", None)

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
                    if incremental_type not in [
                        "int",
                    ]:
                        extract_config["last_value"] = f"'{last_value}'"
                    else:
                        extract_config["last_value"] = last_value
                    extract_config["extract_type"] = "INCREMENTAL"
                    extract_config["incremental_field"] = incremental_field
            else:
                # No previous state, use initial value
                last_value = initial_value
                if incremental_type not in [
                    "int",
                ]:
                    extract_config["last_value"] = f"'{last_value}'"
                else:
                    extract_config["last_value"] = last_value
                extract_config["extract_type"] = "INCREMENTAL"
                extract_config["incremental_field"] = incremental_field
        else:
            # No previous state, use initial value
            last_value = initial_value
            if incremental_type not in [
                "int",
            ]:
                extract_config["last_value"] = f"'{last_value}'"
            else:
                extract_config["last_value"] = last_value
            extract_config["extract_type"] = "INCREMENTAL"
            extract_config["incremental_field"] = incremental_field

    connector: DatabaseInterface
    if source_type == "mysql":
        connector = mysql.MySQLConnector()
        connector.connect(source_config)
        return connector.extract_data(table_config, extract_config)
    elif source_type == "postgresql":
        connector = postgresql.PostgresqlConnector()
        connector.connect(source_config)
        return connector.extract_data(table_config, extract_config)
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
    source_config: DatabaseConfig, table_config: TableConfig
) -> Tuple[Optional[str], Optional[str]]:
    table_name = table_config["name"]
    incremental = table_config.get("incremental")

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
            incremental_field = incremental["field"]
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
