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
from typing import Optional, cast, Union

from extral.config import (
    TableConfig,
    FileItemConfig,
    DatabaseConfig,
    LoadConfig,
    LoadStrategy,
    ReplaceMethod,
    ConnectorConfig,
)
from extral.connectors.connector import Connector
from extral.connectors.database import PostgreSQLConnector, MySQLConnector
from extral.connectors.file import CSVConnector, JSONConnector
from extral.database import DatabaseTypeTranslator
from extral.exceptions import LoadException, ConnectionException
from extral.schema import DatabaseSchema, TargetDatabaseSchema

logger = logging.getLogger(__name__)

DEFAULT_STRATEGY = "replace"
DEFAULT_REPLACE_STRATEGY = "recreate"


def _create_target_database_schema(
    destination_config: ConnectorConfig, schema: DatabaseSchema
) -> TargetDatabaseSchema:
    destination_type = destination_config.type
    if destination_type not in ["mysql", "postgresql", "file"]:
        logger.error("Unsupported destination type: %s", destination_type)
        raise ValueError(f"Unsupported destination type: {destination_type}")

    # For file destinations, return schema as-is (no translation needed)
    if destination_type == "file":
        return cast(TargetDatabaseSchema, schema)

    translator = DatabaseTypeTranslator()
    source_schema = schema["schema_source"]
    translated_schema = {}
    for column_name, column_info in schema["schema"].items():
        target_type = translator.translate(
            column_info["type"], source_schema, destination_type
        )
        translated_schema[column_name] = {
            "type": target_type,
            "nullable": column_info.get("nullable", False),
        }

    return {"schema_source": destination_type, "schema": translated_schema}


def load_data(
    destination_config: ConnectorConfig,
    dataset_config: TableConfig | FileItemConfig,
    file_path: str,
    schema_path: str,
    pipeline_name: Optional[str] = None,
):
    dataset_name = dataset_config.name

    if hasattr(destination_config, "database"):
        logger.info(
            f"Loading data for dataset '{dataset_name}' from file '{file_path}' to destination '{destination_config.database}'"
        )
    else:
        logger.info(
            f"Loading data for dataset '{dataset_name}' from file '{file_path}' to destination file"
        )

    try:
        with open(schema_path, "r") as schema_file:
            schema = json.load(schema_file)
            target_schema = _create_target_database_schema(destination_config, schema)
    except Exception as e:
        raise LoadException(
            f"Failed to read or process schema file: {str(e)}",
            pipeline=pipeline_name,
            dataset=dataset_name,
            operation="schema_loading",
        ) from e

    # TODO: if incremental, verify database schema and recreate + full load if different!

    destination_type = destination_config.type

    # Get the appropriate connector
    connector: Connector
    try:
        if destination_type == "postgresql":
            if not hasattr(destination_config, "host"):
                raise ValueError("PostgreSQL destination requires DatabaseConfig")
            connector = PostgreSQLConnector()
            connector.connect(cast(DatabaseConfig, destination_config))
        elif destination_type == "mysql":
            if not hasattr(destination_config, "host"):
                raise ValueError("MySQL destination requires DatabaseConfig")
            connector = MySQLConnector()
            connector.connect(cast(DatabaseConfig, destination_config))
        elif destination_type == "file":
            # For file connectors, use the dataset_config which contains file-specific info
            if not isinstance(dataset_config, FileItemConfig):
                raise ValueError("File destination requires FileItemConfig")
            if dataset_config.format == "csv":
                connector = CSVConnector(dataset_config)
            elif dataset_config.format == "json":
                connector = JSONConnector(dataset_config)
            else:
                raise ValueError(f"Unsupported file format: {dataset_config.format}")
        else:
            logger.error(f"Unsupported destination type: {destination_type}")
            raise ValueError(f"Unsupported destination type: {destination_type}")
    except ConnectionException:
        # Re-raise connection exceptions with enhanced context
        raise
    except Exception as e:
        raise LoadException(
            f"Failed to establish connection to destination: {str(e)}",
            pipeline=pipeline_name,
            dataset=dataset_name,
            operation="connection",
        ) from e

    try:
        # Create LoadConfig from dataset_config
        load_config = LoadConfig(
            strategy=dataset_config.strategy,
            replace_method=dataset_config.replace.how
            if hasattr(dataset_config, "replace") and dataset_config.replace
            else ReplaceMethod.RECREATE,
            merge_key=getattr(dataset_config, "merge_key", None),
            batch_size=getattr(dataset_config, "batch_size", None),
        )

        # Handle table creation/truncation for replace strategy (only for database connectors)
        if (
            destination_type in ["mysql", "postgresql"]
            and load_config.strategy == LoadStrategy.REPLACE
        ):
            # For database connectors, handle replace strategy
            db_connector = cast(Union[PostgreSQLConnector, MySQLConnector], connector)
            if load_config.replace_method == ReplaceMethod.RECREATE:
                # Recreate the table, dropping it first
                db_connector.create_table(dataset_name, target_schema)
            elif load_config.replace_method == ReplaceMethod.TRUNCATE:
                # Only truncate the table, keeping the structure
                db_connector.truncate_table(dataset_name)
            else:
                logger.error(
                    f"Unsupported replace method '{load_config.replace_method.value}' for dataset '{dataset_name}'"
                )
                raise LoadException(
                    f"Unsupported replace method: {load_config.replace_method.value}",
                    pipeline=pipeline_name,
                    dataset=dataset_name,
                    operation="table_preparation",
                )

        # Read data from file
        try:
            with open(file_path, "rb") as file:
                import extral.store as store

                data_bytes = store.decompress_data(file.read())
                data_str = data_bytes.decode("utf-8")
                data = json.loads(data_str)
        except Exception as e:
            raise LoadException(
                f"Failed to read or decompress data file: {str(e)}",
                pipeline=pipeline_name,
                dataset=dataset_name,
                operation="data_reading",
            ) from e

        # Use the new load_data method
        try:
            connector.load_data(dataset_name, data, load_config)
        except Exception as e:
            raise LoadException(
                f"Failed to load data to destination: {str(e)}",
                pipeline=pipeline_name,
                dataset=dataset_name,
                operation="data_loading",
                details={"records_count": len(data)},
            ) from e

    finally:
        # Only disconnect database connectors
        if destination_type in ["mysql", "postgresql"]:
            db_connector = cast(Union[PostgreSQLConnector, MySQLConnector], connector)
            db_connector.disconnect()
