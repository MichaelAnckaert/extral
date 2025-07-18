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

from extral.config import TableConfig, FileItemConfig, LoadConfig, LoadStrategy, ReplaceMethod, ConnectorConfig
from extral.connectors import PostgreSQLConnector, MySQLConnector
from extral.connectors.file import CSVConnector, JSONConnector
from extral.database import DatabaseTypeTranslator
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
        return schema

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
):
    dataset_name = dataset_config.name

    if hasattr(destination_config, 'database'):
        logger.info(
            f"Loading data for dataset '{dataset_name}' from file '{file_path}' to destination '{destination_config.database}'"
        )
    else:
        logger.info(
            f"Loading data for dataset '{dataset_name}' from file '{file_path}' to destination file"
        )

    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)
        target_schema = _create_target_database_schema(destination_config, schema)

    # TODO: if incremental, verify database schema and recreate + full load if different!

    destination_type = destination_config.type
    
    # Get the appropriate connector
    if destination_type == "postgresql":
        connector = PostgreSQLConnector()
        connector.connect(destination_config)
    elif destination_type == "mysql":
        connector = MySQLConnector()
        connector.connect(destination_config)
    elif destination_type == "file":
        # For file connectors
        file_config = destination_config  # type: ignore
        if file_config.format == "csv":
            connector = CSVConnector(file_config)
        elif file_config.format == "json":
            connector = JSONConnector(file_config)
        else:
            raise ValueError(f"Unsupported file format: {file_config.format}")
    else:
        logger.error(f"Unsupported destination type: {destination_type}")
        raise ValueError(f"Unsupported destination type: {destination_type}")
    
    try:
        # Create LoadConfig from dataset_config
        load_config = LoadConfig(
            strategy=dataset_config.strategy,
            replace_method=dataset_config.replace.how if hasattr(dataset_config, 'replace') and dataset_config.replace else ReplaceMethod.RECREATE,
            merge_key=getattr(dataset_config, 'merge_key', None),
            batch_size=getattr(dataset_config, 'batch_size', None)
        )
        
        # Handle table creation/truncation for replace strategy (only for database connectors)
        if destination_type in ["mysql", "postgresql"] and load_config.strategy == LoadStrategy.REPLACE:
            if load_config.replace_method == ReplaceMethod.RECREATE:
                # Recreate the table, dropping it first
                connector.create_table(dataset_name, target_schema)
            elif load_config.replace_method == ReplaceMethod.TRUNCATE:
                # Only truncate the table, keeping the structure
                connector.truncate_table(dataset_name)
            else:
                logger.error(
                    f"Unsupported replace method '{load_config.replace_method.value}' for dataset '{dataset_name}'"
                )
                raise ValueError(f"Unsupported replace method: {load_config.replace_method.value}")
        
        # Read data from file
        with open(file_path, "rb") as file:
            import extral.store as store
            data_bytes = store.decompress_data(file.read())
            data_str = data_bytes.decode("utf-8")
            data = json.loads(data_str)
        
        # Use the new load_data method
        connector.load_data(dataset_name, data, load_config)
        
    finally:
        # Only disconnect database connectors
        if destination_type in ["mysql", "postgresql"]:
            connector.disconnect()
