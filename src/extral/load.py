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

from extral.config import DatabaseConfig, TableConfig
from extral.connectors import postgresql
from extral.database import DatabaseTypeTranslator
from extral.schema import DatabaseSchema, TargetDatabaseSchema

logger = logging.getLogger(__name__)

DEFAULT_STRATEGY = "replace"


def _create_target_database_schema(
    destination_config: DatabaseConfig, schema: DatabaseSchema
) -> TargetDatabaseSchema:
    destination_type = destination_config.get("type")
    if destination_type not in ["mysql", "postgresql"]:
        logger.error("Unsupported destination type: %s", destination_type)
        raise ValueError(f"Unsupported destination type: {destination_type}")

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
    destination_config: DatabaseConfig,
    table_config: TableConfig,
    file_path: str,
    schema_path: str,
):
    table_name = table_config["name"]
    incremental = table_config.get("incremental", None)

    logger.info(
        f"Loading data for table '{table_name}' from file '{file_path}' to destination '{destination_config['database']}'"
    )

    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)
        target_schema = _create_target_database_schema(destination_config, schema)

    # TODO: if incremental, verify database schema and recreate + full load if different!

    if destination_config.get("type") == "postgresql":
        connector = postgresql.PostgresqlConnector()
        connector.connect(destination_config)

        if incremental is None or not connector.is_table_exists(table_name):
            connector.create_table(table_name, dbschema=target_schema)

        # Handle strategy
        strategy = table_config.get("strategy") or DEFAULT_STRATEGY
        if strategy == "replace":
            connector.truncate_table(table_name)
            connector.load_table(
                table_config,
                file_path=file_path,
            )
        elif strategy == "merge":
            merge_key = table_config.get("merge_key")
            if not merge_key:
                logger.error(
                    f"Merge strategy requires a 'merge_key' for table '{table_name}'"
                )
                raise ValueError(f"Merge key not specified for table '{table_name}'")
            connector.load_table(
                table_config,
                file_path=file_path,
            )

        elif strategy == "append":
            connector.load_table(
                table_config,
                file_path=file_path,
            )
