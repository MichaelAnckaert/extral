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
from typing import Any, Generator, Optional
from extral.config import DatabaseConfig, TableConfig
from extral.database import DatabaseRecord
from extral.schema import TargetDatabaseSchema

# Legacy types for backward compatibility
ExtractConfig = dict[str, Optional[str | int | None]]
DEFAULT_BATCH_SIZE = 50000

# Import new connector interfaces
from .connector import Connector
from .database import DatabaseConnector, PostgreSQLConnector, MySQLConnector

# Legacy DatabaseInterface for backward compatibility
class DatabaseInterface:
    def connect(self, config: DatabaseConfig) -> None:
        raise NotImplementedError(
            "The method 'connect' should be overridden by subclasses"
        )

    def disconnect(self) -> None:
        raise NotImplementedError(
            "The method 'disconnect' should be overridden by subclasses"
        )

    def extract_schema_for_table(self, table_name: str) -> tuple[dict[str, Any], ...]:
        raise NotImplementedError(
            "The method 'extract_schema_for_table' should be overridden by subclasses"
        )

    def is_table_exists(self, table_name: str) -> bool:
        raise NotImplementedError(
            "The method 'is_table_exists' should be overridden by subclasses"
        )

    def create_table(self, table_name: str, dbschema: TargetDatabaseSchema) -> None:
        raise NotImplementedError(
            "The method 'create_table' should be overridden by subclasses"
        )

    def extract_data(
        self,
        table_config: TableConfig,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        raise NotImplementedError(
            "The method 'extract_data' should be overridden by subclasses"
        )

    def truncate_table(self, table_name: str) -> None:
        raise NotImplementedError(
            "The method 'truncate_table' should be overridden by subclasses"
        )

    def load_table(self, table_config: TableConfig, file_path: str) -> None:
        raise NotImplementedError(
            "The method 'load_table' should be overridden by subclasses"
        )


# Export both old and new interfaces
__all__ = [
    # Legacy
    "DatabaseInterface", 
    "ExtractConfig", 
    "DEFAULT_BATCH_SIZE",
    # New interfaces
    "Connector",
    "DatabaseConnector", 
    "PostgreSQLConnector", 
    "MySQLConnector"
]
