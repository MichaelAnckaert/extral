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
"""
Generic Database Connector Interface
Provides database-specific operations that extend the generic Connector interface.
"""

from abc import abstractmethod
import logging
from typing import Any, Dict, Generator, Optional, Tuple

from extral.connectors.connector import Connector
from extral.config import DatabaseConfig, ExtractConfig, LoadConfig, LoadStrategy, ReplaceMethod
from extral.database import DatabaseRecord
from extral.schema import TargetDatabaseSchema

logger = logging.getLogger(__name__)


class DatabaseConnector(Connector):
    """
    Generic database connector that extends the base Connector interface
    with database-specific operations.
    
    This class provides the database-specific functionality like connection management,
    table operations, and schema handling while implementing the generic ETL operations.
    """
    
    def __init__(self):
        """Initialize the database connector."""
        self.connection = None
        self.cursor = None
        self.config: Optional[DatabaseConfig] = None
    
    @abstractmethod
    def connect(self, config: DatabaseConfig) -> None:
        """
        Establish connection to the database.
        
        Args:
            config: Database configuration containing connection parameters
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def is_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        pass
    
    @abstractmethod
    def extract_schema_for_table(self, table_name: str) -> Tuple[Dict[str, Any], ...]:
        """
        Extract schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Tuple of dictionaries containing column information
        """
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, schema: TargetDatabaseSchema) -> None:
        """
        Create a table with the specified schema.
        
        Args:
            table_name: Name of the table to create
            schema: Schema definition for the table
        """
        pass
    
    @abstractmethod
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate a table (remove all data but keep structure).
        
        Args:
            table_name: Name of the table to truncate
        """
        pass
    
    # Implementation of generic Connector interface methods
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """
        Extract data from a database table.
        
        Args:
            dataset_name: Name of the table to extract from
            extract_config: Configuration for extraction
            
        Yields:
            Batches of database records
        """
        # This method should be implemented by subclasses
        raise NotImplementedError(
            "Subclasses must implement extract_data method"
        )

    def _handle_load_data(
        self,
        schema: str,
        dataset_name: str,
        data: list[DatabaseRecord],
    ):
        pass

    def _handle_merge_data(
            self,
            schema: str,
            dataset_name: str,
            data: list[DatabaseRecord],
            load_config: LoadConfig,
    ):
        pass

    def _handle_append_strategy(
        self, 
        schema: str, 
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        pass

    def load_data(
        self,
        dataset_name: str,
        data: list[DatabaseRecord],
        load_config: LoadConfig,
    ) -> None:
        """
        Load data into a database table using the specified strategy.
        
        Args:
            dataset_name: Name of the target table
            data: List of records to load
            load_config: Configuration specifying the load strategy
        """
        if not self.config:
            raise ValueError("Database connection not established")
        
        schema = self.config.schema or "public"
        
        logger.debug(
            f"Loading data into table '{dataset_name}' "
            f"with strategy: {load_config.strategy.value}"
        )
        
        if load_config.strategy == LoadStrategy.REPLACE:
            if load_config.replace_method == ReplaceMethod.TRUNCATE:
                self.truncate_table(dataset_name)
            elif load_config.replace_method == ReplaceMethod.RECREATE:
                self.truncate_table(dataset_name)
            else:
                raise ValueError(f"Unsupported replace method: {load_config.replace_method}")
            self._handle_load_data(schema, dataset_name, data)
        elif load_config.strategy == LoadStrategy.MERGE:
            self._handle_merge_data(schema, dataset_name, data, load_config)
        elif load_config.strategy == LoadStrategy.APPEND:
            self._handle_append_strategy(schema, dataset_name, data, load_config)
        
        self.connection.commit()
        logger.info(f"Data loaded successfully into table '{dataset_name}'")
    
    # Helper methods for common database operations
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing schema information
        """
        schema_tuple = self.extract_schema_for_table(table_name)
        return {
            "columns": [
                {
                    "name": col.get("Field", col.get("column_name")),
                    "type": col.get("Type", col.get("data_type")),
                    "nullable": col.get("Null") == "YES" or col.get("is_nullable") == "YES",
                }
                for col in schema_tuple
            ]
        }
    
    def dataset_exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset (table) exists - implements generic Connector interface.
        
        Args:
            dataset_name: Name of the dataset/table to check
            
        Returns:
            True if dataset exists, False otherwise
        """
        return self.is_table_exists(dataset_name)
    
    def get_dataset_schema(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get schema information for a dataset (table) - implements generic Connector interface.
        
        Args:
            dataset_name: Name of the dataset/table
            
        Returns:
            Dictionary containing schema information
        """
        return self.get_table_schema(dataset_name)