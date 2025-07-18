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
MySQL Database Connector
Implementation of the generic DatabaseConnector interface for MySQL/MariaDB databases.
"""

import logging
from typing import Any, Dict, Generator, Tuple

import pymysql.cursors

from extral import schema
from extral.connectors.database.generic import DatabaseConnector
from extral.config import DatabaseConfig, ExtractConfig, LoadConfig, LoadStrategy
from extral.database import DatabaseRecord
from extral.schema import TargetDatabaseSchema

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 50000


class MySQLConnector(DatabaseConnector):
    """
    MySQL implementation of the generic DatabaseConnector interface.
    
    This connector handles MySQL-specific operations while implementing
    both the generic Connector interface and database-specific operations.
    """
    
    def connect(self, config: DatabaseConfig) -> None:
        """Establish connection to MySQL database."""
        self.config = config
        self.connection = pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            charset=config.charset,
            cursorclass=pymysql.cursors.SSDictCursor,
        )
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
    
    def is_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.config:
            return False

        database = self.config.database
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (database, table_name)
            )
            result = cursor.fetchone()
            return result["COUNT(*)"] > 0 if result else False
    
    def extract_schema_for_table(self, table_name: str) -> Tuple[Dict[str, Any], ...]:
        """Extract schema information for a table."""
        with self.connection.cursor() as cursor:
            sql_query = f"DESCRIBE {table_name}"
            logger.debug(f"Running SQL: {sql_query}")
            cursor.execute(sql_query)
            result = cursor.fetchall()

            schema = [
                {
                    "Field": column["Field"],
                    "type": column["Type"],
                    "nullable": column["Null"] == "YES",
                }
                for column in result
            ]

            return tuple(schema)
    
    def create_table(self, table_name: str, schema: TargetDatabaseSchema) -> None:
        """Create a table with the specified schema."""
        logger.debug(f"Creating table '{table_name}' with schema: {schema}")
        
        columns = []
        for column_name, column_info in schema["schema"].items():
            column_type = column_info["type"]
            nullable = "NULL" if column_info.get("nullable", False) else "NOT NULL"
            columns.append(f"`{column_name}` {column_type} {nullable}")
        
        create_table_query = (
            f"DROP TABLE IF EXISTS {table_name}; "
            f"CREATE TABLE {table_name} ({', '.join(columns)});"
        )
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
        self.connection.commit()
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate a table in the MySQL database."""
        logger.debug(f"Truncating table '{table_name}'")
        
        if not self.is_table_exists(table_name):
            return
        
        with self.connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        self.connection.commit()
    
    
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """Extract data from a MySQL table."""
        batch_size = extract_config.batch_size or DEFAULT_BATCH_SIZE
        
        logger.debug(
            f"Extracting data from table: {dataset_name} "
            f"with batch size: {batch_size}"
        )
        
        where_sql = ""
        if extract_config.extract_type == "INCREMENTAL":
            incremental_field = extract_config.incremental_field
            last_value = extract_config.last_value
            where_sql = f"WHERE {incremental_field} > {last_value}"
        
        with self.connection:
            with self.connection.cursor() as cursor:
                base_sql = f"SELECT * FROM {dataset_name} {where_sql}"
                if batch_size:
                    limit_size = batch_size + 1
                    offset = 0
                    sql_query = f"{base_sql} LIMIT {limit_size}"
                    
                    while True:
                        logger.debug(f"Running SQL: {sql_query}")
                        cursor.execute(sql_query)
                        result = cursor.fetchall()
                        
                        if not result:
                            break
                        
                        yield result[:batch_size]
                        
                        if len(result) > batch_size:
                            offset += batch_size
                            sql_query = f"{base_sql} LIMIT {limit_size} OFFSET {offset}"
                            logger.debug("Fetching next batch of size %d", batch_size)
                        else:
                            break
                else:
                    sql_query = base_sql
                    logger.debug(f"Running SQL: {sql_query}")
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    yield result
    
    def _handle_replace_strategy(
        self, 
        schema: str,
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle REPLACE strategy with truncate method."""
        # For MySQL, we'll use truncate for replace strategy
        self.truncate_table(dataset_name)
        self._bulk_insert_data(dataset_name, data)
    
    def _handle_merge_strategy(
        self, 
        schema: str,
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle MERGE strategy using INSERT ... ON DUPLICATE KEY UPDATE."""
        if not load_config.merge_key:
            raise ValueError(f"Merge key not specified for table '{dataset_name}'")
        
        self._upsert_data(dataset_name, data, load_config.merge_key)
    
    def _handle_append_strategy(
        self,
        schema: str,
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle APPEND strategy by inserting all records."""
        self._bulk_insert_data(dataset_name, data)
    
    def _bulk_insert_data(
        self,
        table_name: str,
        data: list[DatabaseRecord],
    ) -> None:
        """Insert data using bulk INSERT statements."""
        if not data:
            return
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join([f"`{col}`" for col in columns])
        
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Convert data to list of tuples for executemany
        values = []
        for record in data:
            values.append(tuple(record.get(col) for col in columns))
        
        with self.connection.cursor() as cursor:
            cursor.executemany(insert_query, values)
        
        logger.debug(f"Inserted {len(data)} records into table '{table_name}'")
    
    def _upsert_data(
        self,
        table_name: str,
        data: list[DatabaseRecord],
        merge_key: str,
    ) -> None:
        """Perform upsert using INSERT ... ON DUPLICATE KEY UPDATE."""
        if not data:
            return
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join([f"`{col}`" for col in columns])
        
        # Create UPDATE clause for non-key columns
        update_columns = [col for col in columns if col != merge_key]
        update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
        
        upsert_query = (
            f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
        
        # Convert data to list of tuples for executemany
        values = []
        for record in data:
            values.append(tuple(record.get(col) for col in columns))
        
        with self.connection.cursor() as cursor:
            cursor.executemany(upsert_query, values)
        
        logger.debug(f"Upserted {len(data)} records into table '{table_name}'")