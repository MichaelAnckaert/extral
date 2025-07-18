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
PostgreSQL Database Connector
Implementation of the generic DatabaseConnector interface for PostgreSQL databases.
"""

import json
import logging
from io import StringIO
from typing import Any, Dict, Generator, Optional, Tuple

import psycopg2
from psycopg2 import sql

from extral.connectors.database.generic import DatabaseConnector
from extral.config import DatabaseConfig, ExtractConfig, LoadConfig, LoadStrategy, ReplaceMethod
from extral.database import DatabaseRecord
from extral.schema import TargetDatabaseSchema

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 50000


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL implementation of the generic DatabaseConnector interface.
    
    This connector handles PostgreSQL-specific operations while implementing
    both the generic Connector interface and database-specific operations.
    """
    
    def connect(self, config: DatabaseConfig) -> None:
        """Establish connection to PostgreSQL database."""
        self.config = config
        self.connection = psycopg2.connect(
            dbname=config.database,
            user=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
        )
        self.cursor = self.connection.cursor()
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def is_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        if not self.config:
            return False
        
        schema = self.config.schema or "public"
        self.cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables "
            "WHERE table_name=%s AND table_schema=%s)",
            (table_name, schema)
        )
        result = self.cursor.fetchone()
        return result[0] if result else False
    
    def extract_schema_for_table(self, table_name: str) -> Tuple[Dict[str, Any], ...]:
        """Extract the schema for a given table in the PostgreSQL database."""
        if not self.config:
            raise ValueError("Database connection not established")
        
        schema = self.config.schema or "public"
        self.cursor.execute(
            """
            SELECT 
                column_name,
                CASE 
                    WHEN data_type = 'character' AND character_octet_length IS NOT NULL THEN
                        concat('character(', character_octet_length, ')')
                    ELSE data_type
                END AS data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
            """,
            (table_name, schema),
        )
        columns = self.cursor.fetchall()
        
        if not columns:
            raise ValueError(
                f"Table '{table_name}' does not exist in schema '{schema}'"
            )
        
        schema_info = [
            {
                "Field": col[0],
                "Type": col[1],
                "Null": col[2],
            }
            for col in columns
        ]
        
        return tuple(schema_info)
    
    def create_table(self, table_name: str, schema: TargetDatabaseSchema) -> None:
        """Create a table in the PostgreSQL database based on the provided schema."""
        if not self.config:
            raise ValueError("Database connection not established")
        
        db_schema = self.config.schema or "public"
        logger.debug(f"Creating table '{table_name}' with schema: {schema}")
        
        columns = []
        for column_name, column_info in schema["schema"].items():
            column_type = column_info["type"]
            nullable = "NULL" if column_info.get("nullable", False) else "NOT NULL"
            columns.append(f'"{column_name}" {column_type} {nullable}')
        
        create_table_query = (
            f"DROP TABLE IF EXISTS {db_schema}.{table_name}; "
            f"CREATE TABLE {db_schema}.{table_name} ({', '.join(columns)});"
        )
        
        self.cursor.execute(create_table_query)
        self.connection.commit()
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate a table in the PostgreSQL database."""
        if not self.config:
            raise ValueError("Database connection not established")
        
        schema = self.config.schema or "public"
        logger.debug(f"Truncating table '{table_name}' in schema '{schema}'")
        
        if not self.is_table_exists(table_name):
            return
        
        self.cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} CASCADE;")
        self.connection.commit()
    
    
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """Extract data from a PostgreSQL table."""
        if not self.config:
            raise ValueError("Database connection not established")
        
        schema = self.config.schema or "public"
        batch_size = extract_config.batch_size or DEFAULT_BATCH_SIZE
        
        logger.debug(
            f"Extracting data from table: {schema}.{dataset_name} "
            f"with batch size: {batch_size}"
        )
        
        where_sql = ""
        if extract_config.extract_type == "INCREMENTAL":
            incremental_field = extract_config.incremental_field
            last_value = extract_config.last_value
            where_sql = f"WHERE {incremental_field} > {last_value}"
        
        with self.connection:
            with self.connection.cursor() as cursor:
                base_sql = f"SET search_path TO {schema}; SELECT * FROM {dataset_name} {where_sql}"
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
                        
                        yield self._tuple_to_dict_results(cursor, result[:batch_size])
                        
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
                    results = cursor.fetchall()
                    yield self._tuple_to_dict_results(cursor, results)

    
    def _handle_replace_strategy(
        self, 
        schema: str, 
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle REPLACE strategy with truncate or recreate method."""
        if load_config.replace_method == ReplaceMethod.TRUNCATE:
            self.truncate_table(dataset_name)
        elif load_config.replace_method == ReplaceMethod.RECREATE:
            # Note: This would require schema information to recreate the table
            # For now, we'll just truncate as recreate needs additional schema handling
            self.truncate_table(dataset_name)
        
    def _handle_load_data(
        self,
        schema: str,
        dataset_name: str,
        data: list[DatabaseRecord],
    ) -> None:
        self._bulk_load_with_copy(schema, dataset_name, data)
    
    def _handle_merge_strategy(
        self, 
        schema: str, 
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle MERGE strategy using upsert operations."""
        if not load_config.merge_key:
            raise ValueError(f"Merge key not specified for table '{dataset_name}'")
        
        self._merge_load_data(schema, dataset_name, data, load_config.merge_key)
    
    def _handle_append_strategy(
        self, 
        schema: str, 
        dataset_name: str, 
        data: list[DatabaseRecord], 
        load_config: LoadConfig
    ) -> None:
        """Handle APPEND strategy by inserting all records."""
        self._bulk_load_with_copy(schema, dataset_name, data)
    
    def _tuple_to_dict_results(
        self, 
        cursor: psycopg2.extensions.cursor, 
        results: Optional[list[tuple]]
    ) -> list[DatabaseRecord]:
        """Convert tuple-based query results to dictionary format."""
        if results is None:
            return []
        
        if cursor.description is None:
            raise ValueError("Cursor description is None, cannot convert results to dict")
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in results]
    
    def _bulk_load_with_copy(
        self,
        schema: str,
        table_name: str,
        json_data: list[DatabaseRecord],
        columns: Optional[list[str]] = None,
    ) -> None:
        """Use COPY FROM for maximum performance with large datasets."""
        if not json_data:
            return
        
        if columns is None:
            columns = list(json_data[0].keys())
        
        # Create a StringIO buffer with tab-delimited data
        buffer = StringIO()
        for record in json_data:
            values = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    values.append("\\N")  # PostgreSQL NULL representation
                elif isinstance(value, (dict, list)):
                    values.append(json.dumps(value))
                else:
                    # Convert to string and escape special characters
                    str_value = str(value)
                    str_value = str_value.replace("\\", "\\\\")
                    str_value = str_value.replace("\t", "\\t")
                    str_value = str_value.replace("\n", "\\n")
                    str_value = str_value.replace("\r", "\\r")
                    values.append(str_value)
            
            buffer.write("\t".join(values) + "\n")
        
        # Reset buffer position
        buffer.seek(0)
        
        # Use COPY FROM
        self.cursor.execute(f"SET search_path TO {schema}")
        self.cursor.copy_from(buffer, table_name, columns=columns, null="\\N")
    
    def _merge_load_data(
        self,
        schema: str,
        table_name: str,
        json_data: list[DatabaseRecord],
        merge_key: str,
    ) -> None:
        """Perform merge operation using temporary table."""
        if not json_data:
            return
        
        try:
            columns = list(json_data[0].keys())
            temp_table = f"temp_{table_name}_{id(self.connection)}"
            
            # Create staging table with same structure
            self.cursor.execute(
                sql.SQL("""
                CREATE TEMP TABLE {temp_table} 
                (LIKE {schema}.{table_name} INCLUDING ALL)
            """).format(
                    temp_table=sql.Identifier(temp_table),
                    schema=sql.Identifier(schema),
                    table_name=sql.Identifier(table_name),
                )
            )
            
            # Bulk load data into staging table
            self._bulk_load_with_copy(schema, temp_table, json_data, columns=columns)
            
            # Update existing records
            update_columns = [col for col in columns if col != merge_key]
            update_set = sql.SQL(", ").join(
                [
                    sql.SQL("{col} = s.{col}").format(col=sql.Identifier(col))
                    for col in update_columns
                ]
            )
            
            update_query = sql.SQL("""
                UPDATE {schema}.{table_name} t
                SET {update_set}
                FROM {temp_table} s
                WHERE t.{merge_key} = s.{merge_key}
            """).format(
                schema=sql.Identifier(schema),
                table_name=sql.Identifier(table_name),
                update_set=update_set,
                temp_table=sql.Identifier(temp_table),
                merge_key=sql.Identifier(merge_key),
            )
            
            self.cursor.execute(update_query)
            updated_rows = self.cursor.rowcount
            
            # Insert new records
            insert_columns = sql.SQL(", ").join([sql.Identifier(col) for col in columns])
            select_columns = sql.SQL(", ").join(
                [sql.SQL("s.{}").format(sql.Identifier(col)) for col in columns]
            )
            
            insert_query = sql.SQL("""
                INSERT INTO {schema}.{table_name} ({insert_columns})
                SELECT {select_columns}
                FROM {temp_table} s
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {schema}.{table_name} t
                    WHERE t.{merge_key} = s.{merge_key}
                )
            """).format(
                schema=sql.Identifier(schema),
                table_name=sql.Identifier(table_name),
                insert_columns=insert_columns,
                select_columns=select_columns,
                temp_table=sql.Identifier(temp_table),
                merge_key=sql.Identifier(merge_key),
            )
            
            self.cursor.execute(insert_query)
            inserted_rows = self.cursor.rowcount
            
            # Drop the temporary table
            self.cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table))
            )
            
            logger.debug(
                f"Merge completed. Updated {updated_rows} rows, "
                f"inserted {inserted_rows} new rows."
            )
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error during merge load: {e}")
            raise