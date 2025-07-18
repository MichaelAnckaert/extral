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
from io import StringIO
from typing import Any, Generator, Optional

import psycopg2
from psycopg2 import sql

from extral.connectors import DEFAULT_BATCH_SIZE, DatabaseInterface, ExtractConfig
from extral.database import DatabaseRecord
from extral import store
from extral.config import DatabaseConfig, TableConfig
from extral.schema import TargetDatabaseSchema

logger = logging.getLogger(__name__)


class PostgresqlConnector(DatabaseInterface):
    def connect(self, config: DatabaseConfig) -> None:
        self.config = config
        self.connection = psycopg2.connect(
            dbname=config["database"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
        )
        self.cursor = self.connection.cursor()

    def disconnect(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def extract_schema_for_table(self, table_name: str) -> tuple[dict[str, Any], ...]:
        """Extract the schema for a given table in the PostgreSQL database."""
        schema: str = self.config.get("schema") or "public"
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

    def is_table_exists(self, table_name: str) -> bool:
        schema: str = self.config.get("schema") or "public"
        self.cursor.execute(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}' and table_schema='{schema}')"
        )
        result = self.cursor.fetchone()
        if result is not None:
            return True if result[0] else False
        else:
            return False

    def create_table(self, table_name: str, dbschema: TargetDatabaseSchema) -> None:
        """Create a table in the PostgreSQL database based on the provided schema."""
        schema: str = self.config.get("schema") or "public"
        logger.debug(f"Creating table '{table_name}' with schema: {dbschema}")

        columns: list[str] = []
        for column_name, column_info in dbschema["schema"].items():
            column_type = column_info["type"]
            nullable = "NULL" if column_info.get("nullable", False) else "NOT NULL"
            columns.append(f"\"{column_name}\" {column_type} {nullable}")

        create_table_query = f"DROP TABLE IF EXISTS {schema}.{table_name}; CREATE TABLE {schema}.{table_name} ({', '.join(columns)});"

        self.cursor.execute(create_table_query)
        self.connection.commit()

    def _tuple_to_dict_results(
        self, cursor: psycopg2.extensions.cursor, results: Optional[list[tuple[Any]]]
    ) -> Optional[list[dict[str, Any]]]:
        """
        Convert tuple-based query results to dictionary format.

        Args:
            results: List of tuples or single tuple from fetchone/fetchall/fetchmany

        Returns:
            List of dicts or single dict with column names as keys
        """
        if results is None:
            return None

        # Get column names from cursor description
        if cursor.description is None:
            raise ValueError(
                "Cursor description is None, cannot convert results to dict"
            )

        columns: list[str] = [desc[0] for desc in cursor.description]

        # Handle multiple rows (from fetchall/fetchmany)
        return [dict(zip(columns, row)) for row in results]

    def extract_data(
        self,
        table_config: TableConfig,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        table_name = table_config["name"]
        schema = self.config.get("schema") or "public"
        batch_size = table_config.get("batch_size") or DEFAULT_BATCH_SIZE

        logger.debug(
            f"Extracting data from table: {schema}.{table_name} with batch size: {batch_size} and extract_config: {extract_config}"
        )

        where_sql = ""
        if extract_config:
            if extract_config.get("extract_type") == "INCREMENTAL":
                incremental_field = extract_config.get("incremental_field")
                last_value = extract_config.get("last_value")
                # Modify the SQL query to include the incremental extraction logic
                where_sql = f"WHERE {incremental_field} > {last_value}"

        with self.connection:
            with self.connection.cursor() as cursor:
                base_sql = f"SET search_path TO {schema}; SELECT * FROM {table_name} {where_sql}"
                if batch_size:
                    limit_size = (
                        batch_size + 1
                    )  # +1 to check if there are more records than batch_size
                    offset = 0
                    sql = f"{base_sql} limit {limit_size}"
                    while True:
                        logger.debug(f"Running SQL: {sql}")
                        cursor.execute(sql)
                        result = cursor.fetchall()
                        if not result:
                            break
                        yield self._tuple_to_dict_results(cursor, result[:batch_size])  # type: ignore
                        if len(result) > batch_size:
                            # If we fetched more than batch_size, we need to continue fetching
                            offset += batch_size
                            sql = f"{base_sql} limit {limit_size} offset {offset}"
                            logger.debug("Fetching next batch of size %d", batch_size)
                        else:
                            break
                else:
                    sql = f"{base_sql}"
                    logger.debug(f"Running SQL: {sql}")
                    cursor.execute(sql)
                    results = cursor.fetchall()
                    yield self._tuple_to_dict_results(cursor, results)  # type: ignore

    def truncate_table(self, table_name: str) -> None:
        """Truncate a table in the PostgreSQL database."""
        schema: str = self.config.get("schema") or "public"
        logger.debug(f"Truncating table '{table_name}' in schema '{schema}'")

        self.cursor.execute(f"TRUNCATE TABLE {schema}.{table_name} CASCADE;")
        self.connection.commit()

    def load_table(self, table_config: TableConfig, file_path: str) -> None:
        """Load data into the PostgreSQL database."""
        schema: str = self.config.get("schema") or "public"
        with open(file_path, "rb") as file:
            data_bytes = store.decompress_data(file.read())
            data_str = data_bytes.decode("utf-8")
            data = json.loads(data_str)

        batch_size = table_config.get("batch_size")
        table_name = table_config["name"]
        merge_key = table_config.get("merge_key")
        if batch_size:
            batches = [
                data[i : i + batch_size] for i in range(0, len(data), batch_size)
            ]
        else:
            batches = [data]

        logger.debug(
            f"Loading data into table '{table_name}' in {len(batches)} batches of size {batch_size}"
        )

        for batch in batches:
            if merge_key is not None:
                self._merge_load_data(schema, table_name, batch, merge_key)
            else:
                self._bulk_load_with_copy(schema, table_name, batch)

        self.connection.commit()

        logger.info(f"Data loaded successfully into table '{table_name}'")

    def _bulk_load_with_copy(
        self,
        schema: str,
        table_name: str,
        json_data: list[dict[str, Optional[str]]],
        columns: Optional[list[str]] = None,
    ):
        """
        Use COPY FROM for maximum performance with large datasets
        """

        # If columns not specified, get from first record
        if columns is None:
            columns = list(json_data[0].keys())

        # Create a StringIO buffer with tab-delimited data
        buffer = StringIO()
        for record in json_data:
            values: list[str] = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    values.append("\\N")  # PostgreSQL NULL representation
                elif isinstance(value, (dict, list)):
                    # Convert nested structures to JSON
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
        json_data: list[dict[str, Optional[str]]],
        merge_key: str,
    ):
        if not json_data:
            return

        try:
            # Get columns from first record
            columns = list(json_data[0].keys())

            # Create a temporary staging table
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

            # Bulk load data into staging table using COPY
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
            insert_columns = sql.SQL(", ").join(
                [sql.Identifier(col) for col in columns]
            )
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

            # Drop the temporary table (optional, will be dropped at end of session anyway)
            self.cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table))
            )

            self.connection.commit()

            logger.debug(
                f"Merge completed. Updated {updated_rows} rows, inserted {inserted_rows} new rows."
            )
            return updated_rows + inserted_rows

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error during merge load: {e}")
            raise
