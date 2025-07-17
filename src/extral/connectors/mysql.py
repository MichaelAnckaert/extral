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
import logging
from typing import Any, Generator

import pymysql.cursors

from extral.config import DatabaseConfig, TableConfig
from extral.database import DatabaseRecord
from extral.connectors import DatabaseInterface, ExtractConfig, DEFAULT_BATCH_SIZE
from extral.schema import TargetDatabaseSchema

logger = logging.getLogger(__name__)


class MySQLConnector(DatabaseInterface):
    def connect(self, config: DatabaseConfig) -> None:
        host = config.get("host")
        port = config.get("port", 3306)
        user = config.get("user", "")
        password = config.get("password", "")
        database = config.get("database", "")
        charset: str = config.get("charset", "utf8mb4")

        # validate required parameters
        if not all([host, user, password, database]):
            raise ValueError("Missing required MySQL connection parameters.")

        try:
            port = int(port)
        except ValueError:
            raise ValueError(f"Invalid port number: {port}. It must be an integer.")

        self.connection = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            cursorclass=pymysql.cursors.SSDictCursor,
        )

    def disconnect(self) -> None:
        self.connection.close()

    def extract_schema_for_table(self, table_name: str) -> tuple[dict[str, Any], ...]:
        with self.connection.cursor() as cursor:
            sql = f"DESCRIBE {table_name}"
            logger.debug(f"Running SQL: {sql}")
            cursor.execute(sql)
            result = cursor.fetchall()
            return result

    def is_table_exists(self, table_name: str) -> bool:
        raise NotImplementedError("This method should be overridden by subclasses")

    def create_table(self, table_name: str, dbschema: TargetDatabaseSchema) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")

    def extract_data(
        self,
        table_config: TableConfig,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        table_name = table_config["name"]
        batch_size = table_config.get("batch_size") or DEFAULT_BATCH_SIZE

        logger.debug(
            f"Extracting data from table: {table_name} with batch size: {batch_size} and extract_config: {extract_config}"
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
                base_sql = f"SELECT * FROM {table_name} {where_sql}"
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
                        yield result[:batch_size]  # type: ignore
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
                    result = cursor.fetchall()
                    yield result  # type: ignore

    def truncate_table(self, table_name: str) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")

    def load_table(self, table_config: TableConfig, file_path: str) -> None:
        raise NotImplementedError("This method should be overridden by subclasses")
