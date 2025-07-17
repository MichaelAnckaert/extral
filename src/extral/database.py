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
"""
Database Type Translator Module
Translates data types between MySQL and PostgreSQL databases
"""

import re
from typing import Dict, Optional

DatabaseRecord = dict[str, Optional[str]]


class DatabaseTypeTranslator:
    """Translates database column types between MySQL and PostgreSQL"""

    def __init__(self):
        # MySQL to PostgreSQL mappings
        self.mysql_to_pg = {
            # Numeric types
            "tinyint": "smallint",
            "smallint": "smallint",
            "mediumint": "integer",
            "int": "integer",
            "integer": "integer",
            "bigint": "bigint",
            "decimal": "decimal",
            "numeric": "numeric",
            "float": "real",
            "double": "double precision",
            "double precision": "double precision",
            "bit": "bit",
            # String types
            "char": "char",
            "varchar": "varchar",
            "binary": "bytea",
            "varbinary": "bytea",
            "tinyblob": "bytea",
            "blob": "bytea",
            "mediumblob": "bytea",
            "longblob": "bytea",
            "tinytext": "text",
            "text": "text",
            "mediumtext": "text",
            "longtext": "text",
            "enum": "varchar",  # PostgreSQL has enum but requires type creation
            "set": "text",  # Store as comma-separated values
            # Date and time types
            "date": "date",
            "time": "time",
            "datetime": "timestamp",
            "timestamp": "timestamp",
            "year": "smallint",
            # Boolean
            "bool": "boolean",
            "boolean": "boolean",
            # JSON
            "json": "json",
            # Spatial (basic mapping - may need PostGIS)
            "geometry": "geometry",
            "point": "point",
            "linestring": "path",
            "polygon": "polygon",
        }

        # PostgreSQL to MySQL mappings
        self.pg_to_mysql = {
            # Numeric types
            "smallint": "smallint",
            "integer": "int",
            "bigint": "bigint",
            "decimal": "decimal",
            "numeric": "numeric",
            "real": "float",
            "double precision": "double",
            "smallserial": "smallint",
            "serial": "int",
            "bigserial": "bigint",
            "money": "decimal(19,2)",
            "bit": "bit",
            # String types
            "character varying": "varchar",
            "varchar": "varchar",
            "character": "char",
            "char": "char",
            "text": "text",
            "bytea": "blob",
            # Date and time types
            "timestamp": "datetime",
            "timestamp without time zone": "datetime",
            "timestamp with time zone": "timestamp",
            "date": "date",
            "time": "time",
            "time without time zone": "time",
            "time with time zone": "time",
            "interval": "varchar(255)",  # No direct equivalent
            # Boolean
            "boolean": "tinyint(1)",
            # JSON
            "json": "json",
            "jsonb": "json",
            # UUID
            "uuid": "varchar(36)",
            # Arrays (stored as JSON in MySQL)
            "integer[]": "json",
            "text[]": "json",
            "varchar[]": "json",
            # Network types (stored as varchar in MySQL)
            "inet": "varchar(45)",
            "cidr": "varchar(43)",
            "macaddr": "varchar(17)",
            # Geometric types
            "point": "point",
            "line": "linestring",
            "lseg": "linestring",
            "box": "polygon",
            "path": "linestring",
            "polygon": "polygon",
            "circle": "varchar(255)",  # No direct equivalent
        }

        # Special handling patterns
        self.unsigned_pattern = re.compile(r"(\w+)\s+unsigned", re.IGNORECASE)
        self.auto_increment_pattern = re.compile(r"auto_increment", re.IGNORECASE)
        self.length_pattern = re.compile(r"(\w+)\((\d+(?:,\d+)?)\)")
        self.array_pattern = re.compile(r"(\w+)\[\]")

    def translate(self, type_str: str, from_db: str, to_db: str) -> str:
        """
        Translate a type string from one database to another

        Args:
            type_str: The type string to translate (e.g., 'varchar(255)', 'int unsigned')
            from_db: Source database ('mysql' or 'postgresql')
            to_db: Target database ('mysql' or 'postgresql')

        Returns:
            Translated type string
        """
        from_db = from_db.lower()
        to_db = to_db.lower()

        if from_db == to_db:
            return type_str

        if from_db == "mysql" and to_db == "postgresql":
            return self._mysql_to_postgresql(type_str)
        elif from_db == "postgresql" and to_db == "mysql":
            return self._postgresql_to_mysql(type_str)
        else:
            raise ValueError(f"Unsupported database combination: {from_db} to {to_db}")

    def _mysql_to_postgresql(self, type_str: str) -> str:
        """Convert MySQL type to PostgreSQL"""
        type_str = type_str.strip().lower()

        # Handle AUTO_INCREMENT
        if self.auto_increment_pattern.search(type_str):
            if "bigint" in type_str:
                return "bigserial"
            elif "smallint" in type_str:
                return "smallserial"
            else:
                return "serial"

        # Handle UNSIGNED
        unsigned_match = self.unsigned_pattern.match(type_str)
        if unsigned_match:
            base_type = unsigned_match.group(1)
            # PostgreSQL doesn't have unsigned, so we might need to use a larger type
            if base_type == "tinyint":
                return "smallint"
            elif base_type == "smallint":
                return "integer"
            elif base_type == "int" or base_type == "integer":
                return "bigint"
            elif base_type == "bigint":
                return "numeric(20,0)"

        # Extract base type and length
        length_match = self.length_pattern.match(type_str)
        if length_match:
            base_type = length_match.group(1)
            length = length_match.group(2)

            # Get the PostgreSQL equivalent
            pg_type = self.mysql_to_pg.get(base_type, base_type)

            # Some types keep their length specification
            if pg_type in ["varchar", "char", "bit", "decimal", "numeric"]:
                return f"{pg_type}({length})"
            else:
                return pg_type

        # Direct mapping
        return self.mysql_to_pg.get(type_str, type_str)

    def _postgresql_to_mysql(self, type_str: str) -> str:
        """Convert PostgreSQL type to MySQL"""
        type_str = type_str.strip().lower()

        # Handle arrays
        array_match = self.array_pattern.match(type_str)
        if array_match:
            return "json"

        # Handle serial types
        if type_str in ["smallserial", "serial", "bigserial"]:
            base_types = {
                "smallserial": "smallint",
                "serial": "int",
                "bigserial": "bigint",
            }
            return f"{base_types[type_str]} AUTO_INCREMENT"

        # Extract base type and length
        length_match = self.length_pattern.match(type_str)
        if length_match:
            base_type = length_match.group(1)
            length = length_match.group(2)

            # Get the MySQL equivalent
            mysql_type = self.pg_to_mysql.get(base_type, base_type)

            # Some types keep their length specification
            if mysql_type in ["varchar", "char", "bit", "decimal", "numeric"]:
                return f"{mysql_type}({length})"
            else:
                return mysql_type

        # Direct mapping
        return self.pg_to_mysql.get(type_str, type_str)

    def get_supported_types(self, database: str) -> Dict[str, str]:
        """Get all supported types for a database"""
        database = database.lower()
        if database == "mysql":
            return dict(self.mysql_to_pg)
        elif database == "postgresql":
            return dict(self.pg_to_mysql)
        else:
            raise ValueError(f"Unsupported database: {database}")


# Convenience functions
def translate_type(type_str: str, from_db: str, to_db: str) -> str:
    """
    Translate a database type from one system to another

    Args:
        type_str: The type string to translate
        from_db: Source database ('mysql' or 'postgresql')
        to_db: Target database ('mysql' or 'postgresql')

    Returns:
        Translated type string
    """
    translator = DatabaseTypeTranslator()
    return translator.translate(type_str, from_db, to_db)


def mysql_to_postgresql(type_str: str) -> str:
    """Convert MySQL type to PostgreSQL"""
    return translate_type(type_str, "mysql", "postgresql")


def postgresql_to_mysql(type_str: str) -> str:
    """Convert PostgreSQL type to MySQL"""
    return translate_type(type_str, "postgresql", "mysql")


# Example usage
if __name__ == "__main__":
    translator = DatabaseTypeTranslator()

    # MySQL to PostgreSQL examples
    print("MySQL to PostgreSQL:")
    mysql_types = [
        "INT UNSIGNED",
        "VARCHAR(255)",
        "TEXT",
        "DATETIME",
        "DECIMAL(10,2)",
        "BIGINT AUTO_INCREMENT",
        "ENUM('a','b','c')",
        "JSON",
        "TINYINT(1)",
    ]

    for mysql_type in mysql_types:
        pg_type = translator.translate(mysql_type, "mysql", "postgresql")
        print(f"  {mysql_type} -> {pg_type}")

    print("\nPostgreSQL to MySQL:")
    pg_types = [
        "SERIAL",
        "VARCHAR(255)",
        "TEXT",
        "TIMESTAMP",
        "NUMERIC(10,2)",
        "BOOLEAN",
        "JSON",
        "UUID",
        "INTEGER[]",
    ]

    for pg_type in pg_types:
        mysql_type = translator.translate(pg_type, "postgresql", "mysql")
        print(f"  {pg_type} -> {mysql_type}")
