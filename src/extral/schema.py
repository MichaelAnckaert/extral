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
from typing import Iterable, Optional, TypedDict


class SchemaCreateException(Exception):
    """Custom exception for schema creation errors."""

    pass


DatabaseSchema = TypedDict(
    "DatabaseSchema",
    {
        "schema_source": str,
        "schema": dict[str, dict[str, str]],
    },
)

TargetDatabaseSchema = TypedDict(
    "TargetDatabaseSchema",
    {
        "schema_source": str,
        "schema": dict[str, dict[str, str | bool]],
    },
)


def infer_schema(data: Iterable[dict[str, Optional[str]]]) -> TargetDatabaseSchema:
    """Infer database schema based on the provided data."""
    schema: dict[str, dict[str, str | bool]] = {}
    for record in data:
        for key, value in record.items():
            if key not in schema:
                schema[key] = {"type": "", "nullable": False}
            if value is None:
                schema[key]["nullable"] = True
            else:
                inferred_type = type(value).__name__
                if schema[key]["type"] == "":
                    schema[key]["type"] = inferred_type
                elif schema[key]["type"] != inferred_type:
                    raise SchemaCreateException(
                        f"Type mismatch for key '{key}': {schema[key]['type']} vs {inferred_type}"
                    )
    return {"schema_source": "inferred", "schema": schema}
