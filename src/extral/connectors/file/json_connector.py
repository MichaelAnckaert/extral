"""JSON file connector implementation."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Generator, List

from extral.config import ExtractConfig, FileItemConfig, LoadConfig, LoadStrategy
from extral.connectors.file.base import FileConnector
from extral.connectors.file.utils import get_file_handle
from extral.database import DatabaseRecord

logger = logging.getLogger(__name__)


class JSONConnector(FileConnector):
    """
    JSON file connector for reading and writing JSON files.

    Supports both JSON array format and JSON Lines format.
    Supports both local files and HTTP/HTTPS URLs for extraction.
    """

    def __init__(self, config: FileItemConfig):
        """
        Initialize JSON connector.

        Args:
            config: FileItemConfig with JSON-specific options
        """
        super().__init__(config)

        # JSON format: "array" (default) or "jsonl" (JSON Lines)
        self.json_format = self.config.options.get("format", "array")

    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """
        Extract data from JSON file.

        Args:
            dataset_name: File path or identifier
            extract_config: Extraction configuration

        Yields:
            Batches of database records
        """
        path = self.get_effective_path(dataset_name)
        batch_size = extract_config.batch_size or 50000

        logger.info(
            f"Extracting data from JSON file: {path} (format: {self.json_format})"
        )

        with get_file_handle(path, "r") as file_path:
            if self.json_format == "jsonl":
                # JSON Lines format - one JSON object per line
                batch: list[DatabaseRecord] = []
                row_count = 0

                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            record = json.loads(line)
                            # Convert to DatabaseRecord format (all values as strings)
                            db_record = self._convert_to_database_record(record)
                            batch.append(db_record)
                            row_count += 1

                            if len(batch) >= batch_size:
                                logger.debug(f"Yielding batch of {len(batch)} records")
                                yield batch
                                batch = []
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing JSON line: {e}")
                            raise

                # Yield remaining records
                if batch:
                    logger.debug(f"Yielding final batch of {len(batch)} records")
                    yield batch

                logger.info(f"Extracted {row_count} records from JSON Lines file")

            else:
                # Array format - entire file is a JSON array
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON file: {e}")
                        raise

                if not isinstance(data, list):
                    raise ValueError(f"Expected JSON array, got {type(data).__name__}")

                data_batch: list[DatabaseRecord] = []
                row_count = 0

                for record in data:
                    db_record = self._convert_to_database_record(record)
                    data_batch.append(db_record)
                    row_count += 1

                    if len(data_batch) >= batch_size:
                        logger.debug(f"Yielding batch of {len(data_batch)} records")
                        yield data_batch
                        data_batch = []

                # Yield remaining records
                if data_batch:
                    logger.debug(f"Yielding final batch of {len(data_batch)} records")
                    yield data_batch

                logger.info(f"Extracted {row_count} records from JSON array file")

    def load_data(
        self,
        dataset_name: str,
        data: list[DatabaseRecord],
        load_config: LoadConfig,
    ) -> None:
        """
        Load data to JSON file.

        Args:
            dataset_name: Target file path
            data: Records to write
            load_config: Load configuration (only REPLACE strategy supported)
        """
        if self.config.http_path:
            raise ValueError("Cannot write to HTTP/HTTPS URLs")

        path = self.get_effective_path(dataset_name)
        strategy = load_config.strategy

        if strategy != LoadStrategy.REPLACE:
            raise ValueError(
                f"JSON connector only supports REPLACE strategy, got {strategy}"
            )

        logger.info(
            f"Loading {len(data)} records to JSON file: {path} (format: {self.json_format})"
        )

        file_path = Path(path)

        if self.json_format == "jsonl":
            # JSON Lines format
            with open(file_path, "w", encoding="utf-8") as f:
                for record in data:
                    # Convert back from DatabaseRecord format
                    json_record = self._convert_from_database_record(record)
                    f.write(json.dumps(json_record) + "\n")
        else:
            # Array format
            records = [self._convert_from_database_record(record) for record in data]
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)

        logger.info(f"Successfully wrote {len(data)} records to JSON file")

    def infer_schema(self, dataset_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Infer schema from JSON file by sampling records.

        Args:
            dataset_name: File path

        Returns:
            Schema dictionary with column names and types
        """
        path = self.get_effective_path(dataset_name)
        schema: Dict[str, Dict[str, Any]] = {}

        with get_file_handle(path, "r") as file_path:
            sample_records: List[Dict[str, Any]] = []

            if self.json_format == "jsonl":
                # Read first few lines
                with open(file_path, "r", encoding="utf-8") as f:
                    for i, line in enumerate(f):
                        if i >= 100:  # Sample first 100 records
                            break
                        line = line.strip()
                        if line:
                            sample_records.append(json.loads(line))
            else:
                # Read entire array (or first part if large)
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        sample_records = data[:100]  # Sample first 100 records

        # Infer schema from sample records
        for record in sample_records:
            for key, value in record.items():
                if key not in schema:
                    schema[key] = {"type": self._infer_type(value), "nullable": False}
                elif value is None:
                    schema[key]["nullable"] = True

        return schema

    def _convert_to_database_record(self, record: Dict[str, Any]) -> DatabaseRecord:
        """Convert JSON record to DatabaseRecord format (all values as strings)."""
        db_record: DatabaseRecord = {}
        for key, value in record.items():
            if value is None:
                db_record[key] = None
            elif isinstance(value, (dict, list)):
                # Convert complex types to JSON strings
                db_record[key] = json.dumps(value)
            else:
                # Convert primitive types to strings
                db_record[key] = str(value)
        return db_record

    def _convert_from_database_record(self, record: DatabaseRecord) -> Dict[str, Any]:
        """Convert DatabaseRecord back to JSON-compatible format."""
        json_record: Dict[str, Any] = {}
        for key, value in record.items():
            if value is None:
                json_record[key] = None
            else:
                # Try to parse as JSON first (for nested objects/arrays)
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, (dict, list)):
                        json_record[key] = parsed
                    else:
                        json_record[key] = value
                except (json.JSONDecodeError, TypeError):
                    # Keep as string if not valid JSON
                    json_record[key] = value
        return json_record

    def _infer_type(self, value: Any) -> str:
        """Infer SQL type from Python value."""
        if value is None:
            return "VARCHAR(255)"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            # Check if it's a large integer
            if abs(value) > 2147483647:
                return "BIGINT"
            else:
                return "INTEGER"
        elif isinstance(value, float):
            return "DOUBLE PRECISION"
        elif isinstance(value, str):
            # Check string length
            if len(value) > 255:
                return "TEXT"
            else:
                return "VARCHAR(255)"
        elif isinstance(value, (dict, list)):
            return "JSON"
        else:
            return "VARCHAR(255)"
