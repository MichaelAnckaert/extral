"""CSV file connector implementation."""

import csv
import logging
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from extral.config import ExtractConfig, FileConfig, LoadConfig, LoadStrategy
from extral.connectors.file.base import FileConnector
from extral.connectors.file.utils import get_file_handle
from extral.database import DatabaseRecord

logger = logging.getLogger(__name__)


class CSVConnector(FileConnector):
    """
    CSV file connector for reading and writing CSV files.
    
    Supports both local files and HTTP/HTTPS URLs for extraction.
    """
    
    def __init__(self, config: FileConfig):
        """
        Initialize CSV connector.
        
        Args:
            config: FileConfig with CSV-specific options:
                - delimiter: Field delimiter (default: ',')
                - quotechar: Quote character (default: '"')
                - header: Comma-separated header string if file has no header
        """
        super().__init__(config)
        
        # Extract CSV-specific options
        self.delimiter = self.config.options.get("delimiter", ",")
        self.quotechar = self.config.options.get("quotechar", '"')
        self.header = self.config.options.get("header")
        
        # Parse header if provided
        self.header_fields: Optional[List[str]] = None
        if self.header:
            self.header_fields = [field.strip() for field in self.header.split(",")]
    
    def extract_data(
        self,
        dataset_name: str,
        extract_config: ExtractConfig,
    ) -> Generator[list[DatabaseRecord], None, None]:
        """
        Extract data from CSV file.
        
        Args:
            dataset_name: File path or identifier
            extract_config: Extraction configuration
            
        Yields:
            Batches of database records
        """
        path = self.get_effective_path(dataset_name)
        batch_size = extract_config.batch_size or 50000
        
        logger.info(f"Extracting data from CSV file: {path}")
        
        with get_file_handle(path, "r") as file_path:
            with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                # Detect if file has header or use provided header
                if self.header_fields:
                    # Use provided header
                    reader = csv.DictReader(
                        csvfile,
                        fieldnames=self.header_fields,
                        delimiter=self.delimiter,
                        quotechar=self.quotechar
                    )
                else:
                    # Assume first row is header
                    reader = csv.DictReader(
                        csvfile,
                        delimiter=self.delimiter,
                        quotechar=self.quotechar
                    )
                
                batch: list[DatabaseRecord] = []
                row_count = 0
                
                for row in reader:
                    # Convert row to DatabaseRecord format (all values as strings)
                    record: DatabaseRecord = {}
                    for key, value in row.items():
                        # Handle None/empty values
                        record[key] = value if value != "" else None
                    
                    batch.append(record)
                    row_count += 1
                    
                    if len(batch) >= batch_size:
                        logger.debug(f"Yielding batch of {len(batch)} records")
                        yield batch
                        batch = []
                
                # Yield remaining records
                if batch:
                    logger.debug(f"Yielding final batch of {len(batch)} records")
                    yield batch
                
                logger.info(f"Extracted {row_count} records from CSV file")
    
    def load_data(
        self,
        dataset_name: str,
        data: list[DatabaseRecord],
        load_config: LoadConfig,
    ) -> None:
        """
        Load data to CSV file.
        
        Args:
            dataset_name: Target file path
            data: Records to write
            load_config: Load configuration (strategy: REPLACE or APPEND)
        """
        if self.config.http_path:
            raise ValueError("Cannot write to HTTP/HTTPS URLs")
        
        path = self.get_effective_path(dataset_name)
        strategy = load_config.strategy
        
        if strategy not in [LoadStrategy.REPLACE, LoadStrategy.APPEND]:
            raise ValueError(f"CSV connector only supports REPLACE and APPEND strategies, got {strategy}")
        
        logger.info(f"Loading {len(data)} records to CSV file: {path} (strategy: {strategy.value})")
        
        # Determine fieldnames from data
        if not data:
            logger.warning("No data to write")
            return
        
        fieldnames = list(data[0].keys())
        
        # Determine write mode based on strategy
        file_path = Path(path)
        write_header = True
        
        if strategy == LoadStrategy.APPEND and file_path.exists():
            # Check if file exists and has content
            if file_path.stat().st_size > 0:
                write_header = False
        
        mode = "w" if strategy == LoadStrategy.REPLACE else "a"
        
        with open(file_path, mode, newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=fieldnames,
                delimiter=self.delimiter,
                quotechar=self.quotechar
            )
            
            if write_header:
                writer.writeheader()
            
            for record in data:
                # Convert None to empty string for CSV
                csv_record = {k: (v if v is not None else "") for k, v in record.items()}
                writer.writerow(csv_record)
        
        logger.info(f"Successfully wrote {len(data)} records to CSV file")
    
    def infer_schema(self, dataset_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Infer schema from CSV file.
        
        For CSV files, all columns are treated as VARCHAR since CSV doesn't
        have type information.
        
        Args:
            dataset_name: File path
            
        Returns:
            Schema dictionary with column names and types
        """
        path = self.get_effective_path(dataset_name)
        
        with get_file_handle(path, "r") as file_path:
            with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
                # Read first few rows to determine columns
                if self.header_fields:
                    fieldnames = self.header_fields
                else:
                    # Use csv.DictReader to get fieldnames
                    reader = csv.DictReader(
                        csvfile,
                        delimiter=self.delimiter,
                        quotechar=self.quotechar
                    )
                    fieldnames = reader.fieldnames or []
        
        # Build schema - all columns as VARCHAR(255) by default
        schema = {}
        for field in fieldnames:
            schema[field] = {
                "type": "VARCHAR(255)",
                "nullable": True
            }
        
        return schema