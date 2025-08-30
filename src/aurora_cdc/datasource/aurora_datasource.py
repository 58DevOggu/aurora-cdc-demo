"""Aurora CDC DataSource for Spark Structured Streaming."""

from typing import Optional, Dict, Any, List
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from .stream_reader import AuroraCDCStreamReader


class AuroraCDCDataSource(DataSource):
    """Custom DataSource for reading CDC events from Aurora MySQL."""

    @classmethod
    def name(cls) -> str:
        """Return the name of the data source."""
        return "aurora_cdc"

    def schema(self) -> StructType:
        """Define the schema for CDC events."""
        return StructType([
            StructField("database", StringType(), False),
            StructField("table", StringType(), False),
            StructField("operation", StringType(), False),  # INSERT, UPDATE, DELETE
            StructField("timestamp", TimestampType(), False),
            StructField("binlog_file", StringType(), True),
            StructField("binlog_position", LongType(), True),
            StructField("server_id", LongType(), True),
            StructField("before", StringType(), True),  # JSON string
            StructField("after", StringType(), True),   # JSON string
            StructField("transaction_id", StringType(), True),
        ])

    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a reader for the data source."""
        return AuroraCDCStreamReader(
            schema=schema,
            options=self.options
        )

    def validate_options(self) -> None:
        """Validate required options."""
        required_options = [
            "host",
            "port",
            "database",
            "username",
            "password"
        ]
        
        for option in required_options:
            if option not in self.options:
                raise ValueError(f"Required option '{option}' is missing")
        
        # Validate port is a number
        try:
            int(self.options["port"])
        except ValueError:
            raise ValueError("Option 'port' must be a valid integer")

    @property
    def options(self) -> Dict[str, Any]:
        """Get the options passed to the data source."""
        return self._options if hasattr(self, '_options') else {}
    
    @options.setter
    def options(self, value: Dict[str, Any]) -> None:
        """Set the options for the data source."""
        self._options = value
        self.validate_options()