"""Enhanced Aurora CDC DataSource for Spark 4 with real-time streaming support."""

from typing import Optional, Dict, Any, List, Iterator, Tuple
from dataclasses import dataclass
import json
import time
from datetime import datetime, timedelta
from pyspark.sql.datasource import (
    DataSource, 
    DataSourceReader, 
    SimpleDataSourceStreamReader
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    LongType, BooleanType, MapType
)
from pyspark.sql import Row


@dataclass
class CDCOffset:
    """Represents CDC offset for checkpointing."""
    binlog_file: str
    binlog_position: int
    timestamp: str
    table_offsets: Dict[str, Dict[str, Any]]
    
    def to_json(self) -> str:
        """Convert offset to JSON string."""
        return json.dumps({
            "binlog_file": self.binlog_file,
            "binlog_position": self.binlog_position,
            "timestamp": self.timestamp,
            "table_offsets": self.table_offsets
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'CDCOffset':
        """Create offset from JSON string."""
        data = json.loads(json_str)
        return cls(**data)


class AuroraCDCDataSourceV2(DataSource):
    """
    Enhanced CDC DataSource for Aurora MySQL supporting 500+ tables.
    Uses Spark 4's SimpleDataSourceStreamReader for efficient streaming.
    """
    
    @classmethod
    def name(cls) -> str:
        """Return the short name of the data source."""
        return "aurora_cdc_v2"
    
    def schema(self) -> StructType:
        """Define the unified schema for CDC events across all tables."""
        return StructType([
            # Metadata fields
            StructField("database", StringType(), False),
            StructField("table", StringType(), False),
            StructField("operation", StringType(), False),  # INSERT, UPDATE, DELETE, TRUNCATE
            StructField("timestamp", TimestampType(), False),
            StructField("event_time", TimestampType(), False),  # For watermarking
            
            # Binlog position for exactly-once processing
            StructField("binlog_file", StringType(), True),
            StructField("binlog_position", LongType(), True),
            StructField("gtid", StringType(), True),  # Global Transaction ID
            StructField("server_id", LongType(), True),
            StructField("transaction_id", StringType(), True),
            
            # Data fields
            StructField("before", MapType(StringType(), StringType()), True),  # Map for flexibility
            StructField("after", MapType(StringType(), StringType()), True),
            StructField("primary_keys", MapType(StringType(), StringType()), True),
            
            # Schema evolution support
            StructField("schema_version", StringType(), True),
            StructField("ddl_statement", StringType(), True),  # For schema changes
            
            # Performance tracking
            StructField("processing_time", TimestampType(), True),
            StructField("batch_id", StringType(), True),
            StructField("partition_id", LongType(), True),
            
            # Data quality
            StructField("is_snapshot", BooleanType(), True),  # Initial load vs CDC
            StructField("error_flag", BooleanType(), True),
            StructField("error_message", StringType(), True)
        ])
    
    def reader(self, schema: StructType) -> DataSourceReader:
        """Create a stream reader for CDC data."""
        return AuroraCDCStreamReaderV2(
            schema=schema,
            options=self.options
        )
    
    @property
    def options(self) -> Dict[str, Any]:
        """Get data source options."""
        return getattr(self, '_options', {})
    
    @options.setter
    def options(self, value: Dict[str, Any]):
        """Set and validate options."""
        self._options = value
        self._validate_options()
    
    def _validate_options(self):
        """Validate required options for 500+ table CDC."""
        required = ["host", "port", "database", "username", "password"]
        for opt in required:
            if opt not in self._options:
                raise ValueError(f"Required option '{opt}' is missing")
        
        # Validate numeric options
        try:
            int(self._options["port"])
            int(self._options.get("parallelism", "10"))
            int(self._options.get("batch_size", "10000"))
            int(self._options.get("max_tables_per_batch", "50"))
        except ValueError as e:
            raise ValueError(f"Invalid numeric option: {e}")


class AuroraCDCStreamReaderV2(SimpleDataSourceStreamReader):
    """
    Enhanced stream reader using SimpleDataSourceStreamReader for Spark 4.
    Handles 500+ tables with parallel processing and checkpointing.
    """
    
    def __init__(self, schema: StructType, options: Dict[str, Any]):
        """Initialize the enhanced stream reader."""
        self.schema = schema
        self.options = options
        
        # Connection configuration
        self.host = options["host"]
        self.port = int(options["port"])
        self.database = options["database"]
        self.username = options["username"]
        self.password = options["password"]
        
        # Table configuration for 500+ tables
        self.table_pattern = options.get("table_pattern", "*")  # Regex or wildcard
        self.excluded_tables = set(options.get("excluded_tables", "").split(","))
        self.included_tables = options.get("included_tables", "").split(",") if options.get("included_tables") else None
        
        # Performance configuration
        self.parallelism = int(options.get("parallelism", "10"))
        self.batch_size = int(options.get("batch_size", "10000"))
        self.max_tables_per_batch = int(options.get("max_tables_per_batch", "50"))
        self.fetch_interval_seconds = int(options.get("fetch_interval_seconds", "900"))  # 15 minutes
        
        # Checkpointing configuration
        self.checkpoint_location = options.get("checkpoint_location", "/tmp/aurora_cdc_checkpoint")
        self.enable_exactly_once = options.get("enable_exactly_once", "true").lower() == "true"
        
        # Real-time mode configuration
        self.real_time_mode = options.get("real_time_mode", "true").lower() == "true"
        self.max_trigger_delay = int(options.get("max_trigger_delay_ms", "100"))  # milliseconds
        
        # State management
        self.current_offset = None
        self.table_registry = {}
        self.last_fetch_time = {}
        
        # Initialize connection pool and table registry
        self._initialize_connections()
        self._discover_tables()
    
    def _initialize_connections(self):
        """Initialize connection pool for parallel processing."""
        from .connection_pool_v2 import EnhancedConnectionPool
        self.connection_pool = EnhancedConnectionPool(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            max_connections=self.parallelism * 2,
            enable_binlog_client=True
        )
    
    def _discover_tables(self):
        """Discover and register all tables for CDC."""
        import re
        
        with self.connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get all tables in the database
            cursor.execute("""
                SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, UPDATE_TIME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_ROWS DESC
            """, (self.database,))
            
            all_tables = cursor.fetchall()
            
            # Filter tables based on pattern and exclusions
            pattern = re.compile(self.table_pattern.replace("*", ".*"))
            
            for table_name, row_count, data_size, update_time in all_tables:
                # Skip excluded tables
                if table_name in self.excluded_tables:
                    continue
                
                # Check included tables or pattern
                if self.included_tables:
                    if table_name not in self.included_tables:
                        continue
                elif not pattern.match(table_name):
                    continue
                
                # Register table with metadata
                self.table_registry[table_name] = {
                    "row_count": row_count or 0,
                    "data_size": data_size or 0,
                    "last_update": update_time,
                    "priority": self._calculate_table_priority(row_count, data_size, update_time),
                    "last_processed": None,
                    "current_offset": None
                }
            
            print(f"Discovered {len(self.table_registry)} tables for CDC")
    
    def _calculate_table_priority(self, row_count: int, data_size: int, update_time: datetime) -> int:
        """Calculate processing priority for a table."""
        # Higher priority for frequently updated and larger tables
        priority = 0
        
        if row_count:
            priority += min(row_count // 10000, 100)  # Cap at 100
        
        if update_time:
            hours_since_update = (datetime.now() - update_time).total_seconds() / 3600
            if hours_since_update < 1:
                priority += 50
            elif hours_since_update < 24:
                priority += 20
        
        return priority
    
    def initialOffset(self) -> str:
        """Get initial offset for the stream."""
        if self.options.get("start_position") == "earliest":
            # Start from beginning of available binlog
            offset = CDCOffset(
                binlog_file="",
                binlog_position=0,
                timestamp=datetime.min.isoformat(),
                table_offsets={}
            )
        else:
            # Start from current position
            with self.connection_pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW MASTER STATUS")
                file, position, _, _, _ = cursor.fetchone()
                
                offset = CDCOffset(
                    binlog_file=file,
                    binlog_position=position,
                    timestamp=datetime.now().isoformat(),
                    table_offsets={
                        table: {"last_updated": None, "last_id": 0}
                        for table in self.table_registry
                    }
                )
        
        self.current_offset = offset
        return offset.to_json()
    
    def latestOffset(self) -> str:
        """Get latest available offset."""
        with self.connection_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW MASTER STATUS")
            file, position, _, _, _ = cursor.fetchone()
            
            # Get latest position for each table
            table_offsets = {}
            for table_name in self.table_registry:
                cursor.execute(f"""
                    SELECT MAX(updated_at) as last_update,
                           COUNT(*) as row_count
                    FROM {table_name}
                    WHERE updated_at > %s
                """, (self.current_offset.timestamp if self.current_offset else datetime.min,))
                
                result = cursor.fetchone()
                table_offsets[table_name] = {
                    "last_updated": result[0].isoformat() if result[0] else None,
                    "row_count": result[1] if result[1] else 0
                }
            
            offset = CDCOffset(
                binlog_file=file,
                binlog_position=position,
                timestamp=datetime.now().isoformat(),
                table_offsets=table_offsets
            )
            
            return offset.to_json()
    
    def read(self, start: str, end: str) -> Iterator[Tuple]:
        """
        Read CDC data between start and end offsets.
        Returns iterator of tuples matching the schema.
        """
        start_offset = CDCOffset.from_json(start)
        end_offset = CDCOffset.from_json(end)
        
        # Determine tables to process in this micro-batch
        tables_to_process = self._select_tables_for_batch(start_offset, end_offset)
        
        # Process tables in parallel batches
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = []
            
            for table_batch in self._batch_tables(tables_to_process, self.max_tables_per_batch):
                future = executor.submit(
                    self._process_table_batch,
                    table_batch,
                    start_offset,
                    end_offset
                )
                futures.append(future)
            
            # Yield results as they complete
            for future in as_completed(futures):
                try:
                    for row in future.result():
                        yield self._row_to_tuple(row)
                except Exception as e:
                    # Log error and continue
                    print(f"Error processing batch: {e}")
                    # Yield error row
                    yield self._create_error_row(str(e))
        
        # Update current offset
        self.current_offset = end_offset
    
    def _select_tables_for_batch(self, start_offset: CDCOffset, end_offset: CDCOffset) -> List[str]:
        """Select tables to process in this batch based on priority and changes."""
        tables_with_changes = []
        
        for table_name, end_info in end_offset.table_offsets.items():
            if table_name not in self.table_registry:
                continue
            
            start_info = start_offset.table_offsets.get(table_name, {})
            
            # Check if table has changes
            if end_info.get("row_count", 0) > 0 or end_info.get("last_updated") != start_info.get("last_updated"):
                priority = self.table_registry[table_name]["priority"]
                tables_with_changes.append((priority, table_name))
        
        # Sort by priority (descending) and return table names
        tables_with_changes.sort(reverse=True)
        return [table for _, table in tables_with_changes]
    
    def _batch_tables(self, tables: List[str], batch_size: int) -> Iterator[List[str]]:
        """Batch tables for parallel processing."""
        for i in range(0, len(tables), batch_size):
            yield tables[i:i + batch_size]
    
    def _process_table_batch(
        self, 
        tables: List[str], 
        start_offset: CDCOffset, 
        end_offset: CDCOffset
    ) -> List[Row]:
        """Process a batch of tables and return CDC events."""
        rows = []
        batch_id = f"{datetime.now().isoformat()}_{hash(tuple(tables))}"
        
        with self.connection_pool.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            for table_name in tables:
                try:
                    # Get table schema for proper type conversion
                    cursor.execute(f"""
                        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION
                    """, (self.database, table_name))
                    
                    columns = cursor.fetchall()
                    primary_keys = [col['COLUMN_NAME'] for col in columns if col['COLUMN_KEY'] == 'PRI']
                    
                    # Query for changes (simplified CDC using timestamps)
                    start_time = start_offset.table_offsets.get(table_name, {}).get("last_updated", start_offset.timestamp)
                    end_time = end_offset.timestamp
                    
                    query = f"""
                        SELECT *, 
                               'UPDATE' as _cdc_operation,
                               updated_at as _cdc_timestamp
                        FROM {table_name}
                        WHERE updated_at > %s AND updated_at <= %s
                        ORDER BY updated_at
                        LIMIT %s
                    """
                    
                    cursor.execute(query, (start_time, end_time, self.batch_size))
                    
                    for record in cursor:
                        # Remove internal fields
                        operation = record.pop('_cdc_operation', 'UPDATE')
                        timestamp = record.pop('_cdc_timestamp', datetime.now())
                        
                        # Create CDC event row
                        row = Row(
                            database=self.database,
                            table=table_name,
                            operation=operation,
                            timestamp=timestamp,
                            event_time=timestamp,
                            binlog_file=end_offset.binlog_file,
                            binlog_position=end_offset.binlog_position,
                            gtid=None,
                            server_id=None,
                            transaction_id=None,
                            before=None if operation == 'INSERT' else {k: str(v) for k, v in record.items()},
                            after={k: str(v) for k, v in record.items()},
                            primary_keys={pk: str(record.get(pk)) for pk in primary_keys},
                            schema_version="1.0",
                            ddl_statement=None,
                            processing_time=datetime.now(),
                            batch_id=batch_id,
                            partition_id=hash(table_name) % self.parallelism,
                            is_snapshot=False,
                            error_flag=False,
                            error_message=None
                        )
                        rows.append(row)
                    
                    # Update table registry
                    self.table_registry[table_name]["last_processed"] = datetime.now()
                    
                except Exception as e:
                    print(f"Error processing table {table_name}: {e}")
                    # Add error row for this table
                    rows.append(self._create_error_row(f"Table {table_name}: {str(e)}"))
        
        return rows
    
    def _row_to_tuple(self, row: Row) -> Tuple:
        """Convert Row to tuple matching schema order."""
        return (
            row.database, row.table, row.operation, row.timestamp, row.event_time,
            row.binlog_file, row.binlog_position, row.gtid, row.server_id, row.transaction_id,
            row.before, row.after, row.primary_keys,
            row.schema_version, row.ddl_statement,
            row.processing_time, row.batch_id, row.partition_id,
            row.is_snapshot, row.error_flag, row.error_message
        )
    
    def _create_error_row(self, error_message: str) -> Tuple:
        """Create an error row tuple."""
        return (
            self.database, None, "ERROR", datetime.now(), datetime.now(),
            None, None, None, None, None,
            None, None, None,
            None, None,
            datetime.now(), None, None,
            False, True, error_message
        )
    
    def commit(self, end: str):
        """Commit the processed offset for checkpointing."""
        # Store checkpoint for recovery
        import os
        os.makedirs(self.checkpoint_location, exist_ok=True)
        
        checkpoint_file = os.path.join(self.checkpoint_location, "offset.json")
        with open(checkpoint_file, "w") as f:
            f.write(end)
        
        print(f"Committed offset: {end[:100]}...")
    
    def stop(self):
        """Clean up resources."""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.close()