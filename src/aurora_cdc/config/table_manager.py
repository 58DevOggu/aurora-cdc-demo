"""Table management for 500+ table CDC operations with intelligent batching."""

import json
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import heapq
import threading
from pathlib import Path


@dataclass
class TableMetadata:
    """Metadata for a single table."""
    name: str
    schema: str
    row_count: int
    data_size_mb: float
    avg_row_size: float
    primary_keys: List[str]
    update_frequency: float  # Updates per hour
    last_updated: Optional[datetime]
    last_processed: Optional[datetime]
    processing_priority: int
    partition_key: Optional[str]
    cdc_enabled: bool = True
    error_count: int = 0
    last_error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        d = asdict(self)
        d['last_updated'] = self.last_updated.isoformat() if self.last_updated else None
        d['last_processed'] = self.last_processed.isoformat() if self.last_processed else None
        return d
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'TableMetadata':
        """Create from dictionary."""
        if d.get('last_updated'):
            d['last_updated'] = datetime.fromisoformat(d['last_updated'])
        if d.get('last_processed'):
            d['last_processed'] = datetime.fromisoformat(d['last_processed'])
        return cls(**d)


class TableManager:
    """
    Manages 500+ tables for CDC operations with intelligent scheduling.
    """
    
    def __init__(
        self,
        metadata_path: Optional[str] = None,
        max_tables_per_batch: int = 50,
        min_batch_interval_seconds: int = 900,  # 15 minutes
        enable_auto_discovery: bool = True
    ):
        """Initialize table manager."""
        self.metadata_path = Path(metadata_path) if metadata_path else Path("/tmp/aurora_cdc/table_metadata")
        self.max_tables_per_batch = max_tables_per_batch
        self.min_batch_interval = timedelta(seconds=min_batch_interval_seconds)
        self.enable_auto_discovery = enable_auto_discovery
        
        # Table registry
        self.tables: Dict[str, TableMetadata] = {}
        self.table_groups: Dict[str, List[str]] = {}  # Logical groupings
        
        # Processing state
        self.processing_queue: List[Tuple[int, str]] = []  # Priority queue
        self.active_tables: Set[str] = set()
        self.failed_tables: Set[str] = set()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Statistics
        self.stats = {
            "total_tables": 0,
            "active_tables": 0,
            "failed_tables": 0,
            "total_rows_processed": 0,
            "total_batches": 0,
            "avg_batch_time": 0
        }
        
        # Load existing metadata
        self._load_metadata()
    
    def register_table(self, table: TableMetadata):
        """Register a table for CDC."""
        with self._lock:
            self.tables[table.name] = table
            self._update_priority(table.name)
            self.stats["total_tables"] = len(self.tables)
    
    def register_tables_from_database(self, connection, schema: str = None):
        """Auto-discover and register tables from database."""
        cursor = connection.cursor()
        
        # Get table information
        query = """
            SELECT 
                t.TABLE_NAME,
                t.TABLE_SCHEMA,
                t.TABLE_ROWS,
                t.DATA_LENGTH / 1024 / 1024 as DATA_SIZE_MB,
                t.AVG_ROW_LENGTH,
                t.UPDATE_TIME,
                GROUP_CONCAT(
                    CASE WHEN c.COLUMN_KEY = 'PRI' THEN c.COLUMN_NAME END
                ) as PRIMARY_KEYS,
                GROUP_CONCAT(
                    CASE WHEN c.EXTRA LIKE '%auto_increment%' THEN c.COLUMN_NAME END
                ) as AUTO_INCREMENT_COL
            FROM information_schema.TABLES t
            LEFT JOIN information_schema.COLUMNS c 
                ON t.TABLE_SCHEMA = c.TABLE_SCHEMA 
                AND t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE = 'BASE TABLE'
                AND t.TABLE_SCHEMA = COALESCE(%s, t.TABLE_SCHEMA)
                AND t.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
            GROUP BY t.TABLE_NAME, t.TABLE_SCHEMA
            ORDER BY t.TABLE_ROWS DESC
        """
        
        cursor.execute(query, (schema,))
        tables = cursor.fetchall()
        
        for row in tables:
            table_name, table_schema, row_count, data_size_mb, avg_row_size, update_time, primary_keys, auto_inc = row
            
            # Calculate update frequency (simplified - in production, analyze binlog)
            update_frequency = self._estimate_update_frequency(update_time)
            
            # Create table metadata
            metadata = TableMetadata(
                name=table_name,
                schema=table_schema,
                row_count=row_count or 0,
                data_size_mb=float(data_size_mb or 0),
                avg_row_size=float(avg_row_size or 0),
                primary_keys=primary_keys.split(',') if primary_keys else [],
                update_frequency=update_frequency,
                last_updated=update_time,
                last_processed=None,
                processing_priority=0,
                partition_key=auto_inc,
                cdc_enabled=True
            )
            
            self.register_table(metadata)
        
        cursor.close()
        print(f"Registered {len(self.tables)} tables for CDC")
    
    def _estimate_update_frequency(self, last_update: Optional[datetime]) -> float:
        """Estimate table update frequency."""
        if not last_update:
            return 0.0
        
        hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
        if hours_since_update < 1:
            return 100.0  # Very frequent
        elif hours_since_update < 24:
            return 10.0   # Daily updates
        elif hours_since_update < 168:
            return 1.0    # Weekly updates
        else:
            return 0.1    # Rare updates
    
    def _update_priority(self, table_name: str):
        """Update processing priority for a table."""
        table = self.tables.get(table_name)
        if not table:
            return
        
        # Priority calculation factors
        priority = 0
        
        # Factor 1: Update frequency (40% weight)
        priority += int(table.update_frequency * 40)
        
        # Factor 2: Data size (20% weight)
        if table.data_size_mb > 1000:
            priority += 20
        elif table.data_size_mb > 100:
            priority += 15
        elif table.data_size_mb > 10:
            priority += 10
        else:
            priority += 5
        
        # Factor 3: Time since last processed (30% weight)
        if table.last_processed:
            hours_since_processed = (datetime.now() - table.last_processed).total_seconds() / 3600
            priority += min(30, int(hours_since_processed * 2))
        else:
            priority += 30  # Never processed
        
        # Factor 4: Error penalty (10% weight)
        priority -= min(10, table.error_count * 2)
        
        table.processing_priority = max(0, priority)
    
    def get_next_batch(self, max_tables: Optional[int] = None) -> List[TableMetadata]:
        """Get next batch of tables to process."""
        with self._lock:
            max_tables = max_tables or self.max_tables_per_batch
            batch = []
            
            # Update priorities
            for table_name in self.tables:
                self._update_priority(table_name)
            
            # Build priority queue
            priority_queue = []
            for table_name, table in self.tables.items():
                if table.cdc_enabled and table_name not in self.active_tables:
                    # Check if enough time has passed since last processing
                    if table.last_processed:
                        time_since_last = datetime.now() - table.last_processed
                        if time_since_last < self.min_batch_interval:
                            continue
                    
                    # Add to priority queue (negative priority for max heap)
                    heapq.heappush(priority_queue, (-table.processing_priority, table_name))
            
            # Select top priority tables
            while priority_queue and len(batch) < max_tables:
                _, table_name = heapq.heappop(priority_queue)
                table = self.tables[table_name]
                batch.append(table)
                self.active_tables.add(table_name)
            
            self.stats["active_tables"] = len(self.active_tables)
            self.stats["total_batches"] += 1
            
            return batch
    
    def mark_processed(self, table_name: str, success: bool = True, error: Optional[str] = None):
        """Mark a table as processed."""
        with self._lock:
            if table_name in self.tables:
                table = self.tables[table_name]
                table.last_processed = datetime.now()
                
                if success:
                    table.error_count = 0
                    table.last_error = None
                    if table_name in self.failed_tables:
                        self.failed_tables.remove(table_name)
                else:
                    table.error_count += 1
                    table.last_error = error
                    self.failed_tables.add(table_name)
                
                if table_name in self.active_tables:
                    self.active_tables.remove(table_name)
                
                self.stats["active_tables"] = len(self.active_tables)
                self.stats["failed_tables"] = len(self.failed_tables)
    
    def get_table_groups(self) -> Dict[str, List[str]]:
        """Get logical table groupings for coordinated processing."""
        with self._lock:
            if not self.table_groups:
                self._create_table_groups()
            return self.table_groups.copy()
    
    def _create_table_groups(self):
        """Create logical table groups based on relationships and patterns."""
        # Group by common prefixes
        prefix_groups = {}
        for table_name in self.tables:
            # Extract prefix (e.g., "order_" from "order_items")
            parts = table_name.split('_')
            if len(parts) > 1:
                prefix = parts[0]
                if prefix not in prefix_groups:
                    prefix_groups[prefix] = []
                prefix_groups[prefix].append(table_name)
        
        # Group by size categories
        size_groups = {
            "large": [],   # > 1GB
            "medium": [],  # 100MB - 1GB
            "small": []    # < 100MB
        }
        
        for table_name, table in self.tables.items():
            if table.data_size_mb > 1000:
                size_groups["large"].append(table_name)
            elif table.data_size_mb > 100:
                size_groups["medium"].append(table_name)
            else:
                size_groups["small"].append(table_name)
        
        # Group by update frequency
        frequency_groups = {
            "high_frequency": [],    # > 50 updates/hour
            "medium_frequency": [],  # 10-50 updates/hour
            "low_frequency": []      # < 10 updates/hour
        }
        
        for table_name, table in self.tables.items():
            if table.update_frequency > 50:
                frequency_groups["high_frequency"].append(table_name)
            elif table.update_frequency > 10:
                frequency_groups["medium_frequency"].append(table_name)
            else:
                frequency_groups["low_frequency"].append(table_name)
        
        self.table_groups = {
            **prefix_groups,
            **{f"size_{k}": v for k, v in size_groups.items()},
            **{f"freq_{k}": v for k, v in frequency_groups.items()}
        }
    
    def get_statistics(self) -> Dict:
        """Get table management statistics."""
        with self._lock:
            stats = self.stats.copy()
            
            # Add detailed statistics
            if self.tables:
                total_size = sum(t.data_size_mb for t in self.tables.values())
                total_rows = sum(t.row_count for t in self.tables.values())
                avg_update_freq = sum(t.update_frequency for t in self.tables.values()) / len(self.tables)
                
                stats.update({
                    "total_size_gb": total_size / 1024,
                    "total_rows": total_rows,
                    "avg_update_frequency": avg_update_freq,
                    "tables_by_status": {
                        "enabled": sum(1 for t in self.tables.values() if t.cdc_enabled),
                        "disabled": sum(1 for t in self.tables.values() if not t.cdc_enabled),
                        "failed": len(self.failed_tables),
                        "active": len(self.active_tables)
                    }
                })
            
            return stats
    
    def _save_metadata(self):
        """Save table metadata to disk."""
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        
        metadata_file = self.metadata_path / "tables.json"
        with open(metadata_file, 'w') as f:
            data = {
                "tables": {name: table.to_dict() for name, table in self.tables.items()},
                "groups": self.table_groups,
                "stats": self.stats
            }
            json.dump(data, f, indent=2)
    
    def _load_metadata(self):
        """Load table metadata from disk."""
        metadata_file = self.metadata_path / "tables.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    data = json.load(f)
                    
                    # Load tables
                    for name, table_dict in data.get("tables", {}).items():
                        self.tables[name] = TableMetadata.from_dict(table_dict)
                    
                    # Load groups and stats
                    self.table_groups = data.get("groups", {})
                    self.stats = data.get("stats", self.stats)
                    
                print(f"Loaded metadata for {len(self.tables)} tables")
            except Exception as e:
                print(f"Error loading metadata: {e}")
    
    def close(self):
        """Save metadata and clean up."""
        self._save_metadata()
        print(f"Table manager closed. Final statistics: {self.get_statistics()}")