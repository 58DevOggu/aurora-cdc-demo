"""Enhanced connection pool with binlog client support for real-time CDC."""

import threading
from contextlib import contextmanager
from queue import Queue, Empty, Full
from typing import Optional, Any, Dict, List
import time
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import pymysqlreplication
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)


class EnhancedConnectionPool:
    """
    Thread-safe connection pool with binlog streaming support.
    Optimized for 500+ table CDC operations.
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        max_connections: int = 20,
        min_connections: int = 5,
        connection_timeout: int = 30,
        enable_binlog_client: bool = True,
        binlog_config: Optional[Dict] = None
    ):
        """Initialize enhanced connection pool."""
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self.enable_binlog_client = enable_binlog_client
        
        # Connection pools
        self._query_pool = Queue(maxsize=max_connections)
        self._binlog_pool = Queue(maxsize=max_connections // 2)  # Separate pool for binlog
        
        # Thread safety
        self._lock = threading.Lock()
        self._stats_lock = threading.Lock()
        
        # Connection statistics
        self._stats = {
            "created": 0,
            "active": 0,
            "idle": 0,
            "failed": 0,
            "total_requests": 0,
            "total_wait_time": 0,
            "max_wait_time": 0
        }
        
        # Binlog configuration
        self.binlog_config = binlog_config or {
            "server_id": 100,
            "blocking": False,
            "only_events": [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            "ignored_databases": ["mysql", "information_schema", "performance_schema", "sys"],
            "resume_stream": True,
            "slave_heartbeat": 30
        }
        
        # Health check thread
        self._health_check_interval = 60  # seconds
        self._health_check_thread = None
        self._shutdown = False
        
        # Initialize pools
        self._initialize_pools()
        self._start_health_check()
    
    def _initialize_pools(self):
        """Initialize connection pools with minimum connections."""
        # Create minimum query connections
        for _ in range(self.min_connections):
            try:
                conn = self._create_query_connection()
                if conn:
                    self._query_pool.put(conn)
            except Exception as e:
                print(f"Failed to create initial connection: {e}")
        
        # Create binlog connections if enabled
        if self.enable_binlog_client:
            for _ in range(min(2, self.max_connections // 4)):
                try:
                    stream = self._create_binlog_stream()
                    if stream:
                        self._binlog_pool.put(stream)
                except Exception as e:
                    print(f"Failed to create binlog stream: {e}")
    
    def _create_query_connection(self) -> Optional[Any]:
        """Create a new query connection."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                autocommit=True,
                connection_timeout=self.connection_timeout,
                use_pure=False,  # Use C extension for better performance
                pool_name=f"aurora_cdc_pool_{id(self)}",
                pool_size=1
            )
            
            # Test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            with self._stats_lock:
                self._stats["created"] += 1
            
            return conn
            
        except Error as e:
            with self._stats_lock:
                self._stats["failed"] += 1
            print(f"Error creating query connection: {e}")
            return None
    
    def _create_binlog_stream(self) -> Optional[BinLogStreamReader]:
        """Create a new binlog stream reader."""
        try:
            # MySQL connection settings for binlog
            connection_settings = {
                "host": self.host,
                "port": self.port,
                "user": self.username,
                "passwd": self.password
            }
            
            # Create binlog stream reader
            stream = BinLogStreamReader(
                connection_settings=connection_settings,
                server_id=self.binlog_config["server_id"] + self._stats["created"],
                blocking=self.binlog_config["blocking"],
                only_events=self.binlog_config["only_events"],
                ignored_databases=self.binlog_config["ignored_databases"],
                resume_stream=self.binlog_config["resume_stream"],
                slave_heartbeat=self.binlog_config["slave_heartbeat"]
            )
            
            return stream
            
        except Exception as e:
            print(f"Error creating binlog stream: {e}")
            return None
    
    @contextmanager
    def get_connection(self, connection_type: str = "query"):
        """
        Get a connection from the pool.
        
        Args:
            connection_type: "query" for regular queries, "binlog" for binlog streaming
        """
        connection = None
        start_time = time.time()
        
        try:
            with self._stats_lock:
                self._stats["total_requests"] += 1
            
            if connection_type == "binlog" and self.enable_binlog_client:
                pool = self._binlog_pool
                create_func = self._create_binlog_stream
            else:
                pool = self._query_pool
                create_func = self._create_query_connection
            
            # Try to get existing connection
            try:
                connection = pool.get(block=False)
            except Empty:
                # Create new connection if under limit
                with self._lock:
                    if self._stats["created"] < self.max_connections:
                        connection = create_func()
                    else:
                        # Wait for available connection
                        connection = pool.get(block=True, timeout=self.connection_timeout)
            
            # Update wait time statistics
            wait_time = time.time() - start_time
            with self._stats_lock:
                self._stats["total_wait_time"] += wait_time
                self._stats["max_wait_time"] = max(self._stats["max_wait_time"], wait_time)
                self._stats["active"] += 1
                self._stats["idle"] = max(0, self._stats["idle"] - 1)
            
            # Verify connection is valid
            if connection_type == "query" and connection:
                if not connection.is_connected():
                    connection.reconnect(attempts=3, delay=1)
            
            yield connection
            
        finally:
            # Return connection to pool
            if connection:
                try:
                    if connection_type == "query":
                        if connection.is_connected():
                            pool.put(connection, block=False)
                        else:
                            # Replace with new connection
                            new_conn = create_func()
                            if new_conn:
                                pool.put(new_conn, block=False)
                    else:
                        # Binlog streams are typically not reused
                        pass
                except Full:
                    # Pool is full, close connection
                    if connection_type == "query":
                        connection.close()
                
                with self._stats_lock:
                    self._stats["active"] = max(0, self._stats["active"] - 1)
                    self._stats["idle"] += 1
    
    def get_binlog_events(self, start_position: Optional[Dict] = None, tables: Optional[List[str]] = None):
        """
        Get binlog events for specified tables.
        
        Args:
            start_position: Dictionary with 'log_file' and 'log_pos'
            tables: List of table names to monitor
        
        Yields:
            Binlog events
        """
        if not self.enable_binlog_client:
            raise RuntimeError("Binlog client is not enabled")
        
        connection_settings = {
            "host": self.host,
            "port": self.port,
            "user": self.username,
            "passwd": self.password
        }
        
        # Configure starting position
        kwargs = {
            "connection_settings": connection_settings,
            "server_id": self.binlog_config["server_id"] + hash(str(tables)),
            "blocking": True,
            "only_events": self.binlog_config["only_events"],
            "ignored_databases": self.binlog_config["ignored_databases"],
            "resume_stream": self.binlog_config["resume_stream"]
        }
        
        if start_position:
            kwargs["log_file"] = start_position.get("log_file")
            kwargs["log_pos"] = start_position.get("log_pos")
        
        if tables:
            kwargs["only_tables"] = tables
            kwargs["only_schemas"] = [self.database]
        
        # Create binlog stream
        stream = BinLogStreamReader(**kwargs)
        
        try:
            for binlog_event in stream:
                if self._shutdown:
                    break
                
                # Filter by database if needed
                if hasattr(binlog_event, 'schema') and binlog_event.schema != self.database:
                    continue
                
                yield binlog_event
        finally:
            stream.close()
    
    def _health_check_worker(self):
        """Background thread to check connection health."""
        while not self._shutdown:
            try:
                # Check query pool health
                idle_connections = []
                while not self._query_pool.empty():
                    try:
                        conn = self._query_pool.get(block=False)
                        if conn.is_connected():
                            # Test with simple query
                            cursor = conn.cursor()
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                            cursor.close()
                            idle_connections.append(conn)
                        else:
                            # Replace with new connection
                            new_conn = self._create_query_connection()
                            if new_conn:
                                idle_connections.append(new_conn)
                    except Empty:
                        break
                    except Exception as e:
                        print(f"Health check error: {e}")
                
                # Return healthy connections to pool
                for conn in idle_connections:
                    try:
                        self._query_pool.put(conn, block=False)
                    except Full:
                        conn.close()
                
                # Update statistics
                with self._stats_lock:
                    self._stats["idle"] = self._query_pool.qsize()
                
            except Exception as e:
                print(f"Health check thread error: {e}")
            
            # Sleep until next check
            time.sleep(self._health_check_interval)
    
    def _start_health_check(self):
        """Start the health check thread."""
        if not self._health_check_thread:
            self._health_check_thread = threading.Thread(
                target=self._health_check_worker,
                daemon=True
            )
            self._health_check_thread.start()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self._stats_lock:
            stats = self._stats.copy()
            stats["query_pool_size"] = self._query_pool.qsize()
            stats["binlog_pool_size"] = self._binlog_pool.qsize()
            if stats["total_requests"] > 0:
                stats["avg_wait_time"] = stats["total_wait_time"] / stats["total_requests"]
            else:
                stats["avg_wait_time"] = 0
        return stats
    
    def close(self):
        """Close all connections and clean up resources."""
        self._shutdown = True
        
        # Wait for health check thread to finish
        if self._health_check_thread:
            self._health_check_thread.join(timeout=5)
        
        # Close query connections
        while not self._query_pool.empty():
            try:
                conn = self._query_pool.get(block=False)
                if conn and hasattr(conn, 'close'):
                    conn.close()
            except Empty:
                break
        
        # Close binlog streams
        while not self._binlog_pool.empty():
            try:
                stream = self._binlog_pool.get(block=False)
                if stream and hasattr(stream, 'close'):
                    stream.close()
            except Empty:
                break
        
        print(f"Connection pool closed. Final statistics: {self.get_statistics()}")