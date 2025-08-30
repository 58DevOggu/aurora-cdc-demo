"""Connection pool for Aurora MySQL."""

import threading
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional, Any
import mysql.connector
from mysql.connector import Error


class ConnectionPool:
    """Thread-safe connection pool for Aurora MySQL."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        max_connections: int = 10,
        connection_timeout: int = 30
    ):
        """Initialize the connection pool."""
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        
        self._pool = Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._created_connections = 0
        
        # Pre-create minimum connections
        min_connections = min(2, max_connections)
        for _ in range(min_connections):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)

    def _create_connection(self) -> Optional[Any]:
        """Create a new database connection."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                autocommit=True,
                connection_timeout=self.connection_timeout
            )
            with self._lock:
                self._created_connections += 1
            return conn
        except Error as e:
            print(f"Error creating connection: {e}")
            return None

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        connection = None
        try:
            # Try to get an existing connection
            try:
                connection = self._pool.get(block=False)
            except Empty:
                # Create a new connection if under limit
                with self._lock:
                    if self._created_connections < self.max_connections:
                        connection = self._create_connection()
                    else:
                        # Wait for an available connection
                        connection = self._pool.get(block=True, timeout=self.connection_timeout)
            
            # Verify connection is still valid
            if connection and not connection.is_connected():
                connection.reconnect()
            
            yield connection
            
        finally:
            # Return connection to pool
            if connection and connection.is_connected():
                self._pool.put(connection)

    def close(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                if conn and conn.is_connected():
                    conn.close()
            except Empty:
                break
        
        with self._lock:
            self._created_connections = 0